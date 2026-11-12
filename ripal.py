#!/usr/bin/env python3
"""RIPAL Mark: local prefill/decode load benchmark. Python standard library only."""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextlib import nullcontext
from dataclasses import asdict, dataclass, replace
from difflib import SequenceMatcher
from pathlib import Path
from urllib.error import HTTPError, URLError
import argparse
import hashlib
import json
import math
import os
import queue
import random
import re
import shutil
import signal
import socket
import statistics
import sys
import threading
import time as real_time
import urllib.request


class Clock:
    backend = None

    @property
    def virtual(self):
        return self.backend is not None

    def perf_counter(self):
        return self.backend.now if self.backend else real_time.perf_counter()

    def time(self):
        return self.backend.epoch + self.backend.now if self.backend else real_time.time()

    def time_ns(self):
        if not self.backend:
            return real_time.time_ns()
        self.backend.serial += 1
        return int(self.time() * 1e9) + self.backend.serial


clock = Clock()
time = clock  # Shared name in the standalone bundle.


class RunCancelled(Exception):
    pass


class ToolDeadlineExceeded(TimeoutError):
    pass


class RunControl:
    def __init__(self):
        self.stop = threading.Event()
        self.kind = None
        self.reason = None
        self.init_responses()

    def init_responses(self):
        self.response_lock = threading.Lock()
        self.response_sockets = set()

    @contextmanager
    def watch_response(self, response):
        # urllib's HTTPResponse wraps a SocketIO (also for HTTPS). Shutting down
        # that socket unblocks a read waiting for the first SSE token. Merely
        # closing the buffered reader from another thread can block on its lock.
        sock = getattr(getattr(getattr(response, 'fp', None), 'raw', None), '_sock', None)
        if sock is not None:
            with self.response_lock:
                self.response_sockets.add(sock)
        try:
            self.check()
            yield
        finally:
            if sock is not None:
                with self.response_lock:
                    self.response_sockets.discard(sock)

    def abort_responses(self):
        with self.response_lock:
            sockets = list(self.response_sockets)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    @property
    def stopped(self):
        return self.stop.is_set()

    def request_stop(self, *_):
        # Signal handlers must not acquire UI/worker locks or perform I/O.
        self.terminate("interrupt", "Interrupted by Ctrl-C")

    def terminate(self, kind, reason):
        if not self.stopped:
            self.kind, self.reason = kind, reason
            self.stop.set()

    @property
    def exit_code(self):
        return 130 if self.kind == "interrupt" else 2

    def check(self):
        if self.stopped:
            self.abort_responses()
            raise RunCancelled(self.reason or "Run stopped")

    def call(self, operation, timeout=60, label="tool call"):
        self.check()
        if time.virtual:
            return time.backend.invoke(self, operation, timeout, label)
        deadline = time.perf_counter() + timeout
        result = queue.Queue(maxsize=1)
        def execute():
            try:
                result.put((True, operation()))
            except BaseException as exc:
                result.put((False, exc))
        # urllib can block before exposing a response/socket. Keep only this I/O
        # wait daemonized so lane workers can join promptly on Ctrl-C. Process
        # exit closes any still-pending HTTP sockets; no child process is spawned.
        threading.Thread(target=execute, daemon=True, name="ripal-http").start()
        while True:
            self.check()
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                message = f"{label} exceeded the {timeout:g}s response deadline (possible server overload)"
                self.terminate("overload", message)
                raise ToolDeadlineExceeded(message)
            try:
                ok, value = result.get(timeout=min(.05, remaining))
            except queue.Empty:
                continue
            if time.perf_counter() >= deadline:
                continue
            if ok:
                return value
            raise value

    @contextmanager
    def signals(self):
        installed = threading.current_thread() is threading.main_thread()
        previous = signal.getsignal(signal.SIGINT) if installed else None
        if installed:
            signal.signal(signal.SIGINT, self.request_stop)
        try:
            yield self
        finally:
            if installed:
                signal.signal(signal.SIGINT, previous)


class FillerControl(RunControl):
    def __init__(self, parent, finished):
        self.parent, self.finished = parent, finished
        self.init_responses()

    @property
    def stopped(self):
        return self.parent.stopped or self.finished.is_set()

    @property
    def kind(self):
        return self.parent.kind

    @property
    def reason(self):
        return self.parent.reason or "All primary lanes completed"

    def terminate(self, kind, reason):
        self.parent.terminate(kind, reason)


class StageControl(RunControl):
    """Latch the slowest in-flight task at the cap; drain it, then cancel tails."""
    def __init__(self, parent, lanes=0):
        self.parent = parent
        self.init_responses()
        self.deadline = None
        self.seconds = None
        self.progress_lock = threading.Lock()
        self.progress = {lane: {"completed": 0, "active": None} for lane in range(lanes)}
        self.cutoff = None
        self.drainers = set()

    def start(self, seconds):
        self.seconds = seconds
        self.deadline = time.perf_counter() + seconds if seconds else None

    @property
    def expired(self):
        return self.deadline is not None and time.perf_counter() >= self.deadline

    def latch(self):
        # Caller holds progress_lock. Each lane finishes at most its already
        # admitted task; only the slowest cohort earns additional measured credit.
        if self.expired and self.cutoff is None:
            active = [p["active"] for p in self.progress.values() if p["active"] is not None]
            target = min(active) if active else None
            self.drainers = {lane for lane, p in self.progress.items()
                             if target is not None and p["active"] == target}
            self.cutoff = {"task": target, "completion_limits": {
                lane: p["active"] if lane in self.drainers else p["completed"]
                for lane, p in self.progress.items()}}

    def begin_task(self, lane, task):
        with self.progress_lock:
            self.latch()
            if self.cutoff is not None:
                raise RunCancelled(self.reason)
            self.progress.setdefault(lane, {"completed": 0, "active": None})["active"] = task

    def finish_task(self, lane, task):
        with self.progress_lock:
            self.latch()
            self.progress[lane].update(completed=task, active=None)
            self.drainers.discard(lane)

    def finish_lane(self, lane):
        with self.progress_lock:
            self.latch()
            self.progress[lane]["active"] = None
            self.drainers.discard(lane)

    @property
    def stopped(self):
        if self.parent.stopped:
            return True
        with self.progress_lock:
            self.latch()
            return self.cutoff is not None and not self.drainers

    @property
    def kind(self):
        return self.parent.kind if self.parent.stopped else 'stage_time_limit' if self.expired else None

    @property
    def reason(self):
        if self.parent.stopped:
            return self.parent.reason
        return f'Measured stage reached its {self.seconds:g}s time budget' if self.expired else None

    def terminate(self, kind, reason):
        self.parent.terminate(kind, reason)


class TaskGate:
    def __init__(self, stage, lane):
        self.stage, self.lane = stage, lane

    def begin(self, task):
        self.stage.begin_task(self.lane, task)

    def complete(self, task):
        self.stage.finish_task(self.lane, task)

    def finish(self):
        self.stage.finish_lane(self.lane)


def levenshtein(expected, received):
    if expected == received:
        return 0
    if len(expected) > len(received):
        expected, received = received, expected
    size = len(expected)
    if not size:
        return len(received)
    masks = {}
    for index, char in enumerate(expected):
        masks[char] = masks.get(char, 0) | (1 << index)
    limit = (1 << size) - 1
    positive, negative, distance = limit, 0, size
    high = 1 << (size - 1)
    for char in received:
        equal = masks.get(char, 0)
        vertical = equal | negative
        horizontal = (((equal & positive) + positive) ^ positive) | equal
        plus = negative | ~(horizontal | positive)
        minus = positive & horizontal
        distance += bool(plus & high) - bool(minus & high)
        plus = (plus << 1) | 1
        minus <<= 1
        positive = (minus | ~(vertical | plus)) & limit
        negative = (plus & vertical) & limit
    return distance


def text_quality(expected, received):
    distance = levenshtein(expected, received)
    length = max(len(expected), len(received))
    return {"distance": distance, "expected_characters": len(expected),
            "received_characters": len(received),
            "similarity": 1 - distance / length if length else 1.0}


class MixController:
    def __init__(self, padding, enabled=True, history="auto", target=4.0, tolerance=.15):
        self.padding = padding
        self.enabled, self.history = enabled, history
        self.target, self.tolerance = target, tolerance
        self.fresh = self.output = 0
        self.events = []
        self.maximum_padding = max(16384, padding)

    def observe(self, mix, task):
        if not mix.get("available") or not mix.get("output_tokens"):
            return None
        self.fresh += mix["fresh_tokens"]
        self.output += mix["output_tokens"]
        ratio = mix["fresh_per_output"]
        cumulative = self.fresh / self.output
        before = self.padding
        outside = abs(ratio / self.target - 1) > self.tolerance or abs(cumulative / self.target - 1) > self.tolerance
        if self.enabled and outside:
            # Damped cumulative token debt, with a bounded step. This estimate is
            # only for filler sizing; actual ratios always use server counters.
            debt = self.target * self.output - self.fresh
            step = round(.5 * debt / 1.3)
            limit = max(256, before // 2)
            self.padding = max(0, min(self.maximum_padding, before + max(-limit, min(limit, step))))
        compact = self.history == "task" or (self.enabled and self.history == "auto" and (
            mix["cached_tokens"] == 0 or ratio > self.target * (1 + self.tolerance)))
        event = {"after_task": task, "task_ratio": ratio, "cumulative_ratio": cumulative,
                 "padding_words": before, "next_padding_words": self.padding,
                 "compact_history": compact,
                 "floor_warning": before == 0 and ratio > self.target * (1 + self.tolerance),
                 "reason": "no observed cache reuse" if compact and mix["cached_tokens"] == 0 else
                           "prefill over budget" if compact else "padding feedback"}
        self.events.append(event)
        return event


class GenerationController:
    """Repay fresh/decode imbalance with source words, not just padding."""
    def __init__(self, words=5, enabled=True, target=4.0):
        self.words, self.enabled, self.target = words, enabled, target
        self.minimum_words = max(1, min(5, words))
        self.fresh = self.output = 0
        self.events = []
        self.tokens_per_word = 3.0

    def observe(self, requests, words, padding_words, task):
        if not requests or not all(r["tokens"].get("available") for r in requests):
            return None
        fresh = sum(r["tokens"]["fresh_tokens"] for r in requests)
        output = sum(r["tokens"]["output_tokens"] for r in requests)
        submit = next((r for r in reversed(requests) if r["stage"] == "submit_result"), None)
        read = next((r for r in reversed(requests) if r["stage"] == "read_file"), None)
        if not submit or not read or output <= 0:
            return None
        variable = max(1, submit["tokens"]["output_tokens"] - 32)
        observed = max(1, variable / words)
        self.tokens_per_word = observed
        overhead = max(0, output - variable)
        # The read result's prompt increment estimates input cost per extra
        # source word, independent of cached-prefix accounting.
        delta = submit["tokens"]["prompt_tokens"] - read["tokens"]["prompt_tokens"]
        input_per_word = max(0, min(12, (delta - padding_words * 1.3 - 128) / words))
        self.fresh += fresh
        self.output += output
        debt = self.fresh - self.target * self.output
        baseline = max(0, fresh - input_per_word * words)
        denominator = max(1, self.target * observed - input_per_word)
        desired = max(self.minimum_words, round((baseline + .35 * debt - self.target * overhead) / denominator))
        before = self.words
        if self.enabled and (abs(fresh / output / self.target - 1) > .15 or
                             abs(self.fresh / self.output / self.target - 1) > .15):
            self.words = max(self.minimum_words, min(65536, max(round(words / 2), min(words * 16, desired))))
        event = {"after_task": task, "source_words": words, "next_words": self.words,
                 "task_ratio": fresh / output, "cumulative_ratio": self.fresh / self.output,
                 "output_tokens_per_word": observed, "input_tokens_per_word": input_per_word,
                 "previous_words": before}
        self.events.append(event)
        return event


def read_stream(response, started, control=None, on_chunk=None):
    message = {"role": "assistant", "content": None}
    calls, result, data = {}, {}, []
    first = last = None
    finish_reason = None
    done = False
    def consume(payload):
        nonlocal first, last, finish_reason, done
        if on_chunk:
            on_chunk(payload, time.perf_counter())
        if payload == "[DONE]":
            done = True
            return
        chunk = json.loads(payload)
        for key in ("id", "model", "created", "usage", "metrics", "timings"):
            if chunk.get(key) is not None:
                result[key] = chunk[key]
        for choice in chunk.get("choices", []):
            if choice.get("index", 0) != 0:
                continue
            delta = choice.get("delta") or {}
            generated = False
            for key in ("content", "reasoning_content", "reasoning"):
                value = delta.get(key)
                if isinstance(value, str) and value:
                    target = "reasoning_content" if key == "reasoning" else key
                    message[target] = (message.get(target) or "") + value
                    generated = True
            for part in delta.get("tool_calls") or []:
                call = calls.setdefault(part.get("index", 0), {"id": "", "type": "function",
                                        "function": {"name": "", "arguments": ""}})
                if part.get("id"):
                    call["id"] = part["id"]
                function = part.get("function") or {}
                for key in ("name", "arguments"):
                    fragment = function.get(key)
                    if fragment:
                        if key != "name" or fragment != call["function"][key]:
                            call["function"][key] += fragment
                        generated = True
            if generated:
                now = time.perf_counter()
                if first is None:
                    first = now
                last = now
            if choice.get("finish_reason") is not None:
                finish_reason = choice["finish_reason"]
    for raw in response:
        if control:
            control.check()
        line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        line = line.rstrip("\r\n")
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
        elif not line and data:
            consume("\n".join(data))
            data = []
            if done:
                break
    if data and not done:
        consume("\n".join(data))
    if not done:
        raise ValueError("Stream ended before [DONE]")
    if calls:
        message["tool_calls"] = [calls[index] for index in sorted(calls)]
    result["choices"] = [{"index": 0, "finish_reason": finish_reason, "message": message}]
    result["_stream_timing"] = {"source": "visible streaming deltas",
        "first_output_seconds": first - started if first is not None else None,
        "generation_seconds": last - first if first is not None else None,
        "response_seconds": time.perf_counter() - started}
    return result


def phase_timing(response, tokens):
    timing = response.get("_stream_timing")
    if not timing:
        return None
    return {**timing, "fresh_tokens": tokens.get("fresh_tokens"),
            "output_tokens": tokens.get("output_tokens")}


def average_phases(sessions, lanes=1):
    completed = [request for session in sessions for request in session.get("requests", [])
                 if request.get("received_response") and not request.get("cancelled")]
    samples = [r["phase_timing"] for r in completed if r.get("phase_timing")]
    prefill = [s for s in samples if s.get("fresh_tokens") is not None and (s.get("first_output_seconds") or 0) > 0]
    decode = [s for s in samples if (s.get("output_tokens") or 0) > 1 and (s.get("generation_seconds") or 0) > 0]
    prefill_rate = sum(s["fresh_tokens"] for s in prefill) / sum(s["first_output_seconds"] for s in prefill) if prefill else None
    decode_rate = sum(s["output_tokens"] - 1 for s in decode) / sum(s["generation_seconds"] for s in decode) if decode else None
    prefill_values = [s["fresh_tokens"] / s["first_output_seconds"] for s in prefill]
    decode_values = [(s["output_tokens"] - 1) / s["generation_seconds"] for s in decode]
    return {
        "prefill_stdev": statistics.stdev(prefill_values) if len(prefill_values) > 1 else None,
        "decode_stdev": statistics.stdev(decode_values) if len(decode_values) > 1 else None,
        "aggregate_prefill_stdev": lanes * statistics.stdev(prefill_values) if len(prefill_values) > 1 else None,
        "aggregate_decode_stdev": lanes * statistics.stdev(decode_values) if len(decode_values) > 1 else None,
        "source": "streaming estimates; visible deltas may be buffered",
        "prefill_tokens_per_second": prefill_rate,
        "decode_tokens_per_second": decode_rate,
        "aggregate_prefill_tokens_per_second": lanes * prefill_rate if prefill_rate is not None else None,
        "aggregate_decode_tokens_per_second": lanes * decode_rate if decode_rate is not None else None,
        "aggregate_method": "concurrency times time-weighted per-request phase estimate",
        "prefill_samples": len(prefill), "decode_samples": len(decode),
        "average_tool_seconds": sum(r["seconds"] for r in completed) / len(completed) if completed else None,
        "average_completion_seconds": sum(s["seconds"] for s in sessions) / len(sessions) if sessions else None,
        "prefill_caveat": "fresh tokens / time to first visible generation, including queue/network delay",
        "decode_caveat": "(output tokens - 1) / first-to-last visible generation interval; chunk buffering affects estimates"}


BACKGROUND = "48;2;28;25;23"
CREAM = "38;2;230;224;214"
MUTED = "38;2;146;137;123"
ORANGE = "38;2;255;112;25"
AMBER = "38;2;234;181;49"
WARNING = "38;2;239;106;80"
WHITE = "38;2;255;255;255"
STYLES = {"0": CREAM, "1": "1;" + CREAM, "2": MUTED,
          "1;36": "1;" + CREAM, "1;34": MUTED, "36": ORANGE,
          "31": WARNING, "33": AMBER, "32": AMBER}


def format_duration(seconds):
    tenths = max(0, round(seconds * 10))
    minutes, remainder = divmod(tenths, 600)
    return f"{minutes}:{remainder // 10:02d}.{remainder % 10}"


def safe_text(value):
    return "".join(c if c >= " " and c != "\x7f" else " " for c in str(value))


def difference_lines(expected, received, color=False, width=110, prefix=""):
    """Style only changed characters; clipping happens before ANSI styling."""
    expected, received = safe_text(repr(expected)), safe_text(repr(received))
    sides = [[], []]
    for tag, a, b, c, d in SequenceMatcher(None, expected, received, autojunk=False).get_opcodes():
        sides[0].append((expected[a:b] or "∅", tag != "equal"))
        sides[1].append((received[c:d] or "∅", tag != "equal"))
    lines = []
    for label, spans, foreground in zip(("expected", "received"), sides, (AMBER, ORANGE)):
        parts = [(f"  {safe_text(prefix)}{label}: ", False)] + spans
        remaining = max(1, width - 1)
        rendered, used = [], 0
        for text, changed in parts:
            if not color and changed:
                text = "[" + text + "]"
            text = text[:remaining]
            remaining -= len(text)
            used += len(text)
            if color:
                style = f"1;4;{foreground};48;2;65;43;25" if changed else f"{BACKGROUND};{MUTED}"
                rendered.append(f"\x1b[0;{style}m{text}")
            else:
                rendered.append(text)
            if not remaining:
                break
        if color:
            rendered.append(f"\x1b[0;{BACKGROUND};{MUTED}m" + " " * max(0, width - 1 - used) + "\x1b[0m")
        lines.append("".join(rendered))
    return lines


class TerminalUI:
    def __init__(self, model, endpoint, mode="auto", color="auto", stream=None):
        self.stream = stream if stream is not None else sys.stdout
        tty = self.stream.isatty()
        self.live = mode == "live" or (mode == "auto" and tty and os.environ.get("TERM") != "dumb")
        self.color = color == "always" or (color == "auto" and tty and "NO_COLOR" not in os.environ)
        self.model, self.endpoint = safe_text(model), safe_text(endpoint)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.started = time.perf_counter()
        self.phase = "Starting"
        self.lanes = {}
        self.tasks = 0
        self.events = deque(maxlen=50)
        self.finals = []
        self.padding = 0
        self.stage_deadline = None
        self.stage_control = None

    def __enter__(self):
        if self.live:
            self.stream.write("\x1b[?1049h\x1b[2J\x1b[H\x1b[?25l")
            self.stream.flush()
            self.thread = threading.Thread(target=self._refresh, daemon=True)
            self.thread.start()
        else:
            self.log(f"RIPAL | {self.model} | {self.endpoint}")
        return self

    def __exit__(self, *exc):
        self.stop.set()
        if self.thread:
            self.thread.join()
        if self.live:
            self.stream.write("\x1b[?25h\x1b[?1049l")
            for message in self.finals:
                self.stream.write(message + "\n")
            if exc[0]:
                self.stream.write("Run interrupted; terminal restored.\n")
            self.stream.flush()

    def begin(self, phase, lanes, tasks, padding):
        with self.lock:
            self.phase, self.tasks, self.padding = phase, tasks, padding
            self.stage_deadline = None
            self.stage_control = None
            self.lanes = {i: {"task": 0, "completed": 0, "finished": 0, "failures": 0, "stage": "ready", "status": "ready",
                              "since": time.perf_counter(), "calls": 0, "fresh": 0, "output": 0,
                              "accounted": True, "attempt": 1} for i in range(lanes)}
        self.log(f"{phase} | {lanes} lane(s), {tasks} tasks/session, padding={padding} words")

    def stage_budget(self, seconds, control=None):
        with self.lock:
            self.stage_control = control
            self.stage_deadline = time.perf_counter() + seconds if seconds else None

    def lane(self, index, **changes):
        with self.lock:
            state = self.lanes.get(index)
            if state is None:
                return
            state.update(changes)
            if "status" in changes:
                state["since"] = time.perf_counter()
            if not self.live and changes.get("status") == "waiting":
                self.stream.write(f"  lane {index + 1} task {state['task']}/{self.tasks} "
                                  f"{state['stage']} attempt {state['attempt']} — awaiting response\n")
                self.stream.flush()

    def request_done(self, index, seconds, tokens):
        with self.lock:
            state = self.lanes.get(index)
            if state is None:
                return
            state["calls"] += 1
            if tokens.get("available"):
                state["fresh"] += tokens["fresh_tokens"]
                state["output"] += tokens["output_tokens"]
                counts = f"fresh={tokens['fresh_tokens']:,} decode={tokens['output_tokens']:,}"
            else:
                state["accounted"] = False
                counts = "token accounting unavailable"
            message = f"lane {index + 1} {state['stage']} returned in {format_duration(seconds)} | {counts}"
            self.events.append(message)
            if not self.live:
                self.stream.write("  " + message + "\n")
                self.stream.flush()

    def log(self, message, final=False):
        message = safe_text(message)
        with self.lock:
            self.events.append(message)
            if final:
                self.finals.append(message)
            if not self.live:
                self.stream.write(message + "\n")
                self.stream.flush()

    def difference(self, expected, received, prefix=""):
        event = {"kind": "difference", "expected": str(expected),
                 "received": str(received), "prefix": safe_text(prefix)}
        with self.lock:
            self.events.append(event)
            if not self.live:
                width = shutil.get_terminal_size((110, 32)).columns
                self.stream.write("\n".join(difference_lines(expected, received, self.color, width, prefix)) + "\n")
                self.stream.flush()

    def render(self, width=110, height=32, now=None):
        now = time.perf_counter() if now is None else now
        with self.lock:
            lanes = [(i, dict(state)) for i, state in self.lanes.items()]
            phase, tasks, padding = self.phase, self.tasks, self.padding
            events = list(self.events)[-4:]
            stage_deadline = self.stage_deadline
            stage_control = self.stage_control
        def paint(text, code="0"):
            text = safe_text(text)[:max(1, width - 1)]
            return f"\x1b[{BACKGROUND};{STYLES.get(code, CREAM)}m{text.ljust(width - 1)}\x1b[0m" if self.color else text
        completed = sum(s["completed"] for _, s in lanes)
        finished = sum(s["finished"] for _, s in lanes)
        failures = sum(s["failures"] for _, s in lanes)
        total = len(lanes) * tasks
        filled = int(24 * finished / total) if total else 0
        fresh = sum(s["fresh"] for _, s in lanes)
        output = sum(s["output"] for _, s in lanes)
        known = all(s["accounted"] for _, s in lanes)
        ratio = f"{fresh / output:.2f}:1" if known and output else "pending/unknown"
        spin = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"[int(now * 8) % 10]
        budget_text = f"Stage left {format_duration(stage_deadline - now)} · " if stage_deadline is not None else ""
        if stage_control and stage_control.cutoff is not None and stage_control.drainers:
            budget_text = f"Draining task {stage_control.cutoff['task']} · "
        lines = [paint("RIPAL  /  local prefill + decode benchmark", "1;36"),
                 paint(f"{self.model}  ·  {self.endpoint}", "2"), "",
                 paint(f"{phase}  ·  elapsed {format_duration(now - self.started)}  ·  initial padding {padding:,} words", "1"),
                 paint(f"[{'━' * filled}{'─' * (24 - filled)}]  {finished}/{total} processed · {completed} passed · {failures} failed", "36"),
                 paint(f"{budget_text}Batch tokens  fresh {fresh:,}  decode {output:,}  running ratio {ratio}  (target 4:1)", "2"), "",
                 paint("LANE  TASK    TOOL / STATE                  REQUEST AGE    CALLS  FAILS  WORDS", "1;34")]
        visible = max(1, height - 16)
        for index, state in lanes[:visible]:
            waiting = state["status"] == "waiting"
            age = format_duration(now - state["since"]) if waiting else "—"
            label = state["stage"] if waiting else state["status"]
            retry = f" retry {state['attempt'] - 1}" if state["attempt"] > 1 else ""
            marker = spin if waiting else "✓" if state["status"] == "completed" else "!" if state["status"] == "stopped" else "·"
            code = "31" if state["status"] == "stopped" else "33" if retry or state["status"] == "cancelled" or state["failures"] else "32" if state["status"] == "completed" else "0"
            lines.append(paint(f" {index + 1:>2}   {state['task']:>2}/{tasks:<2}  {marker} {label + retry:<29} {age:>9}    {state['calls']:>4}  {state['failures']:>5} {state.get('words', 5):>5}", code))
        if len(lanes) > visible:
            lines.append(paint(f"… {len(lanes) - visible} additional lanes", "2"))
        lines.extend(["", paint("RECENT EVENTS", "1;34")])
        groups = []
        count = 0
        for event in reversed(events):
            group = (difference_lines(event["expected"], event["received"], self.color, width, event["prefix"])
                     if isinstance(event, dict) and event.get("kind") == "difference" else
                     [paint("  " + event, "31" if "FAIL" in event or "mismatch" in event else "2")])
            if count + len(group) > 6:
                break
            groups.append(group)
            count += len(group)
        for group in reversed(groups):
            lines.extend(group)
        lines.extend(["", paint("Request age includes queueing + prefill + decode. Streaming samples timing; final usage gives token totals.", "2")])
        return "\n".join(line if line else paint("") for line in lines[:max(1, height - 1)])

    def card(self, text):
        """Keep the final multiline card after restoring the live screen."""
        lines = [safe_text(line) for line in text.splitlines()]
        if self.color:
            def style(line):
                if "OVERWHELMED" in line or "Stopped:" in line:
                    return WARNING
                if "RealMark" in line and "Results" in line:
                    return "1;" + CREAM
                if any(0x2801 <= ord(char) <= 0x28ff for char in line):
                    return ORANGE
                if any(char.isdigit() for char in line):
                    return AMBER
                return MUTED
            colored = []
            score_span = None
            in_table = False
            for line in lines:
                if "RIPALmark" in line and "Completed" in line:
                    score_span = (line.index("RIPALmark") - 1, line.index("RIPALmark") - 1 + len("RIPALmark"))
                    in_table = True
                elif not line.strip("│ "):
                    in_table = False
                if line.startswith(("╭", "╰")):
                    colored.append(f"\x1b[{BACKGROUND};{WHITE}m{line}\x1b[0m")
                elif line.startswith("│") and line.endswith("│"):
                    base = style(line)
                    content = line[1:-1]
                    if in_table and score_span:
                        a, b = score_span
                        content = content[:a] + "\x1b[1m" + content[a:b] + "\x1b[22m" + content[b:]
                    for heading in ("RIPALmark  ·  Y: points ±1σ", "Total decode~  ·  Y: tok/s ±1σ", "Total prefill~  ·  Y: tok/s ±1σ"):
                        content = content.replace(heading,
                            f"\x1b[0;{BACKGROUND};{MUTED}m{heading}\x1b[0;{BACKGROUND};{base}m")
                    colored.append(f"\x1b[{BACKGROUND};{WHITE}m│\x1b[{base}m{content}\x1b[{WHITE}m│\x1b[0m")
                else:
                    colored.append(f"\x1b[{BACKGROUND};{style(line)}m{line}\x1b[0m")
            lines = colored
        with self.lock:
            self.finals.extend(lines)
            self.events.append("Result card saved; restoring terminal.")
            if not self.live:
                self.stream.write("\n".join(lines) + "\n")
                self.stream.flush()

    def _refresh(self):
        while not self.stop.is_set():
            size = shutil.get_terminal_size((110, 32))
            self.stream.write("\x1b[H" + self.render(size.columns, size.lines).replace("\n", "\x1b[K\n") + "\x1b[K\x1b[J")
            self.stream.flush()
            self.stop.wait(.15)


def api_error_counts(sessions):
    counts = {}
    for session in sessions:
        for request in session.get("requests", []):
            kind = request.get("validation", {}).get("kind")
            if kind in {"transport_error", "tool_timeout", "server_overload"}:
                counts[kind] = counts.get(kind, 0) + 1
    return counts


def aggregate_phase(summary, lanes, key):
    phases = summary.get("phase_averages", {})
    value = phases.get(key)
    return phases.get("aggregate_" + key, int(lanes) * value if value is not None else None)


def braille_plot(points, width=32, height=4, errors=None):
    """Plot actual concurrency on X and autoscale Y to finite observed values."""
    if width < 1 or height < 1:
        raise ValueError("plot dimensions must be positive")
    points = sorted((n, value if value is not None and math.isfinite(value) else None) for n, value in points)
    if not points or any(n <= 0 for n, _ in points):
        raise ValueError("positive lane counts are required")
    known = [(n, value) for n, value in points if value is not None]
    xmin, xmax = (known[0][0], known[-1][0]) if known else (points[0][0], points[-1][0])
    errors = {n: value if value is not None and math.isfinite(value) and value >= 0 else None
              for n, value in (errors or {}).items()}
    low = min((value - max(0, errors.get(n) or 0) for n, value in known), default=0)
    high = max((value + max(0, errors.get(n) or 0) for n, value in known), default=0)
    extent = max(abs(low), abs(high))
    if extent:
        step = 10 ** (math.floor(math.log10(extent)) - 1)
        lo, hi = low / step, high / step
        lower = round(lo) - 1 if math.isclose(lo, round(lo), abs_tol=1e-8) else math.floor(lo)
        upper = round(hi) + 1 if math.isclose(hi, round(hi), abs_tol=1e-8) else math.ceil(hi)
        ymin, ymax = max(0, lower * step), max(step, upper * step)
    else:
        ymin, ymax = 0, .01
        step = .01
    w, h = width * 2, height * 4
    pixels, labels = set(), [" "] * width
    def position(n, value):
        x = round((n - xmin) / (xmax - xmin) * (w - 1)) if xmax != xmin else w // 2
        y = round(round((ymax - value) / (ymax - ymin), 9) * h) if ymax != ymin else h // 2
        return x, max(0, min(h, y))
    previous = None
    for n, value in points:
        if not xmin <= n <= xmax:
            continue
        x, _ = position(n, ymin)
        label = str(n)[:width]
        offset = max(0, min(width - len(label), x // 2 - len(label) // 2))
        labels[offset:offset + len(label)] = label
        if value is None:
            previous = None
            continue
        current = position(n, value)
        pixels.add(current)
        deviation = errors.get(n)
        if deviation is not None and math.isfinite(deviation) and deviation > 0:
            top = position(n, value + deviation)[1]
            bottom = position(n, value - deviation)[1]
            pixels.update((current[0], y) for y in range(top, bottom + 1))
            for y in (top, bottom):
                pixels.update((x, y) for x in range(max(0, current[0] - 1), min(w, current[0] + 2)))
        if previous is not None:
            dx, dy = current[0] - previous[0], current[1] - previous[1]
            steps = max(abs(dx), abs(dy))
            for segment in range(steps + 1):
                fraction = segment / steps if steps else 0
                pixels.add((round(previous[0] + dx * fraction), round(previous[1] + dy * fraction)))
        previous = current
    bits = ((1, 8), (2, 16), (4, 32), (64, 128))
    lines = []
    for row in range(height + 1):
        cells = []
        for col in range(width):
            mask = sum(bits[dy][dx] for dy in range(4) for dx in range(2)
                       if (col * 2 + dx, row * 4 + dy) in pixels)
            cells.append(chr(0x2800 + mask) if mask else " ")
        lines.append("".join(cells))
    return {"lines": lines[:-1], "baseline": lines[-1], "labels": "".join(labels), "has_data": bool(known),
            "min_score": min((value for _, value in known), default=0), "max_score": max((value for _, value in known), default=0), "x_min": xmin, "x_max": xmax,
            "y_min": ymin, "y_max": ymax, "y_step": step, "x_axis": "concurrency (C, linear)",
            "y_axis": "linear; 2-significant-digit bounds outside ±1 sample standard deviation; minimum clamped to zero",
            "points": [{"lanes": n, "score": value, "value": value, "stdev": errors.get(n)} for n, value in points]}


def axis_value(value):
    if not value:
        return "0"
    if abs(value) < .001 or abs(value) >= 1e8:
        return f"{value:.1e}"
    decimals = max(0, 1 - math.floor(math.log10(abs(value))))
    return f"{value:,.{decimals}f}"


def axis_labels(plot):
    return axis_value(plot["y_min"]), axis_value(plot["y_max"])



def middle_label(plot):
    value = (plot["y_min"] + plot["y_max"]) / 2
    if value and (abs(value) < .001 or abs(value) >= 1e8):
        return axis_value(value)
    precision = max(0, -math.floor(math.log10(plot["y_step"] / 2)))
    label = f"{value:,.{precision}f}"
    return label.rstrip('0').rstrip('.') if '.' in label else label


def chart_lines(plot, title, unit):
    result = [f"{title}  ·  Y: {unit} ±1σ"]
    if not plot["has_data"]:
        return result + ["No measured data available."]
    bottom, top = axis_labels(plot)
    middle = middle_label(plot)
    gutter = max(len(bottom), len(top), len(middle))
    for index, line in enumerate(plot["lines"]):
        tick = top if index == 0 else middle if index == len(plot["lines"]) // 2 else ""
        result.append(tick.rjust(gutter) + " │" + line)
    baseline = "".join(dot if dot != " " else "─" for dot in plot["baseline"])
    result.append(bottom.rjust(gutter) + " └" + baseline)
    result.append(" " * (gutter + 2) + plot["labels"])
    result.append(f"X: concurrency (C), {plot['x_min']}–{plot['x_max']}")
    return result


CARD_COLUMNS = (("C", 3), ("RIPALmark", 9), ("Decode (tok/s)", 14), ("Prefill (tok/s)", 15),
                ("Completed", 9), ("Time (s)", 8), ("Match (%)", 10), ("Mix (w / t)", 11))


def compact_number(value, width=8, decimals=1):
    if value is None or not math.isfinite(value):
        return "—"
    for precision in range(decimals, -1, -1):
        text = f"{value:,.{precision}f}"
        if len(text) <= width:
            return text
    for scale, suffix in ((1e3, "k"), (1e6, "M"), (1e9, "G"), (1e12, "T")):
        if scale <= abs(value) < scale * 1000:
            for precision in (2, 1, 0):
                text = f"{value / scale:.{precision}f}{suffix}"
                if len(text) <= width:
                    return text
    return f"{value:.0e}".replace("+", "")


def card_rate(summary, lanes, key, width):
    each = summary.get("phase_averages", {}).get(key)
    total = aggregate_phase(summary, lanes, key)
    if each is None or total is None:
        return "—"
    full = f"{total:,.1f} ({each:,.1f})"
    if len(full) <= width:
        return full
    number_width = (width - 3) // 2
    return f"{compact_number(total, number_width)} ({compact_number(each, number_width)})"


def chart_grid(summaries, width):
    gap = 3
    left_width = (width - gap) // 2
    right_width = width - gap - left_width
    right = []
    for key, title in (("decode_tokens_per_second", "Total decode~"),
                       ("prefill_tokens_per_second", "Total prefill~")):
        plot = braille_plot([(int(n), aggregate_phase(s, n, key)) for n, s in summaries],
                            width=1, height=4, errors={int(n): s.get("phase_averages", {}).get("aggregate_" + ("decode_stdev" if key.startswith("decode") else "prefill_stdev")) for n, s in summaries})
        gutter = max(*map(len, axis_labels(plot)), len(middle_label(plot)))
        plot = braille_plot([(int(n), aggregate_phase(s, n, key)) for n, s in summaries],
                            width=right_width - gutter - 2, height=4, errors={int(n): s.get("phase_averages", {}).get("aggregate_" + ("decode_stdev" if key.startswith("decode") else "prefill_stdev")) for n, s in summaries})
        lines = chart_lines(plot, title, "tok/s")
        lines += [""] * (8 - len(lines))
        if right:
            right.append("")
        right.extend(lines)
    plot = braille_plot([(int(n), s["score"]) for n, s in summaries],
                        width=1, height=len(right) - 4, errors={int(n): s.get("score_stdev") for n, s in summaries})
    gutter = max(*map(len, axis_labels(plot)), len(middle_label(plot)))
    plot = braille_plot([(int(n), s["score"]) for n, s in summaries],
                        width=left_width - gutter - 2, height=len(right) - 4, errors={int(n): s.get("score_stdev") for n, s in summaries})
    left = chart_lines(plot, "RIPALmark", "points")
    left += [""] * (len(right) - len(left))
    return [a.ljust(left_width) + " " * gap + b for a, b in zip(left, right)]



def match_label(quality):
    if quality is None:
        return "—"
    percent = round(quality * 100, 1)
    grade = next(letter for threshold, letter in ((100, "A+"), (93, "A"), (90, "A-"),
        (87, "B+"), (83, "B"), (80, "B-"), (77, "C+"), (73, "C"), (70, "C-"),
        (67, "D+"), (63, "D"), (60, "D-"), (0, "F")) if percent >= threshold)
    return f"{percent:g}% ({grade})"


def result_card(report, width=98):
    width = max(sum(size for _, size in CARD_COLUMNS) + 2 * (len(CARD_COLUMNS) - 1) + 4, width)
    body = []
    def add(text=""):
        body.append(safe_text(text)[:width - 4])
    heading = ("RealMark Partial Results — SERVER OVERWHELMED" if report.get("termination_kind") == "overload" else
               "RealMark Partial Results — INTERRUPTED" if report.get("interrupted") else "RealMark Results")
    add(f"{heading}  ·  {report['model']}")
    if report.get("terminated") or report.get("interrupted"):
        if report.get("termination_reason"):
            add("Stopped: " + report["termination_reason"])
        sessions = [r["session"] for r in report.get("calibration", {}).get("rounds", [])]
        sessions += [s for r in report.get("warmup_runs", []) + report.get("runs", [])
                     for s in r["sessions"] + r.get("filler_sessions", [])]
        cancelled = sum(s.get("cancelled_requests", 0) for s in sessions)
        received = sum(r.get("received_response", False) for s in sessions for r in s.get("requests", []))
        observed = [s.get("observed_token_mix", {}) for s in sessions]
        fresh = sum(m.get("fresh_tokens", 0) for m in observed)
        output = sum(m.get("output_tokens", 0) for m in observed)
        add(f"Collected {received} responses · {cancelled} cancelled requests (not errors)")
        complete = sum(s.get("load_completed_tasks", 0) for s in sessions)
        unfinished = sum(s.get("unfinished_tasks", 0) for s in sessions)
        add(f"Load tasks across all phases: {complete} completed · {unfinished} unfinished")
        add(f"Observed tokens across all phases: fresh {fresh:,} · decode {output:,}")
        add("Partial batch scores are provisional; unfinished work gets no credit.")
        if report.get("unmeasured_lanes"):
            add("Not measured: " + ", ".join(f"{n}L" for n in report["unmeasured_lanes"]))
    if report.get("context_error"):
        add("Context setup: " + report["context_error"])
    add()
    summaries = sorted(report.get("summaries", {}).items(), key=lambda item: int(item[0]))
    def table_row(values):
        return "  ".join(str(value).rjust(size) for value, (_, size) in zip(values, CARD_COLUMNS))
    add(table_row([label for label, _ in CARD_COLUMNS]))
    for lanes, summary in summaries:
        ratio = summary["token_mix"].get("fresh_per_output")
        words = summary.get("word_mix", {}).get("input_per_output")
        approximate = summary.get("mix_excludes_cancelled_filler_usage", False)
        prefix = "~" if approximate else ""
        space = (11 - 3 - len(prefix)) // 2
        mix = prefix + compact_number(words, space) + "w/" + compact_number(ratio, space) + "t"
        planned = summary.get("planned_tasks", 0)
        complete = summary.get("load_completed_tasks", planned)
        completed = f"{complete}/{planned}"
        if len(completed) > 9:
            completed = f"{compact_number(complete, 4, 0)}/{compact_number(planned, 4, 0)}"
        quality = summary.get("edit_quality")
        match = match_label(quality)
        estimated = summary.get("time_limited")
        seconds = summary.get("estimated_full_seconds") if estimated else summary.get("mean_seconds")
        elapsed = ("~" if estimated else "") + compact_number(seconds, 7 if estimated else 8, 2)
        add(table_row([str(lanes) if len(str(lanes)) <= 3 else compact_number(int(lanes), 3, 0), compact_number(summary["score"], 9),
                       card_rate(summary, lanes, "decode_tokens_per_second", 14),
                       card_rate(summary, lanes, "prefill_tokens_per_second", 15),
                       completed, elapsed, match, mix]))
    add()
    if summaries:
        for line in chart_grid(summaries, width - 4):
            add(line)
    else:
        add("No measured scores available.")
    add()
    calibration = report.get("calibration", {})
    if calibration.get("error"):
        add("Calibration: " + calibration["error"])
    for lanes, summary in summaries:
        if summary.get("time_limited"):
            elapsed = summary.get("mean_seconds", 0)
            projected = summary.get("estimated_full_seconds")
            estimate = f"{projected:.1f}s projected full batch" if projected is not None else "no completed tasks to project"
            add(f"C={lanes} time cap: {elapsed:.1f}s observed; {estimate}.")
        if summary.get("context_limited_sessions"):
            add(f"C={lanes} context limit: {summary['context_skipped_tasks']} tasks not started; scores use partial work.")
    failures = api_error_counts([s for r in report.get("runs", []) for s in r["sessions"] + r.get("filler_sessions", [])])
    if failures:
        add("API errors: " + ", ".join(f"{kind}={count}" for kind, count in sorted(failures.items())))
    return "\n".join(["╭" + "─" * (width - 2) + "╮"] +
                     ["│ " + line.ljust(width - 4) + " │" for line in body] +
                     ["╰" + "─" * (width - 2) + "╯"])


class FillerProgress:
    def __init__(self, ui):
        self.ui = ui

    def lane(self, index, **changes):
        for key in ("task", "completed", "finished", "failures"):
            changes.pop(key, None)
        if "stage" in changes:
            changes["stage"] = "filler/" + changes["stage"]
        if changes.get("status") not in ("waiting", "validating"):
            changes["status"] = "filler"
        self.ui.lane(index, **changes)

    def request_done(self, *args):
        self.ui.request_done(*args)

    def log(self, *args, **kwargs):
        self.ui.log(*args, **kwargs)

    def difference(self, expected, received):
        self.ui.difference(expected, received, prefix="FILLER ")


class FillerClient:
    is_filler = True

    def __init__(self, client, finished):
        self.client = client
        self.control = FillerControl(client.control, finished)
        self.progress = FillerProgress(client.progress) if getattr(client, "progress", None) else None

    def __getattr__(self, name):
        return getattr(self.client, name)

    def complete(self, messages, stage, config, on_chunk=None):
        if getattr(self.client, "supports_stream_cancel", False):
            kwargs = {"cancel_control": self.control}
            if on_chunk is not None:
                kwargs["on_chunk"] = on_chunk
            return self.client.complete(messages, stage, config, **kwargs)
        return self.client.complete(messages, stage, config)


class PhaseProgress:
    def __init__(self, ui, phase, offset):
        self.ui, self.phase, self.offset = ui, phase, offset

    def lane(self, index, **changes):
        for key in ("task", "finished"):
            if key in changes:
                changes[key] += self.offset
        if self.phase == "warmup":
            changes.pop("completed", None)
            changes.pop("failures", None)
            if "stage" in changes:
                changes["stage"] = "warmup/" + changes["stage"]
            if changes.get("status") == "completed":
                changes["status"] = "warmup done"
        self.ui.lane(index, **changes)

    def request_done(self, index, seconds, tokens):
        if self.phase == "warmup":
            self.ui.log(f"Warmup lane {index + 1} response {seconds:.2f}s (unscored)")
        else:
            self.ui.request_done(index, seconds, tokens)

    def log(self, *args, **kwargs):
        self.ui.log(*args, **kwargs)

    def difference(self, *args, **kwargs):
        self.ui.difference(*args, **kwargs)


class PhaseClient:
    def __init__(self, client, phase, offset=0):
        self.client = client
        self.phase = phase
        self.progress = PhaseProgress(client.progress, phase, offset) if getattr(client, "progress", None) else None

    def __getattr__(self, name):
        return getattr(self.client, name)


class StageClient:
    def __init__(self, client, control, task_gate=None):
        self.client, self.control = client, control
        self.task_gate = task_gate

    def __getattr__(self, name):
        return getattr(self.client, name)

    def complete(self, messages, stage, config, cancel_control=None, on_chunk=None):
        if getattr(self.client, 'supports_stream_cancel', False):
            kwargs = {'cancel_control': cancel_control or self.control}
            if on_chunk is not None:
                kwargs['on_chunk'] = on_chunk
            return self.client.complete(messages, stage, config, **kwargs)
        return self.client.complete(messages, stage, config)


class ContextLimitReached(Exception):
    pass


class ContextBudget:
    def __init__(self, client, tools, output_tokens, margin=128):
        self.client, self.tools = client, tools
        self.limit = getattr(client, "context_window", None)
        self.output_tokens, self.margin = output_tokens, margin
        self.tokenizer_available = None
        self.source = "server tokenizer"
        self.peak_prompt = 0
        self.peak_reserved = 0

    def count(self, messages):
        if self.tokenizer_available is not False and hasattr(self.client, "count_prompt_tokens"):
            try:
                control = getattr(self.client, "control", None)
                operation = lambda: self.client.count_prompt_tokens(messages)
                count = control.call(operation, self.client.timeout, "tokenizer") if control else operation()
                if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                    raise ValueError("Invalid token count")
                self.tokenizer_available = True
                return count
            except RunCancelled:
                raise
            except Exception as exc:
                if control and control.stopped:
                    raise RunCancelled(control.reason)
                if isinstance(exc, HTTPError) and exc.code == 429:
                    control = getattr(self.client, "control", None)
                    if control:
                        control.terminate("overload", "HTTP 429 from tokenizer endpoint")
                        raise RunCancelled(control.reason)
                self.tokenizer_available = False
        self.source = "conservative UTF-8 size estimate (tokenizer unavailable)"
        encoded = json.dumps({"messages": messages, "tools": self.tools}, ensure_ascii=False).encode()
        return len(encoded) + 32 * len(messages) + 128

    def check(self, messages, label="request"):
        if not self.limit:
            return None
        count = self.count(messages)
        reserved = count + self.output_tokens + self.margin
        self.peak_prompt = max(self.peak_prompt, count)
        self.peak_reserved = max(self.peak_reserved, reserved)
        if reserved > self.limit:
            raise ContextLimitReached(f"{label}: estimated {count:,} input + {self.output_tokens:,} output "
                                      f"+ {self.margin:,} margin exceeds {self.limit:,}-token context")
        return count

    def check_task(self, messages, task, padding):
        preview = list(messages)
        for stage, arguments, result in [
            ("search_records", {"id": task["target"]["id"]}, {"path": task["path"]}),
            ("read_file", {"path": task["path"]}, {"contents": task["contents"], "read_padding": padding,
                          "transformation": task["transformation"], "instructions": task["instructions"]}),
        ]:
            call_id = "budget-" + "x" * 100
            preview.append({"role": "assistant", "content": None, "tool_calls": [{
                "id": call_id, "type": "function", "function": {"name": stage, "arguments": json.dumps(arguments)}}]})
            preview.append({"role": "tool", "tool_call_id": call_id,
                            "content": json.dumps({**result, "validation_error": " " * 512})})
        # No transformed answer is supplied, even to the tokenizer endpoint.
        return self.check(preview, "next task")

    def snapshot(self):
        return {"window": self.limit, "source": self.source, "output_reserve": self.output_tokens,
                "margin": self.margin, "peak_estimated_prompt": self.peak_prompt,
                "peak_reserved_context": self.peak_reserved}


DEFAULT_TASK_SCHEDULE = [(1, 16), (2, 8), (4, 4)]
SCORE_WORDS = 1000
SCORE_REFERENCE_SECONDS = 100


def schedule_entry(value):
    try:
        concurrency, tasks = (int(part) for part in value.split(':'))
        if concurrency < 1 or tasks < 1:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError('use C:TASKS with positive integers, e.g. 1:16') from None
    return concurrency, tasks


def tasks_for_lanes(lanes, schedule):
    if len({level for level, _ in schedule}) != len(schedule):
        raise ValueError('task schedule has duplicate concurrency thresholds')
    eligible = [(level, count) for level, count in schedule if level <= lanes]
    if not eligible:
        raise ValueError(f'task schedule has no entry for concurrency {lanes}')
    return max(eligible)[1]


def stage_budget(warmups, workload, tasks, prefill_rate=1000):
    """Forecast fresh input growth and allow observed decode time in parallel."""
    fresh_total, decode_seconds = 0, 0.0
    sources = set()
    for index, lane in enumerate(workload):
        predicted_output = None
        warm = next((s for s in warmups if s['session'] == index), None)
        requests = warm.get('requests', []) if warm else []
        by_task = {}
        for request in requests:
            tokens = request.get('tokens', {})
            if tokens.get('available'):
                by_task[request['task']] = by_task.get(request['task'], 0) + tokens['fresh_tokens']
        completed = warm.get('load_completed_tasks', 0) if warm else 0
        complete_usage = completed and len(by_task) == completed and all(
            r.get('tokens', {}).get('available') for r in requests)
        if complete_usage and warm.get('generation_adjustments'):
            event = warm['generation_adjustments'][-1]
            last = [r for r in requests if r['task'] == max(by_task)]
            last_output = sum(r['tokens']['output_tokens'] for r in last)
            words = event['source_words']
            per_word = event['output_tokens_per_word']
            overhead = max(0, last_output - per_word * words)
            predicted_output = (per_word * warm['next_words'] + overhead) * tasks
            fresh_total += 4 * predicted_output
            sources.add('adaptive generation forecast at target 4:1')
        elif complete_usage:
            values = [by_task[t] for t in sorted(by_task)]
            growth = max(0, (values[-1] - values[0]) / max(1, len(values) - 1))
            fresh_total += values[-1] * tasks + growth * tasks * (tasks + 1) / 2
            sources.add('warmup fresh tokens with observed per-task growth')
        else:
            # Without usable warmup counters, conservatively model uncached history
            # growing through each full three-request tool cycle. No answers leave
            # the harness, and tokenization calls are not generation work.
            history = 512
            for task in lane[-tasks:]:
                data = len(json.dumps({'contents': task['contents'], 'read_padding': task['read_padding'],
                                       'instructions': task['instructions']})) / 3
                output = len(task['target']['payload']) / 2 + 96
                fresh_total += 3 * history + data + 128
                history += data + output + 128
            sources.add('uncached text-size forecast (warmup usage unavailable)')
        if completed:
            samples = [(r.get('phase_timing') or {}).get('generation_seconds') for r in requests]
            if samples and all(value is not None and math.isfinite(value) for value in samples):
                decode = (sum(samples) / sum(r["tokens"]["output_tokens"] for r in requests) * predicted_output
                          if predicted_output is not None else sum(samples) / completed * tasks)
            else:
                decode = 0
                sources.add('decode allowance unavailable without streaming timings')
            decode_seconds = max(decode_seconds, decode)
    raw_seconds = fresh_total / prefill_rate + decode_seconds
    seconds = max(60, math.ceil(raw_seconds / 60) * 60)
    return {'seconds': seconds, 'estimated_fresh_tokens': round(fresh_total),
            'prefill_floor_tokens_per_second': prefill_rate,
            'decode_allowance_seconds': decode_seconds, 'unrounded_seconds': raw_seconds,
            'source': '; '.join(sorted(sources)), 'rounding': 'up to whole minutes'}


def extrapolate_seconds(seconds, completed, planned):
    return seconds * planned / completed if completed > 0 else None


WORD_PATTERN = re.compile(r'\w+', re.UNICODE)
SYMBOL_PATTERN = re.compile(r'[^\w\s]', re.UNICODE)


def weighted_words(text):
    if not isinstance(text, str):
        return 0.0
    return len(WORD_PATTERN.findall(text)) + .25 * len(SYMBOL_PATTERN.findall(text))


def generated_words(message):
    total = weighted_words(message.get('content'))
    for call in message.get('tool_calls') or []:
        arguments = (call.get('function') or {}).get('arguments', '')
        total += weighted_words(arguments if isinstance(arguments, str) else json.dumps(arguments, ensure_ascii=False))
    return total


TOOL_OVERHEAD_WORDS = sum(weighted_words(json.dumps(args)) for args in (
    {'id': 'record-0'}, {'path': '/workspace/session-0/file-0.jsonl'},
    {'id': 'record-0', 'revision': 1, 'text': ''}))


def projected_words(session):
    remaining = max(0, session['planned_tasks'] - len(session['task_results']))
    return session['load_completed_words'] + remaining * (session['next_words'] + TOOL_OVERHEAD_WORDS)


def word_mix(events, session, after=None, through=None):
    incoming = outgoing = 0.0
    for event in events:
        if event['session'] != session:
            continue
        task = event.get('task', 0)
        if after is not None and task <= after:
            continue
        if through is not None and task > through:
            continue
        if event['type'] in ('session_start', 'messages'):
            incoming += sum(weighted_words(m.get('content')) for m in event.get('messages', [])
                            if m.get('role') != 'assistant')
        elif event['type'] == 'response':
            choices = event['body'].get('choices') or []
            if choices:
                outgoing += generated_words(choices[0].get('message') or {})
    return {'input_words': incoming, 'output_words': outgoing,
            'input_per_output': incoming / outgoing if outgoing else None}


def combine_word_mix(items):
    items = list(items)
    incoming = sum(item.get('input_words', 0) for item in items)
    outgoing = sum(item.get('output_words', 0) for item in items)
    return {'input_words': incoming, 'output_words': outgoing,
            'input_per_output': incoming / outgoing if outgoing else None}


SCHEMA = 'ripal-telemetry-v1'
PHASES = ('warmup', 'run', 'filler', 'partial')


class Telemetry:
    def __init__(self):
        self.lock = threading.Lock()
        self.events = []
        self.wall, self.monotonic = time.time(), time.perf_counter()
        self.uid = f'{time.time_ns():x}'
        self.sessions = 0

    def timestamp(self, value=None):
        return self.wall + ((time.perf_counter() if value is None else value) - self.monotonic)

    def emit(self, session, phase, kind, at=None, **data):
        with self.lock:
            seq = len(self.events)
            observed = time.perf_counter() if at is None else at
            item = {'id': f'{self.uid}:{seq}', 'time': self.timestamp(observed), 'elapsed': observed - self.monotonic,
                    'session': session, 'phase': phase, 'type': kind, **data}
            self.events.append(item)
        return item

    def start(self, phase, **data):
        with self.lock:
            self.sessions += 1
            session = f'{self.uid}:{self.sessions}'
        self.emit(session, phase, 'session_start', clock_origin=self.monotonic, time_origin=self.wall, **data)
        return session

    def for_session(self, session):
        with self.lock:
            return [e for e in self.events if e['session'] == session]


def export_report(args, report, protocol, scoring_reference):
    """Persist raw input/output and scheduling parameters, never computed scores."""
    parameters = json.loads(json.dumps(vars(args), default=str))
    raw = {'schema': SCHEMA, 'config': {
        'benchmark_version': report['version'], 'model': report['model'], 'base_url': report['base_url'],
        'parameters': parameters, 'workload': report.get('config', {}),
        'context': report.get('model_context'), 'protocol': protocol,
        'scoring_reference': scoring_reference, 'timestamp_unit': 'Unix seconds (monotonic elapsed time)'}, 'events': [], 'runs': {}}
    if report.get('terminated') or report.get('interrupted'):
        raw['events'].append({'type': 'run_stopped', 'time': time.time(),
                              'kind': report.get('termination_kind'), 'reason': report.get('termination_reason')})
    if report.get('context_error'):
        raw['events'].append({'type': 'setup_error', 'time': time.time(), 'message': report['context_error']})

    def stage_for(c):
        return raw['runs'].setdefault(f'c={c}', {'time': None, 'stages': [],
            'lanes': [{'lane': lane + 1, 'telemetry': {p: [] for p in PHASES}} for lane in range(c)]})

    def append(stage, sessions, trial, limits=None):
        for session in sessions:
            lane = stage['lanes'][session['session']]
            known = {e['id'] for events in lane['telemetry'].values() for e in events}
            for original in session.get('telemetry', []):
                if original['id'] in known:
                    continue
                event = {**original, 'trial': trial}
                if event['phase'] == 'run' and limits is not None and event.get('task', 0) > limits.get(session['session'], 10**9):
                    event['phase'] = 'partial'
                lane['telemetry'][event['phase']].append(event)
                known.add(event['id'])

    for run in report.get('runs', []):
        c, trial = run['lanes'], run.get('repeat', 1)
        stage = stage_for(c)
        bounds = run.get('time')
        if bounds:
            stage['time'] = [min(stage['time'][0], bounds[0]), max(stage['time'][1], bounds[1])] if stage['time'] else bounds
        budget = run.get('stage_budget', {})
        stage['stages'].append({'trial': trial, 'time': bounds, 'measured_time': run.get('measured_time'), 'clock': run['clock'], 'measured_clock': run['measured_clock'],
            'params': {'tasks': run.get('tasks_per_session', report.get('config', {}).get('tasks')),
                       'response_timeout_seconds': run.get('tool_timeout_seconds'),
                       'stage_timeout_seconds': budget.get('seconds')},
            'cap': {'task': budget.get('drain_task'), 'completion_limits': budget.get('completion_limits')}
                if 'drain_task' in budget else None})
        append(stage, run['sessions'] + run.get('filler_sessions', []), trial, budget.get('completion_limits'))
    for warm in report.get('warmup_runs', []):
        stage = stage_for(warm['lanes'])
        trial = warm.get('repeat', 1)
        append(stage, warm['sessions'] + warm.get('filler_sessions', []), trial)
        if not any(s['trial'] == trial for s in stage['stages']):
            stage['stages'].append({'trial': trial, 'time': None, 'measured_time': None, 'clock': None, 'measured_clock': None, 'params': {}, 'cap': None})
    for index, pilot in enumerate(report.get('calibration', {}).get('rounds', [])):
        stage = stage_for(1)
        append(stage, [pilot['session']], -(index + 1))
        stage['stages'].append({'trial': -(index + 1), 'time': None, 'measured_time': None, 'clock': None, 'measured_clock': None,
                                'params': {'calibration': True}, 'cap': None})
    return raw


class VirtualClock:
    def __init__(self, prefill, decode, response_timeouts=False):
        self.prefill, self.decode = prefill, decode
        self.enforce_response_deadlines = response_timeouts
        self.now, self.epoch, self.serial = 0.0, real_time.time(), 0
        self.local = threading.local()
        self.condition = threading.Condition()
        self.active = {0}
        self.jobs = {}
        self.stage_control = None
        self.lanes = 1

    def __enter__(self):
        self.previous = clock.backend
        clock.backend = self
        return self

    def __exit__(self, *exc):
        clock.backend = self.previous

    def begin_profile(self, lanes, control):
        with self.condition:
            self.lanes, self.active, self.stage_control = lanes, set(range(lanes)), control
            self.condition.notify_all()

    def enter_lane(self, lane):
        self.local.lane = lane

    def pause_lane(self, lane):
        with self.condition:
            self.active.discard(lane)
            self.condition.notify_all()

    def resume_lanes(self):
        with self.condition:
            self.active = set(range(self.lanes))
            self.condition.notify_all()

    def invoke(self, control, operation, timeout, label):
        control.check()
        previous = getattr(self.local, 'invocation', None)
        self.local.invocation = (control, self.now + timeout if self.enforce_response_deadlines and timeout is not None else float('inf'), label, timeout)
        try:
            return operation()
        finally:
            self.local.invocation = previous

    def submit(self, body, prompt, cached, arguments, on_chunk):
        control, deadline, label, timeout = self.local.invocation
        lane = getattr(self.local, 'lane', 0)
        count = body['usage']['completion_tokens']
        job = {'lane': lane, 'body': body, 'start': self.now, 'control': control, 'deadline': deadline,
               'label': label, 'timeout': timeout, 'prefill': prompt - cached, 'phase': 'prefill',
               'tokens': count, 'produced': 0, 'credit': 0.0, 'first': None, 'last': None,
               'arguments': arguments, 'pending': '', 'on_chunk': on_chunk, 'done': False, 'error': None}
        with self.condition:
            self.jobs[lane] = job
            self.condition.notify_all()
            while not job['done']:
                self._cancel()
                if job['done']:
                    break
                if self.active.issubset(self.jobs):
                    self._advance()
                else:
                    self.condition.wait()
            if job['error']:
                raise job['error']
            return job['body']

    def _cancel(self):
        for lane, job in list(self.jobs.items()):
            if job['control'].stopped:
                job['error'] = RunCancelled(job['control'].reason)
            elif self.now >= job['deadline'] - 1e-9:
                message = f"{job['label']} exceeded the {job['timeout']:g}s response deadline (virtual model)"
                job['control'].terminate('overload', message)
                job['error'] = ToolDeadlineExceeded(message)
            if job['error']:
                job['done'] = True
                del self.jobs[lane]
                self.condition.notify_all()

    def _chunk(self, job, data):
        if job['on_chunk']:
            job['on_chunk'](data if isinstance(data, str) else json.dumps(data), self.now)

    def _advance(self):
        if not self.jobs:
            return
        prefill = [j for j in self.jobs.values() if j['phase'] == 'prefill']
        decode = [j for j in self.jobs.values() if j['phase'] == 'decode']
        p = self.prefill / max(1, len(prefill))
        d = self.decode / max(1, len(decode))
        candidates = [max(0, j['prefill']) / p for j in prefill]
        candidates += [max(0, min(j['tokens'], ((j['produced'] - 1) // 32 + 1) * 32 + 1)
                           - j['produced'] - j['credit']) / d for j in decode]
        candidates += [max(0, j['deadline'] - self.now) for j in self.jobs.values()]
        if self.stage_control and self.stage_control.deadline and self.stage_control.deadline > self.now + 1e-9:
            candidates.append(self.stage_control.deadline - self.now)
        delta = min(candidates)
        for job in prefill:
            job['prefill'] -= p * delta
        for job in decode:
            job['credit'] += d * delta
        self.now += delta
        self._cancel()
        for job in prefill:
            if job['lane'] in self.jobs and job['prefill'] <= 1e-7:
                job['phase'] = 'decode'
                # Prefill produces logits for the first generated token. The
                # remaining token intervals run at the configured decode rate.
                self._emit_token(job)
        for job in decode:
            if job['lane'] not in self.jobs:
                continue
            while job['credit'] >= 1 - 1e-7 and job['produced'] < job['tokens']:
                job['credit'] -= 1
                self._emit_token(job)

    def _emit_token(self, job):
        n, total = job['produced'], job['tokens']
        text = job['arguments'][len(job['arguments']) * n // total:len(job['arguments']) * (n + 1) // total]
        job['pending'] += text
        if n == 0:
            job['first'] = self.now
        job['last'] = self.now
        job['produced'] += 1
        if n == 0 or n % 32 == 0 or job['produced'] == job['tokens']:
            function = {'arguments': job['pending']}
            call = {'index': 0, 'function': function}
            if n == 0:
                function['name'] = job['body']['choices'][0]['message']['tool_calls'][0]['function']['name']
                call.update(id=job['body']['choices'][0]['message']['tool_calls'][0]['id'], type='function')
            self._chunk(job, {'choices': [{'index': 0, 'delta': {'tool_calls': [call]}}]})
            job['pending'] = ''
        if job['produced'] == job['tokens']:
            job['body']['_stream_timing'] = {'source': 'virtual model streaming observations',
                'first_output_seconds': job['first'] - job['start'],
                'generation_seconds': job['last'] - job['first'], 'response_seconds': self.now - job['start']}
            self._chunk(job, {'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'tool_calls'}]})
            self._chunk(job, {'choices': [], 'usage': job['body']['usage']})
            self._chunk(job, '[DONE]')
            job['done'] = True
            del self.jobs[job['lane']]
            self.condition.notify_all()



class VirtualModel:
    supports_stream_cancel = True
    supports_telemetry = True

    def __init__(self, runtime, tools, transform, context_window=262144, cache_block=8192):
        self.virtual_clock, self.tools, self.transform = runtime, tools, transform
        self.context_window, self.cache_block = context_window, cache_block
        self.extra, self.cache_policy, self.streaming, self.timeout = {}, 'server-default', True, 60
        self.previous_prompts = {}
        self.message_units = {}
        self.base_units = weighted_words(json.dumps({"messages": [], "tools": tools}, ensure_ascii=False))
        self.request_counter = 0
        self.lock = threading.Lock()

    def discover_context_limit(self):
        return {'limit': self.context_window, 'source': 'virtual model'}

    def count_prompt_tokens(self, messages):
        units = self.base_units + .25 * max(0, len(messages) - 1)
        for message in messages:
            calls = tuple((call.get('id'), call.get('type'), (call.get('function') or {}).get('name'),
                           (call.get('function') or {}).get('arguments')) for call in message.get('tool_calls') or [])
            key = (tuple(sorted(message)), message.get('role'), message.get('content'),
                   message.get('tool_call_id'), message.get('reasoning_content'), calls)
            cached = self.message_units.get(key)
            if cached is None:
                cached = weighted_words(json.dumps(message, ensure_ascii=False))
                self.message_units[key] = cached
            units += cached
        return math.ceil(units)

    def complete(self, messages, stage, config, cancel_control=None, on_chunk=None):
        target = None
        for message in messages:
            if message['role'] == 'user' and message.get('content', '').startswith(('Start with task ID ', 'Continue with task ID ')):
                target = message['content'].split()[-1].rstrip('.')
            elif message['role'] == 'tool':
                result = json.loads(message['content'])
                if result.get('next_task_id'):
                    target = result['next_task_id']
        if stage == 'search_records':
            args = {'id': target}
        elif stage == 'read_file':
            found = next(json.loads(m['content']) for m in reversed(messages)
                         if m['role'] == 'tool' and 'path' in json.loads(m['content']))
            args = {'path': found['path']}
        else:
            found = next(json.loads(m['content']) for m in reversed(messages)
                         if m['role'] == 'tool' and 'contents' in json.loads(m['content']))
            record = next(json.loads(line) for line in found['contents'].splitlines() if json.loads(line)['id'] == target)
            args = {'id': target, 'revision': record['revision'], 'text': self.transform(record['payload'], found['transformation'])}
        arguments = json.dumps(args, ensure_ascii=False)
        prompt = self.count_prompt_tokens(messages)
        key = messages[1]['content']
        with self.lock:
            previous = self.previous_prompts.get(key, 0)
            cached = min(prompt, max(0, previous - 4) // self.cache_block * self.cache_block)
            self.previous_prompts[key] = prompt
            self.request_counter += 1
            call_id = f'virtual-{self.request_counter}'
        if self.cache_policy == 'disabled':
            cached = 0
        body = {'choices': [{'finish_reason': 'tool_calls', 'message': {'role': 'assistant', 'content': None,
                 'tool_calls': [{'id': call_id, 'type': 'function', 'function': {'name': stage, 'arguments': arguments}}]}}],
                'usage': {'prompt_tokens': prompt, 'completion_tokens': max(1, math.ceil(weighted_words(arguments))),
                          'prompt_tokens_details': {'cached_tokens': cached}}}
        response = self.virtual_clock.submit(body, prompt, cached, arguments, on_chunk if self.streaming else None)
        if not self.streaming:
            response.pop('_stream_timing', None)
        return response


VERSION = "ripal-v11"
TARGET_RATIO = 4.0
WORDS = """amber birch cedar delta ember fern garden harbor island jungle kettle lantern
maple nectar olive pebble quartz river silver timber umber valley willow yellow zephyr
acorn anchor apple apricot arrow autumn avocado azure badge bamboo banana barrel
basket beach beacon beetle berry blanket blossom bottle branch breeze bridge brook
brush bucket button cabin cactus candle canvas carrot castle cherry chestnut chimney
circle citrus clover cloud cobalt coconut coral cotton cradle crane cricket crystal
cucumber daisy dandelion desert diamond dolphin dragon drawer drum eagle earth echo
elm emerald engine falcon feather fiddle field finch flame fleece flower flute forest
fossil fountain fox freckle frost galaxy garlic gate ginger glacier globe goose grape
grass gravel grove guitar hammock hammer hazel hedgehog helmet heron hickory honey
horse iceberg igloo indigo insect iris iron ivy jacket jasmine jewel jigsaw journey
juniper kernel kestrel kitchen kitten kiwi ladder lagoon lake lattice lavender leaf
lemon leopard letter light lilac lily linen lizard lobster lotus magnet mango marble
marigold meadow melon metal mirror mist monkey moon moss mountain mushroom mustard
napkin needle nickel noodle nutmeg ocean octopus onion orange orchid otter oyster
paddle palace panda paper parsley peach peanut pear pelican pencil pepper petal piano
pickle pigeon pillow pine planet plum pocket pond potato prairie prism pumpkin rabbit
raccoon radish rain raven ribbon ripple robin rocket rope rosemary ruby saddle saffron
sail salmon sand satin scarf shell shield shore shrimp shrub skylark slate sleeve
snail snow sparrow spider spinach sponge spruce squirrel star stone storm strawberry
stream summit sunflower swallow swan tablet tangerine temple thimble thistle thunder
tiger tomato torch tower tractor trail trumpet tulip turtle umbrella uniform urn
vanilla velvet violet walnut water wave weasel wheat wheel whistle window winter
wizard wolf wood wren yarn yeast zebra zipper zucchini""".split()
VOWELS = "aeiouAEIOU"
PIG_LATIN_WORDS = [word for word in WORDS if word[0] not in VOWELS]
TRANSFORMATIONS = {
    "pig_latin": "Move the first letter of EACH word to its end, then append 'ay': "
                 "'bright little lantern' -> 'rightbay ittlelay anternlay'. "
                 "Use exactly word[1:] + word[0] + 'ay'; do not use conventional Pig Latin rules.",
    "reverse_words": "Reverse the letters within EACH word, preserving word order and letter case: "
                     "'Hello world' -> 'olleH dlrow'.",
    "remove_vowels": "Remove every a, e, i, o, u (uppercase or lowercase) from EACH word. "
                     "Keep y and preserve remaining letters, their case, and word order: "
                     "'Hello world' -> 'Hll wrld'.",
    "invert_case": "Swap the case of EVERY letter: lowercase becomes uppercase and uppercase becomes lowercase. "
                   "Keep letter order and word order: 'Mike hello HELlo' -> 'mIKE HELLO helLO'.",
}
TRANSFORMATION_CYCLE = tuple(TRANSFORMATIONS)
SYSTEM = """Complete the synthetic workspace tasks using the requested tools.
Search for the exact task ID, read its file, then submit the matching record's
revision and transformed payload. A file contains many distractor records.
The read_file result specifies the transformation and instructions for that task.
Apply ONLY that transformation to the target payload; do not combine operations
or reuse the previous task's operation. Transform every source word exactly once.
The output word count must be unchanged. Keep word order and single spaces.
Ignore read_padding; it is irrelevant filler.
These are mechanical text conversions. Use tool calls only; do not explain or plan.
Continue with next_task_id even when ok is false; harness_continuation data
is supplied to keep the workload moving after validation errors."""


def tool(name, description, properties):
    return {"type": "function", "function": {
        "name": name, "description": description,
        "parameters": {"type": "object", "properties": properties,
                       "required": list(properties), "additionalProperties": False}}}


TOOLS = [
    tool("search_records", "Find the file containing an exact record ID.",
         {"id": {"type": "string"}}),
    tool("read_file", "Read a workspace file and its task-specific transformation instructions.",
         {"path": {"type": "string"}}),
    tool("submit_result", "Submit the transformed payload of the target record.",
         {"id": {"type": "string"}, "revision": {"type": "integer"},
          "text": {"type": "string"}}),
]


@dataclass(frozen=True)
class Config:
    tasks: int = 8
    records: int = 2
    words: int = 5
    filler_words: int = 1536
    seed: int = 42
    max_tokens: int = 2048
    retries: int = 0
    strict: bool = False
    adaptive_mix: bool = True
    history_mode: str = "full"
    filler: bool = True


def transform(text, transformation="pig_latin"):
    words = text.split()
    if transformation == "pig_latin":
        return " ".join(word[1:] + word[0] + "ay" for word in words)
    if transformation == "reverse_words":
        return " ".join(word[::-1] for word in words)
    if transformation == "remove_vowels":
        return " ".join("".join(char for char in word if char not in VOWELS) for word in words)
    if transformation == "invert_case":
        return " ".join(word.swapcase() for word in words)
    raise ValueError(f"Unknown transformation: {transformation}")


def mixed_case(word, rng):
    style = rng.randrange(4)
    if style == 0:
        return word
    if style == 1:
        return word.upper()
    if style == 2:
        return word.capitalize()
    return "".join(char.upper() if index % 2 == 0 else char for index, char in enumerate(word))


def resize_task(task, config, words):
    transformation = task["transformation"]
    vocabulary = PIG_LATIN_WORDS if transformation == "pig_latin" else WORDS
    metadata = random.Random(task["record_seed"] + ":metadata")
    records = []
    for index in range(config.records):
        record_id = f"record-{metadata.getrandbits(64):016x}"
        revision = metadata.randrange(1, 1000)
        rng = random.Random(task["record_seed"] + f":payload:{index}")
        payload = rng.choices(vocabulary, k=words)
        if transformation == "invert_case":
            payload = [mixed_case(word, rng) for word in payload]
        records.append({"id": record_id, "revision": revision, "payload": " ".join(payload)})
    task.update(target=records[metadata.randrange(len(records))], words=words,
                contents="\n".join(json.dumps(record) for record in records))
    return task


def make_session(config, session, task_offset=0):
    tasks = []
    for task_index in range(config.tasks):
        transformation = TRANSFORMATION_CYCLE[(task_offset + task_index) % len(TRANSFORMATION_CYCLE)]
        task = {"record_seed": f"{VERSION}:{config.seed}:{session}:{task_index}:records",
                "path": f"/workspace/session-{session}/file-{task_index}.jsonl",
                "transformation": transformation, "instructions": TRANSFORMATIONS[transformation],
                "padding_seed": f"{config.seed}:{session}:{task_index}:padding",
                "read_padding": " ".join(random.Random(f"{config.seed}:{session}:{task_index}:padding").choices(
                    WORDS, k=config.filler_words))}
        tasks.append(resize_task(task, config, config.words))
    return tasks


def request_tokens(response, cache_policy="server-default"):
    usage = response.get("usage") or {}
    prompt, output = usage.get("prompt_tokens"), usage.get("completion_tokens")
    cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
    # Missing cache accounting is unknown, unless the operator declares caching off.
    if cached is None and cache_policy == "disabled":
        cached = 0
    def count(value):
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    if not all(count(value) for value in (prompt, output, cached)) or cached > prompt:
        return {"available": False, "requests": 1, "reason": "Missing or invalid prompt/completion/cached token counters"}
    return {"available": True, "requests": 1, "prompt_tokens": prompt,
            "cached_tokens": cached, "fresh_tokens": prompt - cached, "output_tokens": output}


def token_mix(items):
    items = list(items)
    requests = sum(item.get("requests", 0) for item in items)
    if not items or not all(item.get("available") for item in items):
        return {"available": False, "requests": requests,
                "reason": "Complete fresh-input accounting is unavailable"}
    totals = {field: sum(item[field] for item in items) for field in (
        "prompt_tokens", "cached_tokens", "fresh_tokens", "output_tokens")}
    return {"available": True, "requests": requests, **totals,
            "fresh_per_output": totals["fresh_tokens"] / totals["output_tokens"] if totals["output_tokens"] else None}


def mix_matches(mix, target, tolerance):
    ratio = mix.get("fresh_per_output")
    return mix.get("available", False) and ratio is not None and abs(ratio / target - 1) <= tolerance


def emit(client, message, final=False):
    if getattr(client, "is_filler", False):
        message = "FILLER " + message
    ui = getattr(client, "progress", None)
    if ui:
        ui.log(message, final=final)
    else:
        print(message, flush=True)


def stop_requested(client):
    control = getattr(client, "control", None)
    return control is not None and control.stopped


def termination_state(client):
    control = getattr(client, "control", None)
    stopped = control is not None and control.stopped
    return {"terminated": stopped, "interrupted": stopped and control.kind == "interrupt",
            "termination_kind": control.kind if stopped else None,
            "termination_reason": control.reason if stopped else None}


def validation_issue(stage, actual_name, expected, supplied):
    """Bounded diagnostics; expected answer excerpts stay local, out of retries."""
    if actual_name != stage:
        return {"kind": "wrong_tool", "message": f"expected tool {stage}, got {actual_name}"}
    if not isinstance(supplied, dict):
        return {"kind": "invalid_arguments", "message": "tool arguments must be a JSON object"}
    if set(supplied) != set(expected):
        return {"kind": "argument_keys", "message": f"missing keys {sorted(set(expected) - set(supplied))}; "
                f"unexpected keys {sorted(set(supplied) - set(expected))}"}
    for key, value in expected.items():
        if type(supplied[key]) is type(value) and supplied[key] == value:
            continue
        if key == "text" and isinstance(supplied[key], str):
            wanted, received = value.split(), supplied[key].split()
            first = next((i for i, (a, b) in enumerate(zip(wanted, received)) if a != b), min(len(wanted), len(received)))
            wrong = sum(a != b for a, b in zip(wanted, received)) + abs(len(wanted) - len(received))
            message = (f"text mismatch: expected {len(wanted)} words, got {len(received)}; "
                       f"first mismatch at word {first + 1}; {wrong} differing word positions")
            if wanted == received:
                message = "text formatting mismatch: preserve exactly one space between words"
            return {"kind": "text_mismatch", "message": message, "first_word": first + 1,
                    "expected_words": len(wanted), "received_words": len(received),
                    "expected_excerpt": " ".join(wanted[max(0, first - 2):first + 3])[:180],
                    "received_excerpt": " ".join(received[max(0, first - 2):first + 3])[:180]}
        return {"kind": "field_mismatch", "message": f"incorrect {key}",
                "field": key, "expected_excerpt": repr(value)[:180], "received_excerpt": repr(supplied[key])[:180]}
    return None



def response_deadline(client, words, stage):
    virtual = getattr(client, "virtual_clock", None)
    if virtual and not virtual.enforce_response_deadlines:
        return None
    base = getattr(client, "timeout", 60)
    reference = max(1, getattr(client, "timeout_reference_words", 256))
    return base * max(1, words / reference) if stage == "submit_result" else base


class Client:
    supports_stream_cancel = True
    supports_telemetry = True

    def __init__(self, base_url, model, api_key="", timeout=60, extra=None, cache_policy="server-default", streaming=True):
        self.base_url = base_url.rstrip("/")
        self.url = base_url.rstrip("/") + "/chat/completions"
        self.model, self.api_key, self.timeout = model, api_key, timeout
        self.extra = extra or {}
        self.cache_policy = cache_policy
        self.streaming = streaming
        protected = {"model", "messages", "tools", "tool_choice", "stream", "n"}
        if protected.intersection(self.extra):
            raise ValueError(f"extra-body cannot override {sorted(protected)}")

    def json_request(self, url, body=None):
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(url, json.dumps(body).encode() if body is not None else None, headers)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def discover_context_limit(self):
        catalog = self.json_request(self.base_url + "/models")
        model = next((m for m in catalog.get("data", []) if m.get("id") == self.model), None)
        if model is None:
            raise ValueError(f"Model {self.model!r} not found in /models; use --context-window to supply its limit")
        for field in ("max_model_len", "context_length", "max_context_length", "context_window"):
            limit = model.get(field)
            if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
                return {"limit": limit, "source": f"/models {field}"}
        raise ValueError("Model metadata has no context limit; supply --context-window")

    def count_prompt_tokens(self, messages):
        root = self.base_url[:-3] if self.base_url.endswith("/v1") else self.base_url
        body = {"model": self.model, "messages": messages, "tools": TOOLS, "add_generation_prompt": True}
        for key in ("chat_template_kwargs", "chat_template"):
            if key in self.extra:
                body[key] = self.extra[key]
        result = self.json_request(root + "/tokenize", body)
        count = result.get("count")
        return count if count is not None else len(result["tokens"])

    def complete(self, messages, stage, config, cancel_control=None, on_chunk=None):
        body = {"temperature": 0, "max_tokens": config.max_tokens, **self.extra,
                "model": self.model, "messages": messages, "tools": TOOLS,
                "tool_choice": {"type": "function", "function": {"name": stage}},
                "parallel_tool_calls": False, "stream": self.streaming}
        if self.streaming:
            body["stream_options"] = {"include_usage": True}
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(self.url, json.dumps(body).encode(), headers)
        started = time.perf_counter()
        control = cancel_control or getattr(self, "control", None)
        if control:
            control.check()
        socket_timeout = self.timeout if self.streaming else response_deadline(self, config.words, stage)
        with urllib.request.urlopen(request, timeout=socket_timeout) as response:
            with control.watch_response(response) if control else nullcontext():
                if "text/event-stream" in getattr(response, "headers", {}).get("Content-Type", ""):
                    return read_stream(response, started, control, on_chunk)
                return json.load(response)


def run_session(client, config, tasks, session, state=None):
    started = time.perf_counter()
    state = state if state is not None else {}
    messages = state.get("messages")
    continued = messages is not None
    if messages is None:
        messages = [{"role": "system", "content": SYSTEM}]
    messages.append({"role": "user", "content": f"Start with task ID {tasks[0]['target']['id']}."})
    trace = state.get("trace") or Telemetry()
    phase = "filler" if getattr(client, "is_filler", False) else getattr(client, "phase", "warmup")
    phase = "run" if phase == "measured" else phase
    sid = trace.start(phase, params=asdict(config), continued=continued, parent=state.get("sid"),
                      messages=messages[-1:] if continued else list(messages))
    calls = retries = completed = attempted = exercised = 0
    usage, requests, task_results = {}, [], []
    error = None
    interrupted = False
    context_limited = False
    time_limited = False
    context_reason = None
    ui = getattr(client, "progress", None)
    control = getattr(client, "control", None)
    mix_control = MixController(state.get("padding", config.filler_words), config.adaptive_mix, config.history_mode,
                                TARGET_RATIO, getattr(client, "ratio_tolerance", .15))
    output_reserve = int(getattr(client, "extra", {}).get("max_completion_tokens", getattr(client, "extra", {}).get("max_tokens", config.max_tokens)))
    budget = state.get("budget") or ContextBudget(client, TOOLS, output_reserve)
    budget.client = client
    gate = getattr(client, "task_gate", None)
    generation = GenerationController(state.get("words", config.words), config.adaptive_mix)
    compact_before = False
    try:
        if state.get("context_reason"):
            raise ContextLimitReached(state["context_reason"])
        for index, task in enumerate(tasks):
            if gate:
                gate.begin(index + 1)
            if control:
                control.check()
            task_ok, aborted = True, False
            dependency_ok, quality_credit, edit = True, 0.0, None
            resize_task(task, config, generation.words)
            target = task["target"]
            words = task["words"]
            transformation = task["transformation"]
            if compact_before:
                messages = [{"role": "system", "content": SYSTEM},
                            {"role": "user", "content": f"Start with task ID {target['id']}."}]
                trace.emit(sid, phase, "history_reset", task=index + 1, messages=list(messages))
            padding_words = mix_control.padding
            padding = task["read_padding"] if padding_words == config.filler_words else " ".join(
                random.Random(task["padding_seed"]).choices(WORDS, k=padding_words))
            expected_text = transform(target["payload"], transformation)
            task_config = replace(config, words=words, max_tokens=max(config.max_tokens, len(expected_text.encode()) + 256))
            extra = getattr(client, "extra", {})
            budget.output_tokens = int(extra.get("max_completion_tokens", extra.get("max_tokens", task_config.max_tokens)))
            budget.check_task(messages, task, padding)
            attempted += 1
            trace.emit(sid, phase, "task_start", task=index + 1, fixture={**task, "read_padding": padding}, padding=padding)
            task_request_start = len(requests)
            if ui:
                ui.lane(session, padding=padding_words, words=words)
            next_id = tasks[index + 1]["target"]["id"] if index + 1 < len(tasks) else None
            expected = [
                ("search_records", {"id": target["id"]}, {"path": task["path"]}),
                ("read_file", {"path": task["path"]}, {"contents": task["contents"], "read_padding": padding,
                 "transformation": transformation, "instructions": task["instructions"]}),
                ("submit_result", {"id": target["id"], "revision": target["revision"], "text": expected_text},
                 {"ok": True, "next_task_id": next_id}),
            ]
            for stage, arguments, result in expected:
                for attempt in range(config.retries + 1):
                    if control:
                        control.check()
                    budget.check(messages, stage)
                    if stage == "submit_result":
                        quality_credit, edit = 0.0, None
                    if ui:
                        ui.lane(session, task=index + 1, stage=stage, attempt=attempt + 1, status="waiting")
                    entry = {"task": index + 1, "stage": stage, "attempt": attempt + 1,
                             "padding_words": padding_words, "transformation": transformation, "source_words": words,
                             "tokens": {"available": False, "requests": 1}, "valid": False}
                    requests.append(entry)
                    request_started = time.perf_counter()
                    deadline = response_deadline(client, words, stage)
                    entry["deadline_seconds"] = deadline
                    calls += 1
                    request_id = f"{sid}:request:{calls}"
                    trace.emit(sid, phase, "request", task=index + 1, request_id=request_id,
                               stage=stage, attempt=attempt + 1, context_messages=len(messages),
                               params={"temperature": 0, "max_tokens": task_config.max_tokens, **getattr(client, "extra", {}),
                                       "stream": getattr(client, "streaming", True), "deadline_seconds": deadline})
                    def chunk_event(payload, at, request_id=request_id, task_number=index + 1):
                        trace.emit(sid, phase, "stream_chunk", at=at, task=task_number,
                                   request_id=request_id, data=payload)
                    call = message = None
                    received = False
                    try:
                        operation = (lambda: client.complete(messages, stage, task_config, on_chunk=chunk_event)) if getattr(
                            client, "supports_telemetry", False) else (lambda: client.complete(messages, stage, task_config))
                        response = control.call(operation, deadline, stage) if control else operation()
                        received = True
                        trace.emit(sid, phase, "response", task=index + 1, request_id=request_id, body={k: v for k, v in response.items() if k != "_stream_timing"})
                        entry["tokens"] = request_tokens(response, getattr(client, "cache_policy", "server-default"))
                        entry["phase_timing"] = phase_timing(response, entry["tokens"])
                        entry["usage"] = {}
                        for key, value in (response.get("usage") or {}).items():
                            if isinstance(value, (int, float)):
                                usage[key] = usage.get(key, 0) + value
                                entry["usage"][key] = value
                            elif isinstance(value, dict):
                                for detail, count in value.items():
                                    if isinstance(count, (int, float)):
                                        name = f"{key}.{detail}"
                                        usage[name] = usage.get(name, 0) + count
                                        entry["usage"][name] = count
                        choice = response["choices"][0]
                        message = choice["message"]
                        entry["generated_words"] = generated_words(message)
                        tool_calls = message.get("tool_calls", [])
                        if len(tool_calls) == 1 and tool_calls[0].get("id"):
                            call = tool_calls[0]
                        if choice.get("finish_reason") == "length":
                            raise ValueError("generation reached max_tokens")
                        if message.get("content") and str(message["content"]).strip():
                            entry["response_excerpt"] = str(message["content"])[:240]
                            raise ValueError("expected tool calls only, without explanatory text")
                        if call is None:
                            raise ValueError("expected exactly one tool call")
                        function = call["function"]
                        try:
                            supplied = json.loads(function["arguments"])
                        except (ValueError, TypeError):
                            supplied = None
                            entry["arguments_excerpt"] = str(function.get("arguments"))[:240]
                        issue = validation_issue(stage, function["name"], arguments, supplied)
                        if stage == "submit_result" and isinstance(supplied, dict) and isinstance(supplied.get("text"), str):
                            edit = text_quality(arguments["text"], supplied["text"])
                            edit["received_words"] = len(supplied["text"].split())
                            edit["copied_payload"] = supplied["text"].split() == target["payload"].split()
                            entry["text_quality"] = edit
                            if issue:
                                issue["character_edit_distance"] = edit["distance"]
                                issue["text_similarity"] = edit["similarity"]
                                issue["message"] += f"; character edit distance {edit['distance']} ({edit['similarity']:.1%} text match)"
                            metadata_ok = (function["name"] == stage and set(supplied) == set(arguments)
                                           and supplied.get("id") == target["id"]
                                           and type(supplied.get("revision")) is int
                                           and supplied["revision"] == target["revision"])
                            quality_credit = edit["similarity"] if metadata_ok and not edit["copied_payload"] else 0.0
                    except RunCancelled as exc:
                        trace.emit(sid, phase, "request_error", task=index + 1, request_id=request_id,
                                   error={"type": type(exc).__name__, "message": str(exc), "kind": control.kind if control else None})
                        entry["seconds"] = time.perf_counter() - request_started
                        entry["cancelled"] = True
                        entry["received_response"] = False
                        raise
                    except Exception as exc:
                        trace.emit(sid, phase, "request_error", task=index + 1, request_id=request_id,
                                   error={"type": type(exc).__name__, "message": str(exc), "code": getattr(exc, "code", None)})
                        timed_out = isinstance(exc, TimeoutError) or (isinstance(exc, URLError) and isinstance(exc.reason, TimeoutError))
                        rate_limited = isinstance(exc, HTTPError) and exc.code == 429
                        if timed_out or rate_limited:
                            if control is None:
                                control = RunControl()
                                client.control = control
                            message = (f"HTTP 429 Too Many Requests during {stage}" if rate_limited else
                                       f"{stage} hit the {getattr(client, 'timeout', 60):g}s response deadline (possible server overload)")
                            control.terminate("overload", message)
                            entry.update({"seconds": time.perf_counter() - request_started,
                                          "received_response": rate_limited,
                                          "validation": {"kind": "server_overload" if rate_limited else "tool_timeout", "message": control.reason}})
                            raise RunCancelled(control.reason)
                        issue = {"kind": "protocol_error" if received else "transport_error",
                                 "message": f"{type(exc).__name__}: {exc}"}
                    entry["seconds"] = time.perf_counter() - request_started
                    entry["received_response"] = received
                    entry["valid"] = issue is None
                    if ui:
                        ui.request_done(session, entry["seconds"], entry["tokens"])
                    exhausted = attempt == config.retries
                    if issue:
                        entry["validation"] = issue
                        prefix = "FAIL" if exhausted else "RETRY"
                        emit(client, f"{prefix} lane {session + 1} task {index + 1} {stage} ({transformation}): {issue['message']}")
                        if "expected_excerpt" in issue:
                            if ui and hasattr(ui, "difference"):
                                ui.difference(issue["expected_excerpt"], issue["received_excerpt"])
                            else:
                                emit(client, f"  expected: {issue['expected_excerpt']!r} | received: {issue['received_excerpt']!r}")
                    if issue is None:
                        reply = result
                    elif exhausted and not config.strict and received:
                        # Supply only the intended tool fixture / next ID, never a transformed answer.
                        reply = {**result, "ok": False, "validation_error": issue["message"], "harness_continuation": True}
                        entry["harness_continuation"] = True
                    else:
                        reply = {"error": issue["message"] + ". Re-read the task and tool results; try again. "
                                 "Apply only this task's transformation: " + task["instructions"]}
                    replayable = (call is not None and isinstance(call.get("id"), str)
                                  and isinstance(call.get("function"), dict)
                                  and isinstance(call["function"].get("name"), str)
                                  and isinstance(call["function"].get("arguments"), str))
                    replay_start = len(messages)
                    if replayable:
                        replay = {k: v for k, v in message.items() if k in {"content", "reasoning_content"}}
                        replay.update({"role": "assistant", "tool_calls": [{"id": call["id"], "type": "function",
                                       "function": {"name": call["function"]["name"], "arguments": call["function"]["arguments"]}}]})
                        messages.append(replay)
                        messages.append({"role": "tool", "tool_call_id": call["id"], "content": json.dumps(reply)})
                    else:
                        # Do not replay a malformed assistant envelope into the next API request.
                        messages.append({"role": "user", "content": "Harness feedback: " + json.dumps(reply)})
                    trace.emit(sid, phase, "messages", task=index + 1, request_id=request_id, messages=messages[replay_start:])
                    if issue is None:
                        break
                    if exhausted:
                        task_ok = False
                        if stage != "submit_result":
                            dependency_ok = False
                        if config.strict:
                            raise ValueError(f"{stage} failed validation: {issue['message']}")
                        if not received:
                            aborted = True
                        break
                    retries += 1
                if aborted:
                    break
            completed += int(task_ok)
            exercised += int(not aborted)
            task_results.append({"task": index + 1, "transformation": transformation, "source_words": words,
                                 "load_words": sum(r.get("generated_words", 0) for r in requests[task_request_start:]), "correct": task_ok, "load_completed": not aborted,
                                 "quality_credit": quality_credit if dependency_ok and not aborted else 0.0,
                                 "read_padding_words": padding_words, "history_compacted_before": compact_before,
                                 "text_quality": edit, "finished_seconds": time.perf_counter() - started})
            if gate:
                gate.complete(index + 1)
            event = mix_control.observe(token_mix(r["tokens"] for r in requests[task_request_start:]), index + 1)
            gen_event = generation.observe(requests[task_request_start:], words, padding_words, index + 1)
            if gen_event and generation.words != words:
                emit(client, f"WORDS lane {session + 1}: {words} -> {generation.words}; "
                     f"fresh/decode {gen_event['cumulative_ratio']:.2f}:1")
            compact_before = config.history_mode == "task" or bool(event and event["compact_history"])
            if event and index + 1 < len(tasks) and (event["next_padding_words"] != padding_words or compact_before):
                emit(client, f"MIX lane {session + 1}: ratio {event['cumulative_ratio']:.2f}:1; "
                     f"next padding {padding_words} -> {event['next_padding_words']}; "
                     f"history {'compacted' if compact_before else 'retained'} ({event['reason']})")
            trace.emit(sid, phase, "task_end", task=index + 1, status="aborted" if aborted else "finished",
                       next_parameters={"words": generation.words, "padding": mix_control.padding})
            if aborted and next_id:
                messages.append({"role": "user", "content": f"Continue with task ID {next_id}."})
                trace.emit(sid, phase, "messages", task=index + 1, messages=messages[-1:])
            if ui:
                ui.lane(session, completed=completed, finished=index + 1, failures=index + 1 - completed, status="ready")
    except ContextLimitReached as exc:
        context_limited = True
        context_reason = str(exc)
        emit(client, f"CONTEXT lane {session + 1}: {context_reason}; no further tasks added")
    except RunCancelled:
        time_limited = control is not None and control.kind == "stage_time_limit"
        interrupted = not time_limited
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if requests:
            requests[-1]["error"] = error
        if ui:
            ui.log(f"FAIL lane {session + 1}: {error}")
    if gate:
        gate.finish()
    if ui:
        ui.lane(session, completed=completed, finished=len(task_results) if interrupted or time_limited else attempted,
                failures=attempted - completed - int((interrupted or context_limited or time_limited) and attempted > len(task_results)),
                status="cancelled" if interrupted else "time limit" if time_limited else "context limit" if context_limited else "stopped" if error else "completed")
    trace.emit(sid, phase, "session_end", stop={"kind": control.kind if control and control.stopped else
               "stage_time_limit" if time_limited else "context_limit" if context_limited else "error" if error else None,
               "reason": context_reason or error or (control.reason if control and control.stopped else None)})
    state.update({"trace": trace, "sid": sid, "messages": messages, "padding": mix_control.padding, "budget": budget, "context_reason": context_reason, "words": generation.words})
    return {"session": session, "telemetry": trace.events, "telemetry_session": sid,
            "word_mix": word_mix(trace.events, sid), "seconds": time.perf_counter() - started, "planned_tasks": len(tasks),
            "attempted_tasks": attempted, "completed_tasks": completed,
            "failed_tasks": attempted - completed - int((interrupted or context_limited or time_limited) and attempted > len(task_results)),
            "interrupted": interrupted, "unfinished_tasks": len(tasks) - len(task_results),
            "context_limited": context_limited, "context_reason": context_reason, "time_limited": time_limited,
            "context_skipped_tasks": len(tasks) - attempted if context_limited else 0, "context_budget": budget.snapshot(),
            "cancelled_requests": sum(r.get("cancelled", False) for r in requests),
            "load_completed_tasks": exercised, "task_results": task_results,
            "load_completed_words": sum(t["load_words"] for t in task_results if t["load_completed"]),
            "quality_word_credit": sum(t["quality_credit"] * t["load_words"] for t in task_results),
            "generation_adjustments": generation.events, "next_words": generation.words,
            "mix_adjustments": mix_control.events,
            "quality_credit": sum(task["quality_credit"] for task in task_results),
            "calls": calls, "retries": retries,
            "response_failures": sum(not r["valid"] and not r.get("cancelled", False) for r in requests),
            "usage": usage, "token_mix": token_mix(r["tokens"] for r in requests),
            "observed_token_mix": token_mix(r["tokens"] for r in requests if r["tokens"].get("available")),
            "requests": requests, "error": error}



def split_budget_tail(session, limit):
    def subset(requests, results, planned, attempted, tail=False):
        usage = {}
        for request in requests:
            for key, value in request.get("usage", {}).items():
                usage[key] = usage.get(key, 0) + value
        item = {**session, "requests": requests, "task_results": results, "usage": usage,
                "word_mix": word_mix(session["telemetry"], session["telemetry_session"], after=limit if tail else None, through=None if tail else limit),
                "planned_tasks": planned, "attempted_tasks": attempted,
                "completed_tasks": sum(t["correct"] for t in results),
                "load_completed_tasks": sum(t["load_completed"] for t in results),
                "load_completed_words": sum(t["load_words"] for t in results if t["load_completed"]),
                "quality_word_credit": sum(t["quality_credit"] * t["load_words"] for t in results),
                "generation_adjustments": [e for e in session["generation_adjustments"] if (e["after_task"] > limit) == tail],
                "quality_credit": sum(t["quality_credit"] for t in results),
                "failed_tasks": sum(not t["correct"] for t in results),
                "unfinished_tasks": max(0, planned - len(results)), "calls": len(requests),
                "retries": sum(r.get("attempt", 1) > 1 for r in requests),
                "cancelled_requests": sum(r.get("cancelled", False) for r in requests),
                "response_failures": sum(not r["valid"] and not r.get("cancelled", False) for r in requests),
                "token_mix": token_mix(r["tokens"] for r in requests),
                "observed_token_mix": token_mix(r["tokens"] for r in requests if r["tokens"].get("available")),
                "mix_adjustments": [e for e in session["mix_adjustments"] if (e["after_task"] > limit) == tail]}
        if tail:
            item.update(filler=True, budget_tail=True)
        elif session.get("error") and session["requests"][-1]["task"] > limit:
            item["error"] = None
        elif requests and results and any(r["task"] > limit for r in session["requests"]):
            item["wall_seconds"] = session["seconds"]
            item["seconds"] = results[-1].get("finished_seconds", session["seconds"])
        return item
    core_requests = [r for r in session["requests"] if r["task"] <= limit]
    tail_requests = [r for r in session["requests"] if r["task"] > limit]
    core_results = [t for t in session["task_results"] if t["task"] <= limit]
    tail_results = [t for t in session["task_results"] if t["task"] > limit]
    if not tail_requests:
        return session, None
    core = subset(core_requests, core_results, session["planned_tasks"], min(session["attempted_tasks"], limit))
    core["time_limited"] = True
    core["context_skipped_tasks"] = min(core.get("context_skipped_tasks", 0), core["unfinished_tasks"])
    core["next_words"] = core["generation_adjustments"][-1]["next_words"] if core["generation_adjustments"] else 5
    tail = subset(tail_requests, tail_results, max(0, session["attempted_tasks"] - limit),
                  max(0, session["attempted_tasks"] - limit), tail=True)
    return core, tail


def run_profile(client, config, workload, lanes, warmup_tasks=0, stage_prefill_rate=None, stage_timeout=None):
    if len(workload) != lanes:
        raise ValueError("Each profile requires exactly one session per lane")
    if getattr(client, "control", None) is None:
        client.control = RunControl()
    barrier = threading.Barrier(lanes)
    measurement_started_at = None
    warmup_started_at = time.perf_counter()
    warmup_sessions = []
    time_budget = {}
    stage_control = StageControl(client.control, lanes)
    virtual = getattr(client, "virtual_clock", None)
    if virtual:
        virtual.begin_profile(lanes, stage_control)
    stage_client = StageClient(client, stage_control)
    def start_measurement():
        nonlocal measurement_started_at
        if virtual:
            virtual.resume_lanes()
        if stage_prefill_rate is not None:
            time_budget.update(stage_budget(warmup_sessions, workload, config.tasks, stage_prefill_rate))
            if stage_timeout is not None:
                time_budget.update(seconds=stage_timeout, override=True)
            stage_control.start(time_budget["seconds"])
            ui = getattr(client, "progress", None)
            if ui and hasattr(ui, "stage_budget"):
                ui.stage_budget(time_budget["seconds"], stage_control)
            limit = format_duration(time_budget["seconds"]) if time_budget['seconds'] else "disabled"
            emit(client, f"RIPAL-{lanes}L measured time budget: {limit}; "
                 f"forecast fresh={time_budget['estimated_fresh_tokens']:,}, decode allowance={format_duration(time_budget['decode_allowance_seconds'])}")
        measurement_started_at = time.perf_counter()
    measurement_barrier = threading.Barrier(lanes, action=start_measurement)
    finished = threading.Event()
    lock = threading.Lock()
    remaining = lanes
    primary_finished_at = None
    filler_sessions = []
    wallclock = time.time()
    started = time.perf_counter()
    def worker(index, tasks):
        nonlocal remaining, primary_finished_at, measurement_started_at
        if virtual:
            virtual.enter_lane(index)
        barrier.wait()
        state = {}
        if warmup_tasks:
            warm_config = replace(config, tasks=warmup_tasks, filler=False)
            warm_client = PhaseClient(stage_client, "warmup")
            warm = run_session(warm_client, warm_config, tasks[:warmup_tasks], index, state=state)
            with lock:
                warmup_sessions.append(warm)
        if virtual:
            virtual.pause_lane(index)
        measurement_barrier.wait()
        measured_client = PhaseClient(StageClient(client, stage_control, TaskGate(stage_control, index)), "measured", warmup_tasks)
        primary = run_session(measured_client, config, tasks[warmup_tasks:], index, state=state)
        with lock:
            remaining -= 1
            if not remaining:
                primary_finished_at = time.perf_counter()
                finished.set()
        if config.filler:
            filler_client = FillerClient(stage_client, finished)
            sequence = 0
            filler_words = primary["next_words"]
            padding = primary["mix_adjustments"][-1]["next_padding_words"] if primary["mix_adjustments"] else config.filler_words
            while not finished.is_set() and not stage_control.stopped and not stage_control.expired:
                seed = int.from_bytes(hashlib.sha256(f"{config.seed}:filler:{index}:{sequence}".encode()).digest()[:8], "big")
                filler_config = replace(config, seed=seed, tasks=1, filler=False, filler_words=padding, words=filler_words)
                item = run_session(filler_client, filler_config, make_session(filler_config, index, task_offset=len(tasks) + sequence), index)
                item.update({"filler": True, "seed": seed})
                filler_words = item["next_words"]
                if item["mix_adjustments"]:
                    padding = item["mix_adjustments"][-1]["next_padding_words"]
                with lock:
                    filler_sessions.append(item)
                sequence += 1
                if item.get("context_limited"):
                    break
            ui = getattr(client, "progress", None)
            if ui:
                ui.lane(index, status="cancelled" if stop_requested(client) else "time limit" if primary.get("time_limited") else "context limit" if primary.get("context_limited") else "completed")
        return primary
    def dispatched(index, tasks):
        try:
            return worker(index, tasks)
        finally:
            if virtual:
                virtual.pause_lane(index)
    with ThreadPoolExecutor(max_workers=lanes) as pool:
        futures = [pool.submit(dispatched, index, tasks) for index, tasks in enumerate(workload)]
        sessions = [future.result() for future in futures]
    if stage_control.cutoff is not None:
        time_budget.update(drain_task=stage_control.cutoff["task"],
                           completion_limits=stage_control.cutoff["completion_limits"])
        for index, session in enumerate(sessions):
            primary, tail = split_budget_tail(session, stage_control.cutoff["completion_limits"][index])
            sessions[index] = primary
            if tail:
                filler_sessions.append(tail)
    elapsed = (primary_finished_at or time.perf_counter()) - (measurement_started_at or started)
    time_budget["drain_seconds"] = max(0, elapsed - time_budget.get("seconds", elapsed)) if stage_control.cutoff is not None else 0
    warmup_seconds = (measurement_started_at or time.perf_counter()) - warmup_started_at
    valid = all(s["error"] is None and s["completed_tasks"] == config.tasks for s in sessions)
    completed_work = sum(s["load_completed_tasks"] for s in sessions)
    completed_words = sum(s["load_completed_words"] for s in sessions)
    credit = completed_words / SCORE_WORDS
    time_limited = any(s.get("time_limited") for s in sessions)
    forecast_words = sum(projected_words(s) for s in sessions)
    projected_seconds = extrapolate_seconds(elapsed, completed_words, forecast_words) if time_limited else elapsed
    primary_mix = token_mix(s["token_mix"] for s in sessions)
    # A tail filler may be cancelled mid-response. Its unreturned usage is unknown;
    # retain that fact separately instead of inventing token counts.
    filler_observed = [s["observed_token_mix"] for s in filler_sessions if s["observed_token_mix"].get("available")]
    observed_mix = token_mix([primary_mix] + filler_observed) if filler_observed else primary_mix
    return {"lanes": lanes, "time": [wallclock, wallclock + time.perf_counter() - started],
            "clock": [started, time.perf_counter()],
            "measured_clock": [measurement_started_at or started, primary_finished_at or time.perf_counter()],
            "measured_time": [wallclock + (measurement_started_at or started) - started,
                              wallclock + (primary_finished_at or time.perf_counter()) - started],
            "seconds": elapsed, "valid": valid,
            "time_limited": time_limited, "stage_budget": time_budget,
            "estimated_full_seconds": projected_seconds, "score_work_unit_words": SCORE_WORDS,
            **termination_state(client), "interrupted": any(s["interrupted"] for s in sessions) or stop_requested(client),
            "score": score(credit, elapsed) if not config.strict or valid else None,
            "effective_sessions": credit, "sessions": sessions, "completed_words": completed_words, "estimated_planned_words": forecast_words, "token_mix": observed_mix,
            "primary_token_mix": primary_mix, "filler_sessions": filler_sessions,
            "word_mix": combine_word_mix(s["word_mix"] for s in sessions + filler_sessions),
            "warmup_sessions": warmup_sessions, "warmup_seconds": warmup_seconds,
            "has_measurements": any(s["requests"] for s in sessions),
            "filler_completed_tasks": sum(s["load_completed_tasks"] for s in filler_sessions),
            "filler_cancelled_requests": sum(s["cancelled_requests"] for s in filler_sessions),
            "filler_response_failures": sum(s["response_failures"] for s in filler_sessions),
            "mix_excludes_cancelled_filler_usage": any(s["cancelled_requests"] for s in filler_sessions)}


def calibrate_mix(client, config, target=4.0, tolerance=.15, max_rounds=6):
    """Untimed pilot sessions adjust padding only; freeze the resulting config."""
    rounds = []
    current = config
    previous = None
    for index in range(max_rounds):
        if stop_requested(client):
            return current, {"valid": False, "interrupted": True, "error": "Interrupted during calibration", "rounds": rounds}
        seed = int.from_bytes(hashlib.sha256(f"{config.seed}:calibration:{index}".encode()).digest()[:8], "big")
        pilot_config = replace(current, seed=seed)
        ui = getattr(client, "progress", None)
        if ui:
            ui.begin(f"Calibration {index + 1}/{max_rounds}", 1, config.tasks, current.filler_words)
        pilot = run_session(client, pilot_config, make_session(pilot_config, 0), 0)
        mix = pilot["token_mix"]
        rounds.append({"round": index + 1, "seed": seed, "filler_words": current.filler_words,
                       "session": pilot})
        if pilot["interrupted"] or stop_requested(client):
            reason = getattr(getattr(client, "control", None), "reason", None) or "Interrupted"
            return current, {"valid": False, "interrupted": True, "error": f"Calibration stopped: {reason}", "rounds": rounds}
        ratio = mix.get("fresh_per_output")
        ratio_text = f"{ratio:.2f}:1" if ratio is not None else "unknown"
        status = "FAILED (partial token mix)" if pilot["error"] else "complete"
        emit(client, f"Calibration {index + 1} {status}: {pilot['completed_tasks']}/{config.tasks} tasks; "
             f"padding={current.filler_words}; fresh/output={ratio_text}")
        if pilot["error"]:
            return current, {"valid": False, "error": pilot["error"], "rounds": rounds}
        if not mix["available"] or not mix["output_tokens"]:
            return current, {"valid": False, "error": "Fresh/output usage counters required for calibration. "
                            "Declare --cache-policy disabled only if caching is actually off.", "rounds": rounds}
        if mix_matches(mix, target, tolerance):
            return current, {"valid": True, "rounds": rounds}
        if current.filler_words == 0 and mix["fresh_per_output"] > target * (1 + tolerance):
            return current, {"valid": False, "error": "Base workload exceeds target with zero padding. "
                            "Try fewer --records/--tasks, more --words, or enable server prefix caching.", "rounds": rounds}
        if index + 1 == max_rounds:
            break
        tokens_per_word_per_task = 1.3  # first probe only; later use measured slope
        if previous and current.filler_words != previous[0]:
            slope = (mix["fresh_tokens"] - previous[1]) / (current.filler_words - previous[0]) / config.tasks
            if slope > .1:
                tokens_per_word_per_task = slope
        delta = (target * mix["output_tokens"] - mix["fresh_tokens"]) / (config.tasks * tokens_per_word_per_task)
        next_padding = max(0, round(current.filler_words + delta))
        if next_padding == current.filler_words:
            next_padding = max(0, current.filler_words + (1 if delta > 0 else -1))
        previous = (current.filler_words, mix["fresh_tokens"])
        current = replace(current, filler_words=next_padding)
    return current, {"valid": False, "error": "Token mix did not converge; inspect calibration rounds.", "rounds": rounds}


def score(sessions, seconds):
    if sessions <= 0:
        return 0.0
    if seconds <= 0:
        raise ValueError("Measured duration must be positive when work completed")
    return 1000 * math.asinh(SCORE_REFERENCE_SECONDS * sessions / seconds) / math.asinh(1)


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[min(lower + 1, len(ordered) - 1)] * weight


def summarize(runs, lanes, bootstrap_samples=5000, seed=42, warmup_valid=True,
              target_ratio=None, ratio_tolerance=.15, allow_failures=False):
    times = [run["seconds"] for run in runs]
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if len(times) > 1 else None
    correct = warmup_valid and all(run["valid"] for run in runs)
    mix = token_mix(run.get("token_mix", {"available": False}) for run in runs)
    mix_valid = mix_matches(mix, target_ratio, ratio_tolerance) if target_ratio is not None else None
    interrupted = any(run.get("interrupted", False) for run in runs)
    valid = correct and (mix_valid is not False) and not interrupted
    credits = [run.get("effective_sessions", lanes if run["valid"] else 0) for run in runs]
    mean_credit = statistics.mean(credits)
    scored = allow_failures or valid
    interval = None
    if scored and len(times) > 1 and not interrupted:
        rng = random.Random(seed)
        # Resample whole batches: sessions sharing a server are correlated.
        points = []
        for _ in range(bootstrap_samples):
            sample = rng.choices(list(zip(times, credits)), k=len(times))
            points.append(score(sum(c for _, c in sample), sum(t for t, _ in sample)))
        interval = [percentile(points, .025), percentile(points, .975)]
    latencies = [s["seconds"] for run in runs for s in run["sessions"]]
    sessions = [s for run in runs for s in run["sessions"]]
    time_limited = any(run.get("time_limited") for run in runs)
    planned = sum(s.get("planned_tasks", 0) for s in sessions)
    passed = sum(s.get("completed_tasks", 0) for s in sessions)
    failed = sum(s.get("failed_tasks", 0) for s in sessions)
    accuracy = passed / planned if planned else None
    context_skipped = sum(s.get("context_skipped_tasks", 0) for s in sessions)
    quality_tasks = sum(len(s.get("task_results", [])) if s.get("time_limited") else
                        s.get("planned_tasks", 0) - s.get("context_skipped_tasks", 0) for s in sessions)
    quality = sum(s.get("quality_credit", s.get("completed_tasks", 0)) for s in sessions) / quality_tasks if quality_tasks else None
    projected_completions = [extrapolate_seconds(s["seconds"], s.get("load_completed_words", 0),
                projected_words(s))
                             if s.get("time_limited") else s["seconds"] for s in sessions]
    quality_credit = sum(s.get("quality_word_credit", s.get("quality_credit", s.get("completed_tasks", 0)) * 256) for s in sessions)
    edit_score = score(quality_credit / len(runs) / SCORE_WORDS, mean) if scored and quality is not None else None
    score_samples = [score(run["effective_sessions"], run["seconds"]) for run in runs if "effective_sessions" in run]
    sigma_source = "repeated batch scores"
    if len(score_samples) < 2:
        score_samples = []
        sigma_source = "concurrency-scaled measured task throughput"
        for session in sessions:
            previous = 0
            for task in session.get("task_results", []):
                finished = task.get("finished_seconds", previous)
                duration = finished - previous
                if task.get("load_completed") and duration > 0:
                    score_samples.append(score(lanes * task.get("load_words", 0) / SCORE_WORDS, duration))
                previous = finished
    return {"profile": f"RIPAL-{lanes}L", "valid": valid, "measured_runs": len(runs),
            "interrupted": interrupted, "time_limited": time_limited,
            "score_stdev": statistics.stdev(score_samples) if len(score_samples) > 1 else None,
            "score_stdev_source": sigma_source, "score_stdev_samples": len(score_samples),
            "estimated_full_seconds": statistics.mean(run.get("estimated_full_seconds", run["seconds"]) for run in runs)
                if all(run.get("estimated_full_seconds", run["seconds"]) is not None for run in runs) else None,
            "complete_batches": sum(not run.get("interrupted", False) and not run.get("time_limited", False) for run in runs),
            "cancelled_requests": sum(s.get("cancelled_requests", 0) for s in sessions),
            "unfinished_tasks": sum(s.get("unfinished_tasks", 0) for s in sessions),
            "correct": correct, "token_mix": mix, "mix_valid": mix_valid,
            "word_mix": combine_word_mix(run.get("word_mix", {}) for run in runs),
            "score_status": "interrupted_partial" if interrupted else "time_limited_estimate" if time_limited else "verified" if valid else "with_failures_or_unverified_mix",
            "planned_tasks": planned, "passed_tasks": passed, "failed_tasks": failed,
            "context_skipped_tasks": context_skipped,
            "context_limited_sessions": sum(s.get("context_limited", False) for s in sessions),
            "task_accuracy": accuracy,
            "phase_averages": average_phases(sessions, lanes),
            "average_estimated_completion_seconds": statistics.mean(projected_completions)
                if projected_completions and all(value is not None for value in projected_completions) else None,
            "api_errors": sum(api_error_counts(sessions).values()),
            "edit_quality": quality,
            "text_edit_distance": sum(task["text_quality"]["distance"] for s in sessions
                                      for task in s.get("task_results", []) if task.get("text_quality")),
            "response_failures": sum(s.get("response_failures", 0) for s in sessions),
            "retries": sum(s.get("retries", 0) for s in sessions),
            "load_completed_tasks": sum(s.get("load_completed_tasks", 0) for s in sessions),
            "load_completed_words": sum(s.get("load_completed_words", 0) for s in sessions),
            "mean_effective_sessions": mean_credit,
            "filler_completed_tasks": sum(run.get("filler_completed_tasks", 0) for run in runs),
            "filler_cancelled_requests": sum(run.get("filler_cancelled_requests", 0) for run in runs),
            "filler_response_failures": sum(run.get("filler_response_failures", 0) for run in runs),
            "mix_excludes_cancelled_filler_usage": any(run.get("mix_excludes_cancelled_filler_usage") for run in runs),
            "history_compactions": sum(task.get("history_compacted_before", False) for s in sessions
                                       for task in s.get("task_results", [])),
            "padding_adjustments": sum(e["next_padding_words"] != e["padding_words"] and e["after_task"] < s["planned_tasks"]
                                       for s in sessions for e in s.get("mix_adjustments", [])),
            "failed_runs": sum(not run["valid"] for run in runs), "warmup_valid": warmup_valid,
            "mean_seconds": mean, "stdev_seconds": stdev,
            "timing_cv": stdev / mean if stdev is not None else None,
            "min_seconds": min(times), "max_seconds": max(times),
            "score": score(mean_credit, mean) if scored else None,
            "accuracy_adjusted_score": edit_score,
            "edit_adjusted_score": edit_score,
            "score_ci95": interval,
            "sessions_per_minute": 60 * mean_credit / mean if scored else None,
            "session_latency_seconds": {"median": statistics.median(latencies),
                                        "p90": percentile(latencies, .9), "max": max(latencies)},
            "note": "Approximate percentile bootstrap CI; no outliers removed. Single-run uncertainty is unavailable."}


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000/v1")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--simulate", action="store_true", help="Always-correct virtual model with a discrete-event clock")
    source.add_argument("--model")
    source.add_argument("--render", type=Path, help="Re-render a saved JSON datafile offline; leaves the datafile unchanged")
    parser.add_argument("--card-output", type=Path, help="Save the re-rendered plain-text card (requires --render)")
    parser.add_argument("--sim-prefill", type=float, default=1000, help="Virtual model aggregate prefill tokens/s")
    parser.add_argument("--sim-decode", type=float, default=50, help="Virtual model aggregate decode tokens/s")
    parser.add_argument("--sim-context", type=positive, default=1048576, help="Virtual model context capacity")
    parser.add_argument("--sim-response-timeouts", action="store_true", help="Apply response deadlines to virtual requests")
    parser.add_argument("--sim-cache-block", type=positive, default=8192, help="Virtual model prefix-cache block size")
    parser.add_argument("--api-key-env", default="OPENAI_API_KEY",
                        help="API key environment variable (default OPENAI_API_KEY); never saved in results")
    parser.add_argument("--lanes", nargs="+", type=positive, default=[1, 2, 4])
    parser.add_argument("--repeats", type=positive, default=1)
    parser.add_argument("--warmup-tasks", dest="warmup_tasks", type=int, default=2,
                        help="Unscored tasks at the start of each session (default 2; also tunes the token mix).")
    parser.add_argument("--bootstrap-samples", type=positive, default=5000)
    parser.add_argument("--calibration-rounds", type=int, default=0,
                        help="Optional extra calibration prepass; default 0 uses warmup tasks")
    parser.add_argument("--ratio-tolerance", type=float, default=.15,
                        help="Relative tolerance around 4:1, default ±15%%")
    parser.add_argument("--filler-words", type=int, default=Config().filler_words)
    parser.add_argument("--context-window", type=positive,
                        help="Context limit override if /models does not advertise it; lower limits are allowed")
    parser.add_argument("--no-filler", dest="filler", action="store_false",
                        help="Let early-finishing lanes idle instead of running tail filler tasks")
    parser.add_argument("--fixed-mix", dest="adaptive_mix", action="store_false",
                        help="Disable runtime padding and generation-size feedback (enabled by default)")
    parser.add_argument("--history", dest="history_mode", choices=["auto", "full", "task"], default=Config().history_mode,
                        help="full retains warmup + test history (default); auto/task permit compaction")
    parser.add_argument("--cache-policy", choices=["server-default", "enabled", "disabled"], default="server-default",
                        help="Declare server policy; disabled permits zero-cache accounting when absent. Configure the server separately.")
    task_options = parser.add_mutually_exclusive_group()
    task_options.add_argument("--tasks", type=positive,
                              help="Use this many measured tasks per lane at every concurrency")
    task_options.add_argument("--task-schedule", nargs="+", type=schedule_entry, metavar="C:TASKS",
                              help="Task counts by minimum concurrency; default 1:16 2:8 4:4; last entry applies above it")
    parser.add_argument("--stage-prefill-rate", type=float, default=1000,
                        help="Prefill tok/s floor for the minute-rounded measured-stage budget (default 1000)")
    parser.add_argument("--stage-timeout", type=float,
                        help="Override measured-stage seconds; 0 disables the cap; default calibrates from warmups")
    parser.add_argument("--words", type=positive, default=Config().words,
                        help="Initial source words per task (default 5); adaptive feedback changes subsequent tasks")
    parser.add_argument("--max-tokens", type=positive, default=Config().max_tokens,
                        help="Initial response token budget; grows to accommodate requested transformations")
    for name in ("records",):
        parser.add_argument("--" + name.replace("_", "-"), type=positive,
                            default=getattr(Config(), name))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--retries", type=int, default=Config().retries,
                        help="Optional diagnostic retries per tool; default 0 (one attempt, then continue)")
    parser.add_argument("--strict", action="store_true", help="Stop sessions on validation failure; suppress unverified scores")
    parser.add_argument("--timeout", type=float, default=60,
                        help="Initial response deadline (default 60s); later profiles use 4x the previous concurrency maximum")
    parser.add_argument("--extra-body", type=json.loads, default={})
    parser.add_argument("--ui", choices=["auto", "live", "plain"], default="auto",
                        help="Live VT dashboard on interactive terminals; plain logs when redirected")
    parser.add_argument("--color", choices=["auto", "always", "never"], default="auto")
    parser.add_argument("--no-stream", dest="streaming", action="store_false",
                        help="Use non-streaming responses; separate prefill/decode estimates will be unavailable")
    parser.add_argument("--output", type=Path,
                        help="Results filename (default realmark-MODEL.json)")
    args = parser.parse_args()
    if args.render:
        try:
            return render_saved_report(args)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            parser.error(f"Could not render {args.render}: {exc}")
    if args.card_output:
        parser.error("--card-output requires --render")
    if args.retries < 0 or args.warmup_tasks < 0 or args.calibration_rounds < 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("retries and warmup-tasks must be nonnegative; timeout must be positive and finite")
    if args.filler_words < 0 or not math.isfinite(args.ratio_tolerance) or not 0 < args.ratio_tolerance < 1:
        parser.error("filler-words must be nonnegative and ratio-tolerance must be between 0 and 1")
    if len(args.lanes) != len(set(args.lanes)):
        parser.error("lane counts must be unique")
    if not isinstance(args.extra_body, dict):
        parser.error("extra-body must be a JSON object")
    if not math.isfinite(args.stage_prefill_rate) or args.stage_prefill_rate <= 0:
        parser.error("stage-prefill-rate must be positive and finite")
    if args.stage_timeout is not None and (not math.isfinite(args.stage_timeout) or args.stage_timeout < 0):
        parser.error("stage-timeout must be nonnegative and finite")
    if args.tasks is None:
        args.task_schedule = args.task_schedule or DEFAULT_TASK_SCHEDULE
        args.tasks = Config().tasks
        try:
            for lanes in args.lanes:
                tasks_for_lanes(lanes, args.task_schedule)
        except ValueError as exc:
            parser.error(str(exc))
    config = Config(**{name: getattr(args, name) for name in Config.__dataclass_fields__})
    if not all(math.isfinite(v) and v > 0 for v in (args.sim_prefill, args.sim_decode)):
        parser.error("simulator rates must be positive and finite")
    runtime = VirtualClock(args.sim_prefill, args.sim_decode, args.sim_response_timeouts) if args.simulate else None
    with runtime if runtime else nullcontext():
        if runtime:
            args.model, args.base_url, args.ui = "virtual-model", "virtual://local", "plain"
            if args.stage_timeout is None:
                args.stage_timeout = 0
            client = VirtualModel(runtime, TOOLS, transform, args.sim_context, args.sim_cache_block)
            client.extra, client.cache_policy = args.extra_body, args.cache_policy
        else:
            client = Client(args.base_url, args.model, os.environ.get(args.api_key_env, ""),
                            args.timeout, args.extra_body, args.cache_policy)
        if args.output is None:
            model_filename = re.sub(r"[^A-Za-z0-9._-]+", "-", args.model).strip(".-") or "model"
            args.output = Path(f"realmark-{model_filename}.json")
        client.timeout, client.ratio_tolerance = args.timeout, args.ratio_tolerance
        client.streaming = args.streaming
        control = RunControl()
        client.control = control
        with control.signals():
            with TerminalUI(args.model, args.base_url, args.ui, args.color) as ui:
                client.progress = ui
                return run_benchmark(args, client, config)


def max_completed_response_seconds(run):
    values = [request["seconds"]
              for session in run.get("sessions", []) + run.get("filler_sessions", []) + run.get("warmup_sessions", [])
              for request in session.get("requests", [])
              if request.get("received_response") and not request.get("cancelled")
              and request.get("validation", {}).get("kind") not in {"tool_timeout", "server_overload"}
              and isinstance(request.get("seconds"), (int, float))
              and math.isfinite(request["seconds"]) and request["seconds"] > 0]
    return max(values, default=None)


def profile_timeout(lanes, levels, maxima, initial=60):
    index = levels.index(lanes)
    previous = levels[index - 1] if index else None
    observed = maxima.get(previous)
    return (4 * observed if observed is not None else initial), previous, observed


def batch_schedule(lanes, repeats):
    for repeat in range(repeats):
        offset = repeat % len(lanes)
        for level in lanes[offset:] + lanes[:offset]:
            yield repeat, level


def configure_context(client, override=None):
    control = getattr(client, "control", None)
    try:
        operation = client.discover_context_limit
        info = control.call(operation, client.timeout, "model metadata") if control else operation()
    except RunCancelled:
        raise
    except Exception as exc:
        if isinstance(exc, HTTPError) and exc.code == 429 and control:
            control.terminate("overload", "HTTP 429 while reading model context metadata")
            raise RunCancelled(control.reason)
        if override is None:
            raise ValueError(f"Cannot read model context limit: {exc}. Supply --context-window.") from exc
        info = {"limit": override, "source": "--context-window override"}
    if override is not None and override < info["limit"]:
        info = {"limit": override, "source": "--context-window cap", "model_limit": info["limit"]}
    client.context_window = info["limit"]
    return info


def run_benchmark(args, client, config):
    requested_config = config
    try:
        context_info = configure_context(client, args.context_window)
        emit(client, f"Model context limit: {context_info['limit']:,} tokens ({context_info['source']})")
    except Exception as exc:
        report = {"version": VERSION, "model": args.model, "base_url": args.base_url,
                  **termination_state(client), "config": asdict(config), "context_error": str(exc),
                  "scores": {}, "summaries": {}, "runs": [], "warmup_runs": []}
        write_results(args, client, report)
        return client.control.exit_code if stop_requested(client) else 2
    calibration = {"source": "warmup_tasks", "skipped": args.warmup_tasks == 0, "valid": False, "rounds": [], "warmup_profiles": []}
    if args.calibration_rounds:
        config, calibration = calibrate_mix(client, config, TARGET_RATIO,
                                           args.ratio_tolerance, args.calibration_rounds)
        if not calibration["valid"] and args.strict and not stop_requested(client):
            report = {"version": VERSION, "model": args.model, "base_url": args.base_url,
                      "requested_config": asdict(requested_config), "config": asdict(config),
                      "cache_policy": args.cache_policy, "calibration": calibration,
                      "target_fresh_per_output": TARGET_RATIO, "ratio_tolerance": args.ratio_tolerance,
                      "scores": {}, "geometric_mean_score": None, "runs": [], "warmup_runs": []}
            emit(client, f"Calibration failed: {calibration['error']}", final=True)
            write_results(args, client, report)
            return 1
        if not calibration["valid"]:
            # Choose the closest observed usable token mix; do not use an untested adjustment.
            candidates = [r for r in calibration["rounds"] if r["session"]["token_mix"].get("fresh_per_output") is not None]
            if candidates:
                best = min(candidates, key=lambda r: abs(r["session"]["token_mix"]["fresh_per_output"] - TARGET_RATIO))
                config = replace(config, filler_words=best["filler_words"])
            if stop_requested(client):
                emit(client, "Interrupted. Preparing partial results.")
            else:
                emit(client, f"Calibration warning: {calibration['error']}; continuing and reporting the measured mix.")
    runs = []
    warmups = []
    profile_padding = {lanes: config.filler_words for lanes in args.lanes}
    profile_maxima = {}
    profile_response_words = {}
    for repeat, lanes in batch_schedule(args.lanes, args.repeats):
        if stop_requested(client):
            break
        deadline, previous_lanes, previous_max = profile_timeout(lanes, args.lanes, profile_maxima, args.timeout)
        client.timeout = deadline
        client.timeout_reference_words = profile_response_words.get(previous_lanes, 256)
        basis = f"4 × {previous_lanes}L max {format_duration(previous_max)}" if previous_max is not None else "initial deadline"
        emit(client, f"RIPAL-{lanes}L response deadline {format_duration(deadline)} ({basis})")
        seed_text = f"{args.seed}:session:{repeat}:{lanes}"
        batch_seed = int.from_bytes(hashlib.sha256(seed_text.encode()).digest()[:8], "big")
        batch_config = replace(config, seed=batch_seed, filler_words=profile_padding[lanes],
                               tasks=tasks_for_lanes(lanes, args.task_schedule) if args.task_schedule else args.tasks)
        ui = getattr(client, "progress", None)
        if ui:
            ui.begin(f"RIPAL-{lanes}L · {args.warmup_tasks} warmup + {batch_config.tasks} measured · deadline {format_duration(deadline)}",
                     lanes, args.warmup_tasks + batch_config.tasks, batch_config.filler_words)
        workload_config = replace(batch_config, tasks=args.warmup_tasks + batch_config.tasks)
        workload = [make_session(workload_config, index) for index in range(lanes)]
        run = run_profile(client, batch_config, workload, lanes, warmup_tasks=args.warmup_tasks,
                          stage_prefill_rate=args.stage_prefill_rate, stage_timeout=args.stage_timeout)
        run["tool_timeout_seconds"] = deadline
        run["timeout_basis"] = {"previous_lanes": previous_lanes, "previous_max_seconds": previous_max, "multiplier": 4}
        observed_max = max_completed_response_seconds(run)
        if observed_max is not None and observed_max >= profile_maxima.get(lanes, 0):
            profile_maxima[lanes] = observed_max
            profile_response_words[lanes] = next((r.get("source_words", 256)
                for session in run["sessions"] + run.get("warmup_sessions", []) + run.get("filler_sessions", [])
                for r in session["requests"] if r.get("seconds") == observed_max), 256)
        run["repeat"] = repeat + 1
        run["tasks_per_session"] = batch_config.tasks
        run["initial_padding_words"] = batch_config.filler_words
        next_padding = [session["mix_adjustments"][-1]["next_padding_words"] for session in run["sessions"]
                        if session.get("mix_adjustments")]
        if config.adaptive_mix and next_padding:
            profile_padding[lanes] = round(statistics.median(next_padding))
        run["next_batch_padding_words"] = profile_padding[lanes]
        warm_sessions = run.pop("warmup_sessions")
        if warm_sessions:
            warm_mix = token_mix(session["token_mix"] for session in warm_sessions)
            warmups.append({"lanes": lanes, "repeat": repeat + 1, "sessions": warm_sessions,
                            "seconds": run["warmup_seconds"], "tasks_per_session": args.warmup_tasks,
                            "valid": all(session["completed_tasks"] == args.warmup_tasks for session in warm_sessions),
                            "token_mix": warm_mix, "seed": batch_seed})
            calibration.setdefault("warmup_profiles", []).append({"lanes": lanes, "token_mix": warm_mix})
            calibration["valid"] = all(mix_matches(p["token_mix"], TARGET_RATIO, args.ratio_tolerance)
                                        for p in calibration["warmup_profiles"])
        run["seed"] = batch_seed
        run["workload_sha256"] = hashlib.sha256(json.dumps(workload, sort_keys=True).encode()).hexdigest()
        if run["has_measurements"] or not stop_requested(client):
            runs.append(run)
        points = f"{run['score']:.1f}" if run["score"] is not None else "unavailable"
        emit(client, f"RIPAL-{lanes}L measured={repeat + 1} time={format_duration(run['seconds'])} provisional_load_score={points}")
    summaries = {}
    for lanes in args.lanes:
        group = [run for run in runs if run["lanes"] == lanes]
        if not group:
            continue
        summaries[str(lanes)] = summarize(group, lanes, args.bootstrap_samples, args.seed + lanes,
                                         True,
                                         TARGET_RATIO, args.ratio_tolerance, allow_failures=not args.strict)
        summary = summaries[str(lanes)]
        points = f"{summary['score']:.1f}" if summary["score"] is not None else "INVALID"
        interval = summary["score_ci95"]
        confidence = f"[{interval[0]:.1f}, {interval[1]:.1f}]" if interval else "unavailable"
        deviation = format_duration(summary["stdev_seconds"]) if summary["stdev_seconds"] is not None else "unavailable"
        cv = f"{100 * summary['timing_cv']:.1f}%" if summary["timing_cv"] is not None else "unavailable"
        emit(client, f"{summary['profile']}: {points} points; 95% CI {confidence}; "
              f"time {format_duration(summary['mean_seconds'])} ± {deviation}; CV {cv}; "
              f"fresh/output={summary['token_mix'].get('fresh_per_output')}; mix_valid={summary['mix_valid']}")
    scores = {lanes: summary["score"] for lanes, summary in summaries.items()}
    aggregate = (0.0 if any(value == 0 for value in scores.values()) else
                 math.exp(statistics.mean(math.log(value) for value in scores.values()))) if scores and all(
        value is not None for value in scores.values()) else None
    if stop_requested(client):
        aggregate = None
    report = {"version": VERSION, "model": args.model, "base_url": args.base_url,
              **termination_state(client),
              "suite_complete": not stop_requested(client),
              "unmeasured_lanes": [n for n in args.lanes if str(n) not in summaries],
              "requested_lanes": args.lanes,
              "task_schedule": args.task_schedule, "uniform_tasks": args.tasks if not args.task_schedule else None,
              "config": asdict(config), "cache_policy": args.cache_policy,
              "transformation_cycle": list(TRANSFORMATION_CYCLE), "vocabulary_words": len(WORDS),
              "pig_latin_vocabulary_words": len(PIG_LATIN_WORDS),
              "model_context": context_info,
              "requested_config": asdict(requested_config), "calibration": calibration,
              "target_fresh_per_output": TARGET_RATIO, "ratio_tolerance": args.ratio_tolerance,
              "measurement": {"warmup_tasks_per_lane": args.warmup_tasks,
                              "warmups_in_same_session": True, "repeats": args.repeats,
                              "bootstrap_samples": args.bootstrap_samples, "session_count": "equals lanes",
                              "initial_tool_timeout_seconds": args.timeout,
                              "stage_prefill_floor_tokens_per_second": args.stage_prefill_rate,
                              "stage_timeout_override_seconds": args.stage_timeout,
                              "stage_timeout_policy": "finish slowest active task cohort, then cancel advanced tail tasks",
                              "projection_method": "observed source-word rate; remaining tasks use the last requested source-word count",
                              "tool_timeout_policy": "4x previous concurrency maximum completed response",
                              "max_completed_response_seconds_by_lanes": profile_maxima,
                              "max_response_source_words_by_lanes": profile_response_words,
                              "response_deadline_scaling": "submit_result scales up with source words relative to previous profile maximum; initial reference 256 words",
                              "mix_control": "adaptive" if config.adaptive_mix else "fixed", "history_mode": config.history_mode,
                              "seed_policy": "SHA256(base_seed:session:repeat:lanes), warmup and measured task histories concatenated"},
              "score_formula": "1000 * asinh((completed_weighted_words / measured_seconds) / 10) / asinh(1)",
              "score_work_unit_words": SCORE_WORDS,
              "failure_policy": "strict" if args.strict else "continue; load score and accuracy reported separately",
              "quality_method": "character Levenshtein: 1 - distance/max(expected_length, received_length); "
                                "zero credit for copied source, invalid routing/metadata, or unfinished work",
              "scores": scores, "summaries": summaries, "geometric_mean_score": aggregate,
              "warmup_runs": warmups, "runs": runs}
    write_results(args, client, report)
    if stop_requested(client):
        return client.control.exit_code
    if args.strict:
        return 0 if aggregate is not None else 1
    return 0 if any(summary["load_completed_tasks"] > 0 for summary in summaries.values()) else 1


def refresh_report_charts(report):
    if report.get("summaries"):
        report["score_plot"] = braille_plot([(n, report["summaries"].get(str(n), {}).get("score"))
            for n in report.get("requested_lanes", [int(n) for n in report["summaries"]])])
        report["metric_plots"] = {"aggregate_" + key: braille_plot([(int(n), aggregate_phase(s, n, key))
            for n, s in report["summaries"].items()])
            for key in ("decode_tokens_per_second", "prefill_tokens_per_second")}


def timing_from_events(events, request):
    first = last = None
    for event in events:
        if event['type'] != 'stream_chunk' or event.get('request_id') != request['request_id'] or event['data'] == '[DONE]':
            continue
        chunk = json.loads(event['data'])
        visible = False
        for choice in chunk.get('choices', []):
            delta = choice.get('delta') or {}
            visible |= any(bool(delta.get(key)) for key in ('content', 'reasoning', 'reasoning_content'))
            for call in delta.get('tool_calls') or []:
                function = call.get('function') or {}
                visible |= bool(function.get('name') or function.get('arguments'))
        if visible:
            first = event['elapsed'] if first is None else first
            last = event['elapsed']
    return {'source': 'raw stream timestamps', 'first_output_seconds': first - request['elapsed'] if first is not None else None,
            'generation_seconds': last - first if first is not None else None}


def session_from_events(events, lane, cache_policy):
    events = sorted(events, key=lambda e: (e['time'], int(e['id'].rsplit(':', 1)[-1])))
    start = next(e for e in events if e['type'] == 'session_start')
    end = next((e for e in reversed(events) if e['type'] == 'session_end'), events[-1])
    plans = {e['task']: e for e in events if e['type'] == 'task_start'}
    endings = {e['task']: e for e in events if e['type'] == 'task_end'}
    responses = {e['request_id']: e for e in events if e['type'] == 'response'}
    errors = {e['request_id']: e for e in events if e['type'] == 'request_error'}
    streams = {}
    for event in events:
        if event['type'] == 'stream_chunk':
            streams.setdefault(event['request_id'], []).append(event)
    requests, results, usage = [], [], {}
    for event in events:
        if event['type'] != 'request':
            continue
        plan = plans[event['task']]
        fixture, stage = plan['fixture'], event['stage']
        target = fixture['target']
        wanted = ({'id': target['id']} if stage == 'search_records' else
                  {'path': fixture['path']} if stage == 'read_file' else
                  {'id': target['id'], 'revision': target['revision'],
                   'text': transform(target['payload'], fixture['transformation'])})
        response_event = responses.get(event['request_id'])
        failure = errors.get(event['request_id'])
        body = response_event['body'] if response_event else {}
        finish = response_event or failure or end
        entry = {'task': event['task'], 'stage': stage, 'attempt': event['attempt'],
                 'padding_words': len(plan['padding'].split()), 'source_words': fixture['words'],
                 'transformation': fixture['transformation'], 'seconds': max(0, finish['elapsed'] - event['elapsed']),
                 'received_response': response_event is not None, 'tokens': request_tokens(body, cache_policy),
                 'phase_timing': phase_timing({**body, '_stream_timing': timing_from_events(streams.get(event['request_id'], []), event)}, request_tokens(body, cache_policy)), 'valid': False,
                 'generated_words': 0, 'usage': {}}
        for key, value in (body.get('usage') or {}).items():
            if isinstance(value, (int, float)):
                entry['usage'][key] = value
            elif isinstance(value, dict):
                entry['usage'].update({key + '.' + k: v for k, v in value.items() if isinstance(v, (int, float))})
        for key, value in entry['usage'].items():
            usage[key] = usage.get(key, 0) + value
        try:
            if not response_event:
                error = failure.get('error', {}) if failure else {}
                kind, code = error.get('type'), error.get('code')
                if kind == 'RunCancelled':
                    entry['cancelled'] = True
                    raise RunCancelled(error.get('message'))
                issue_kind = ('server_overload' if code == 429 else 'tool_timeout' if kind in
                              ('ToolDeadlineExceeded', 'TimeoutError') or 'timed out' in error.get('message', '').lower()
                              else 'transport_error')
                entry['received_response'] = code == 429
                entry['validation'] = {'kind': issue_kind, 'message': error.get('message', 'No response')}
            else:
                choice = body['choices'][0]
                message = choice['message']
                entry['generated_words'] = generated_words(message)
                calls = message.get('tool_calls') or []
                if choice.get('finish_reason') == 'length':
                    raise ValueError('generation reached max_tokens')
                if message.get('content') and str(message['content']).strip():
                    raise ValueError('expected tool calls only, without explanatory text')
                if len(calls) != 1 or not calls[0].get('id'):
                    raise ValueError('expected exactly one tool call')
                function = calls[0]['function']
                try:
                    supplied = json.loads(function['arguments'])
                except (ValueError, TypeError):
                    supplied = None
                issue = validation_issue(stage, function['name'], wanted, supplied)
                entry['valid'] = issue is None
                if issue:
                    entry['validation'] = issue
                if stage == 'submit_result' and isinstance(supplied, dict) and isinstance(supplied.get('text'), str):
                    quality = text_quality(wanted['text'], supplied['text'])
                    quality['received_words'] = len(supplied['text'].split())
                    quality['copied_payload'] = supplied['text'].split() == target['payload'].split()
                    entry['text_quality'] = quality
                    metadata_ok = function['name'] == stage and set(supplied) == set(wanted) and supplied.get('id') == target['id'] and type(
                        supplied.get('revision')) is int and supplied['revision'] == target['revision']
                    entry['quality_credit'] = quality['similarity'] if metadata_ok and not quality['copied_payload'] else 0
        except RunCancelled:
            pass
        except Exception as exc:
            entry['validation'] = {'kind': 'protocol_error', 'message': f'{type(exc).__name__}: {exc}'}
        requests.append(entry)
    for task, ending in sorted(endings.items()):
        plan = plans[task]
        task_requests = [r for r in requests if r['task'] == task]
        last = {r['stage']: r for r in task_requests}
        completed = ending['status'] == 'finished'
        correct = completed and len(last) == 3 and all(r['valid'] for r in last.values())
        dependencies = all(last.get(stage, {}).get('valid') for stage in ('search_records', 'read_file'))
        submit = last.get('submit_result', {})
        load_words = sum(r['generated_words'] for r in task_requests)
        results.append({'task': task, 'source_words': plan['fixture']['words'],
                        'transformation': plan['fixture']['transformation'], 'correct': correct,
                        'load_completed': completed, 'load_words': load_words,
                        'quality_credit': submit.get('quality_credit', 0) if dependencies and completed else 0,
                        'text_quality': submit.get('text_quality'), 'read_padding_words': len(plan['padding'].split()),
                        'history_compacted_before': any(e['type'] == 'history_reset' and e.get('task') == task for e in events),
                        'finished_seconds': ending['elapsed'] - start['elapsed']})
    stop = end.get('stop') or {}
    planned = start['params']['tasks']
    completed = sum(t['correct'] for t in results)
    time_limited = stop.get('kind') == 'stage_time_limit' and len(results) < planned
    context_limited = stop.get('kind') == 'context_limit' or 'exceeds' in (stop.get('reason') or '') and 'context' in (stop.get('reason') or '')
    interrupted = stop.get('kind') in ('interrupt', 'overload')
    return {'session': lane, 'telemetry': events, 'telemetry_session': start['session'],
            'word_mix': word_mix(events, start['session']), 'phase': start['phase'], 'seed': start['params'].get('seed'), 'seconds': max(0, end['elapsed'] - start['elapsed']),
            'planned_tasks': planned, 'attempted_tasks': len(plans), 'completed_tasks': completed,
            'failed_tasks': sum(not t['correct'] for t in results) + int(stop.get('kind') == 'error' and len(plans) > len(results)),
            'load_completed_tasks': sum(t['load_completed'] for t in results),
            'load_completed_words': sum(t['load_words'] for t in results if t['load_completed']),
            'quality_credit': sum(t['quality_credit'] for t in results),
            'quality_word_credit': sum(t['quality_credit'] * t['load_words'] for t in results),
            'generation_adjustments': [{'after_task': t, 'source_words': plans[t]['fixture']['words'],
                                        'next_words': e['next_parameters']['words']} for t, e in endings.items()],
            'mix_adjustments': [{'after_task': t, 'padding_words': len(plans[t]['padding'].split()),
                                'next_padding_words': e['next_parameters']['padding']} for t, e in endings.items()],
            'next_words': list(endings.values())[-1]['next_parameters']['words'] if endings else start['params']['words'],
            'requests': requests, 'task_results': results, 'usage': usage,
            'calls': len(requests), 'retries': sum(r['attempt'] > 1 for r in requests),
            'token_mix': token_mix(r['tokens'] for r in requests),
            'observed_token_mix': token_mix(r['tokens'] for r in requests if r['tokens'].get('available')),
            'cancelled_requests': sum(r.get('cancelled', False) for r in requests),
            'response_failures': sum(not r['valid'] and not r.get('cancelled') for r in requests),
            'unfinished_tasks': planned - len(results), 'time_limited': time_limited,
            'context_limited': context_limited, 'context_reason': stop.get('reason') if context_limited else None,
            'context_skipped_tasks': planned - len(plans) if context_limited else 0,
            'interrupted': interrupted, 'error': stop.get('reason') if stop.get('kind') == 'error' else None}


def report_from_telemetry(raw):
    if not isinstance(raw, dict) or raw.get('schema') != SCHEMA:
        raise ValueError(f'Expected {SCHEMA} raw telemetry; run a new benchmark to produce this format')
    header = raw['config']
    params = header['parameters']
    report = {'version': header['benchmark_version'], 'model': header['model'], 'base_url': header['base_url'],
              'config': header['workload'], 'model_context': header.get('context'),
              'requested_lanes': params.get('lanes', [1, 2, 4]), 'measurement': {
                  'warmup_tasks_per_lane': params.get('warmup_tasks', 2), 'score_work_unit_words': SCORE_WORDS},
              'calibration': {'rounds': [], 'source': 'warmup_tasks', 'valid': False}, 'runs': [], 'warmup_runs': [], 'summaries': {}}
    stopped = next((e for e in raw.get('events', []) if e['type'] == 'run_stopped'), None)
    report.update(terminated=bool(stopped), interrupted=bool(stopped and stopped['kind'] == 'interrupt'),
                  termination_kind=stopped['kind'] if stopped else None,
                  termination_reason=stopped.get('reason') if stopped else None, suite_complete=not stopped)
    for event in raw.get('events', []):
        if event['type'] == 'setup_error':
            report['context_error'] = event['message']
    for key, profile in raw['runs'].items():
        lanes = int(key.split('=')[-1])
        for stage in profile['stages']:
            primary, warm, filler = [], [], []
            limits = (stage.get('cap') or {}).get('completion_limits')
            for lane in profile['lanes']:
                by_session = {}
                for events in lane['telemetry'].values():
                    for event in events:
                        if event['trial'] == stage['trial']:
                            by_session.setdefault(event['session'], []).append(event)
                for events in by_session.values():
                    session = session_from_events(events, lane['lane'] - 1, params.get('cache_policy', 'server-default'))
                    if session['phase'] == 'warmup':
                        warm.append(session)
                    elif session['phase'] == 'filler':
                        filler.append(session)
                    else:
                        if limits is not None:
                            session, tail = split_budget_tail(session, limits.get(str(lane['lane'] - 1), limits.get(lane['lane'] - 1, 10**9)))
                            if tail:
                                filler.append(tail)
                        primary.append(session)
            if stage['params'].get('calibration'):
                report['calibration']['rounds'].extend({'session': s} for s in warm + primary)
                continue
            if warm:
                report['warmup_runs'].append({'lanes': lanes, 'repeat': stage['trial'], 'sessions': warm,
                    'tasks_per_session': warm[0]['planned_tasks'], 'seed': warm[0]['seed'],
                    'valid': all(s['completed_tasks'] == s['planned_tasks'] for s in warm),
                    'token_mix': token_mix(s['token_mix'] for s in warm)})
            if not stage.get('measured_time') or not primary or not any(s['requests'] for s in primary):
                continue
            elapsed = stage['measured_clock'][1] - stage['measured_clock'][0]
            words = sum(s['load_completed_words'] for s in primary)
            projected = sum(projected_words(s) for s in primary)
            limited = any(s['time_limited'] for s in primary)
            mix = token_mix(s['token_mix'] for s in primary)
            observed = [s['observed_token_mix'] for s in filler if s['observed_token_mix'].get('available')]
            run = {'lanes': lanes, 'repeat': stage['trial'], 'seconds': elapsed, 'sessions': primary,
                   'word_mix': combine_word_mix(s['word_mix'] for s in primary + filler),
                   'filler_sessions': filler, 'tasks_per_session': stage['params']['tasks'],
                   'effective_sessions': words / SCORE_WORDS, 'completed_words': words,
                   'time_limited': limited, 'estimated_full_seconds': extrapolate_seconds(elapsed, words, projected) if limited else elapsed,
                   'interrupted': any(s['interrupted'] for s in primary) or bool(stopped),
                   'valid': all(s['error'] is None and s['completed_tasks'] == s['planned_tasks'] for s in primary),
                   'primary_token_mix': mix, 'token_mix': token_mix([mix] + observed) if observed else mix,
                   'filler_completed_tasks': sum(s['load_completed_tasks'] for s in filler),
                   'filler_cancelled_requests': sum(s['cancelled_requests'] for s in filler),
                   'filler_response_failures': sum(s['response_failures'] for s in filler),
                   'mix_excludes_cancelled_filler_usage': any(s['cancelled_requests'] for s in filler),
                   'stage_budget': {'seconds': stage['params'].get('stage_timeout_seconds'),
                                    'drain_task': (stage.get('cap') or {}).get('task')},
                   'seed': primary[0]['seed'], 'workload_sha256': hashlib.sha256(json.dumps([s['task_results'] for s in primary], sort_keys=True).encode()).hexdigest(),
                   'score': score(words / SCORE_WORDS, elapsed)}
            report['runs'].append(run)
    for lanes in report['requested_lanes']:
        group = [r for r in report['runs'] if r['lanes'] == lanes]
        if group:
            report['summaries'][str(lanes)] = summarize(group, lanes, params.get('bootstrap_samples', 5000),
                params.get('seed', 42) + lanes, True, TARGET_RATIO, params.get('ratio_tolerance', .15), not params.get('strict', False))
    report['scores'] = {c: s['score'] for c, s in report['summaries'].items()}
    values = list(report['scores'].values())
    report['geometric_mean_score'] = (math.exp(statistics.mean(math.log(v) for v in values)) if all(v > 0 for v in values) else 0) if values and all(v is not None for v in values) and not stopped else None
    report['unmeasured_lanes'] = [c for c in report['requested_lanes'] if str(c) not in report['summaries']]
    refresh_report_charts(report)
    report['final_card'] = result_card(report)
    return report


def render_saved_report(args):
    if args.card_output and args.card_output.resolve() == args.render.resolve():
        raise ValueError("Card output must differ from the input JSON datafile")
    report = report_from_telemetry(json.loads(args.render.read_text()))
    if not isinstance(report, dict):
        raise ValueError("Expected a benchmark report object")
    report["results_path"] = str(args.render.resolve())
    for level, summary in report.get("summaries", {}).items():
        sessions = [s for run in report.get("runs", []) if str(run["lanes"]) == str(level) for s in run["sessions"]]
        if sessions:
            summary["api_errors"] = sum(api_error_counts(sessions).values())
    refresh_report_charts(report)
    card = result_card(report, width=min(98, shutil.get_terminal_size((100, 32)).columns - 1))
    if args.card_output:
        args.card_output.write_text(card + "\n")
    ui = TerminalUI(report.get("model", "saved run"), report.get("base_url", ""), mode="plain", color=args.color)
    ui.card(card)
    return 0


def write_results(args, client, report):
    if stop_requested(client):
        report.update(termination_state(client))
    report["results_path"] = str(args.output.resolve())
    card_path = args.output.with_suffix(".card.txt")
    refresh_report_charts(report)
    width = min(98, shutil.get_terminal_size((100, 32)).columns - 1)
    raw = export_report(args, report, {"system": SYSTEM, "tools": TOOLS},
                        {"words": SCORE_WORDS, "seconds": SCORE_REFERENCE_SECONDS, "punctuation_weight": .25})
    args.output.write_text(json.dumps(raw, indent=2) + "\n")
    rebuilt = report_from_telemetry(raw)
    report["final_card"] = result_card(rebuilt, width=width)
    report["card_path"] = str(card_path.resolve())
    card_path.write_text(report["final_card"] + "\n")
    ui = getattr(client, "progress", None)
    if ui:
        ui.card(report["final_card"])
    else:
        print(report["final_card"])
    emit(client, f"Results: {args.output.resolve()}", final=True)
    emit(client, f"Card: {card_path.resolve()}", final=True)


if __name__ == "__main__":
    raise SystemExit(main())
