#!/usr/bin/env python3
"""RIPAL Mark: local prefill/decode load benchmark. Python standard library only."""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
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
import shutil
import signal
import statistics
import sys
import threading
import time
import urllib.request


class RunCancelled(Exception):
    pass


class ToolDeadlineExceeded(TimeoutError):
    pass


class RunControl:
    def __init__(self):
        self.stop = threading.Event()
        self.kind = None
        self.reason = None

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
            raise RunCancelled(self.reason or "Run stopped")

    def call(self, operation, timeout=60, label="tool call"):
        self.check()
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


def read_stream(response, started, control=None):
    message = {"role": "assistant", "content": None}
    calls, result, data = {}, {}, []
    first = last = None
    finish_reason = None
    done = False
    def consume(payload):
        nonlocal first, last, finish_reason, done
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
    return {
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
            self.lanes = {i: {"task": 0, "completed": 0, "finished": 0, "failures": 0, "stage": "ready", "status": "ready",
                              "since": time.perf_counter(), "calls": 0, "fresh": 0, "output": 0,
                              "accounted": True, "attempt": 1} for i in range(lanes)}
        self.log(f"{phase} | {lanes} lane(s), {tasks} tasks/session, padding={padding} words")

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
            message = f"lane {index + 1} {state['stage']} returned in {seconds:.2f}s | {counts}"
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
        lines = [paint("RIPAL  /  local prefill + decode benchmark", "1;36"),
                 paint(f"{self.model}  ·  {self.endpoint}", "2"), "",
                 paint(f"{phase}  ·  elapsed {now - self.started:.1f}s  ·  initial padding {padding:,} words", "1"),
                 paint(f"[{'━' * filled}{'─' * (24 - filled)}]  {finished}/{total} processed · {completed} passed · {failures} failed", "36"),
                 paint(f"Batch tokens  fresh {fresh:,}  decode {output:,}  running ratio {ratio}  (target 4:1)", "2"), "",
                 paint("LANE  TASK    TOOL / STATE                  REQUEST AGE    CALLS  FAILS   PAD", "1;34")]
        visible = max(1, height - 16)
        for index, state in lanes[:visible]:
            waiting = state["status"] == "waiting"
            age = f"{now - state['since']:.1f}s" if waiting else "—"
            label = state["stage"] if waiting else state["status"]
            retry = f" retry {state['attempt'] - 1}" if state["attempt"] > 1 else ""
            marker = spin if waiting else "✓" if state["status"] == "completed" else "!" if state["status"] == "stopped" else "·"
            code = "31" if state["status"] == "stopped" else "33" if retry or state["status"] == "cancelled" or state["failures"] else "32" if state["status"] == "completed" else "0"
            lines.append(paint(f" {index + 1:>2}   {state['task']:>2}/{tasks:<2}  {marker} {label + retry:<29} {age:>9}    {state['calls']:>4}  {state['failures']:>5} {state.get('padding', padding):>5}", code))
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
                if "RESULTS" in line:
                    return "1;" + CREAM
                if any(0x2801 <= ord(char) <= 0x28ff for char in line):
                    return ORANGE
                if any(char.isdigit() for char in line):
                    return AMBER
                return MUTED
            colored = []
            for line in lines:
                if line.startswith(("╭", "╰")):
                    colored.append(f"\x1b[{BACKGROUND};{WHITE}m{line}\x1b[0m")
                elif line.startswith("│") and line.endswith("│"):
                    colored.append(f"\x1b[{BACKGROUND};{WHITE}m│\x1b[{style(line)}m{line[1:-1]}\x1b[{WHITE}m│\x1b[0m")
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


def braille_plot(points, width=32, height=4):
    """Plot actual concurrency on X and autoscale Y to finite observed values."""
    if width < 1 or height < 1:
        raise ValueError("plot dimensions must be positive")
    points = sorted((n, value if value is not None and math.isfinite(value) else None) for n, value in points)
    if not points or any(n <= 0 for n, _ in points):
        raise ValueError("positive lane counts are required")
    known = [(n, value) for n, value in points if value is not None]
    xmin, xmax = (known[0][0], known[-1][0]) if known else (points[0][0], points[-1][0])
    ymin = min((value for _, value in known), default=0)
    ymax = max((value for _, value in known), default=0)
    w, h = width * 2, height * 4
    pixels, labels = set(), [" "] * width
    def position(n, value):
        x = round((n - xmin) / (xmax - xmin) * (w - 1)) if xmax != xmin else w // 2
        y = round((ymax - value) / (ymax - ymin) * (h - 1)) if ymax != ymin else h // 2
        return x, max(0, min(h - 1, y))
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
        if previous is not None:
            dx, dy = current[0] - previous[0], current[1] - previous[1]
            steps = max(abs(dx), abs(dy))
            for step in range(steps + 1):
                fraction = step / steps if steps else 0
                pixels.add((round(previous[0] + dx * fraction), round(previous[1] + dy * fraction)))
        previous = current
    bits = ((1, 8), (2, 16), (4, 32), (64, 128))
    lines = []
    for row in range(height):
        cells = []
        for col in range(width):
            mask = sum(bits[dy][dx] for dy in range(4) for dx in range(2)
                       if (col * 2 + dx, row * 4 + dy) in pixels)
            cells.append(chr(0x2800 + mask) if mask else " ")
        lines.append("".join(cells))
    return {"lines": lines, "labels": "".join(labels), "has_data": bool(known),
            "min_score": ymin, "max_score": ymax, "x_min": xmin, "x_max": xmax,
            "y_min": ymin, "y_max": ymax, "x_axis": "concurrency (C, linear)",
            "y_axis": "observed values, linear, fitted to data",
            "points": [{"lanes": n, "score": value, "value": value} for n, value in points]}


def chart_lines(plot, title, unit):
    result = [f"{title}  ·  Y: {unit}"]
    if not plot["has_data"]:
        return result + ["  No measured data available."]
    span = plot["y_max"] - plot["y_min"]
    decimals = max(0, min(4, -math.floor(math.log10(span / 3)))) if span > 0 else 2
    def label(value):
        return f"{value:,.{decimals}f}"
    for index, line in enumerate(plot["lines"]):
        tick = ""
        if span == 0 and index == len(plot["lines"]) // 2:
            tick = label(plot["y_min"])
        elif span > 0 and index == 0:
            tick = label(plot["y_max"])
        elif span > 0 and index == len(plot["lines"]) - 1:
            tick = label(plot["y_min"])
        result.append(f"{tick:>11} │{line}")
    result.append(" " * 12 + "└" + "─" * len(plot["lines"][0]))
    result.append(" " * 13 + plot["labels"])
    result.append(f"             X: concurrency (C), {plot['x_min']}–{plot['x_max']}")
    return result


def result_card(report, width=98):
    width = max(78, width)
    body = []
    def add(text=""):
        body.append(safe_text(text)[:width - 4])
    heading = ("RIPAL PARTIAL RESULTS — SERVER OVERWHELMED" if report.get("termination_kind") == "overload" else
               "RIPAL PARTIAL RESULTS — INTERRUPTED" if report.get("interrupted") else "RIPAL RESULTS")
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
    show_api_errors = any(s.get("api_errors", 0) for _, s in summaries)
    add("Lanes  Load score    Match   Completed" + ("   API errors" if show_api_errors else "") + "       Mix")
    for lanes, summary in summaries:
        def number(value):
            return f"{value:,.1f}" if value is not None else "—"
        ratio = summary["token_mix"].get("fresh_per_output")
        approximate = summary.get("mix_excludes_cancelled_filler_usage", False)
        mix = ("~" if approximate else "") + f"{ratio:.2f}:1" if ratio is not None else "unknown"
        planned = summary.get("planned_tasks", 0)
        completed = summary.get("load_completed_tasks", planned)
        quality = summary.get("edit_quality")
        match = f"{quality:.1%}" if quality is not None else "—"
        add(f"{int(lanes):>3}L {number(summary['score']):>10}  {match:>7}  "
            f"{completed:>4}/{planned:<5}" +
            (f"  {summary.get('api_errors', 0):>10}" if show_api_errors else "") + f"  {mix:>9}")
    add()
    if summaries:
        add("C    Prefill~ total (each)    Decode~ total (each)    Complete s   Batch s")
        for lanes, summary in summaries:
            phases = summary.get("phase_averages", {})
            def metric(value):
                return f"{value:,.2f}" if value is not None else "—"
            def rate(key):
                each = phases.get(key)
                total = aggregate_phase(summary, lanes, key)
                return f"{total:,.1f} ({each:,.1f})" if each is not None and total is not None else "—"
            add(f"{int(lanes):>2}L {rate('prefill_tokens_per_second'):>23} "
                f"{rate('decode_tokens_per_second'):>24} "
                f"{metric(phases.get('average_completion_seconds')):>12} "
                f"{metric(summary.get('mean_seconds')):>9}")
        add()
        plot = report.get("score_plot") or braille_plot([(int(n), s["score"]) for n, s in summaries])
        for line in chart_lines(plot, "Load score", "points"):
            add(line)
        for key, title in (("decode_tokens_per_second", "Total decode~"),
                           ("prefill_tokens_per_second", "Total prefill~")):
            add()
            metric_plot = report.get("metric_plots", {}).get("aggregate_" + key) or braille_plot([
                (int(n), aggregate_phase(summary, n, key)) for n, summary in summaries])
            for line in chart_lines(metric_plot, title, "tok/s"):
                add(line)
    else:
        add("No measured scores available.")
    add()
    calibration = report.get("calibration", {})
    if calibration.get("error"):
        add("Calibration: " + calibration["error"])
    for lanes, summary in summaries:
        if summary.get("context_limited_sessions"):
            add(f"{lanes}L context limit: {summary['context_skipped_tasks']} tasks not started; scores use partial work.")
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

    def complete(self, messages, stage, config):
        if getattr(self.client, "supports_stream_cancel", False):
            return self.client.complete(messages, stage, config, cancel_control=self.control)
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
        self.progress = PhaseProgress(client.progress, phase, offset) if getattr(client, "progress", None) else None

    def __getattr__(self, name):
        return getattr(self.client, name)


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
            ("read_file", {"path": task["path"]}, {"contents": task["contents"], "read_padding": padding}),
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


VERSION = "ripal-v7"
TARGET_RATIO = 4.0
WORDS = "amber birch cedar delta ember fern garden harbor island jungle kettle lantern maple nectar olive pebble quartz river silver timber umber valley willow yellow zephyr".split()
SYSTEM = """Complete the synthetic workspace tasks using the requested tools.
Search for the exact task ID, read its file, then submit the matching record's
revision and transformed payload. A file contains many distractor records.
Transform EVERY word: move its first letter to the end and append 'ay'.
Example: 'bright little lantern' -> 'rightbay ittlelay anternlay'.
Use exactly word[1:] + word[0] + 'ay', even for vowels: 'ember' -> 'mbereay',
'amber' -> 'mberaay'. Never add 'w' or apply conventional Pig Latin rules.
Process every source word exactly once; the output word count must be unchanged.
Keep word order and single spaces. Ignore read_padding; it is irrelevant filler.
This is a mechanical text conversion. Use tool calls only; do not explain or plan.
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
    tool("read_file", "Read a workspace file containing many records.",
         {"path": {"type": "string"}}),
    tool("submit_result", "Submit the transformed payload of the target record.",
         {"id": {"type": "string"}, "revision": {"type": "integer"},
          "text": {"type": "string"}}),
]


@dataclass(frozen=True)
class Config:
    tasks: int = 8
    records: int = 2
    words: int = 256
    filler_words: int = 1536
    seed: int = 42
    max_tokens: int = 2048
    retries: int = 0
    strict: bool = False
    adaptive_mix: bool = True
    history_mode: str = "full"
    filler: bool = True


def transform(text):
    return " ".join(word[1:] + word[0] + "ay" for word in text.split())


def make_session(config, session):
    tasks = []
    for task_index in range(config.tasks):
        rng = random.Random(f"{VERSION}:{config.seed}:{session}:{task_index}:records")
        records = []
        for record_index in range(config.records):
            records.append({
                "id": f"record-{rng.getrandbits(64):016x}",
                "revision": rng.randrange(1, 1000),
                "payload": " ".join(rng.choices(WORDS, k=config.words)),
            })
        target = records[rng.randrange(len(records))]
        tasks.append({"target": target, "path": f"/workspace/session-{session}/file-{task_index}.jsonl",
                      "contents": "\n".join(json.dumps(record) for record in records),
                      "padding_seed": f"{config.seed}:{session}:{task_index}:padding",
                      "read_padding": " ".join(random.Random(f"{config.seed}:{session}:{task_index}:padding").choices(
                          WORDS, k=config.filler_words))})
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


class Client:
    supports_stream_cancel = True

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

    def complete(self, messages, stage, config, cancel_control=None):
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
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            if "text/event-stream" in getattr(response, "headers", {}).get("Content-Type", ""):
                return read_stream(response, started, cancel_control or getattr(self, "control", None))
            return json.load(response)


def run_session(client, config, tasks, session, state=None):
    started = time.perf_counter()
    state = state if state is not None else {}
    messages = state.get("messages")
    if messages is None:
        messages = [{"role": "system", "content": SYSTEM}]
    messages.append({"role": "user", "content": f"Start with task ID {tasks[0]['target']['id']}."})
    calls = retries = completed = attempted = exercised = 0
    usage, requests, task_results = {}, [], []
    error = None
    interrupted = False
    context_limited = False
    context_reason = None
    ui = getattr(client, "progress", None)
    control = getattr(client, "control", None)
    mix_control = MixController(state.get("padding", config.filler_words), config.adaptive_mix, config.history_mode,
                                TARGET_RATIO, getattr(client, "ratio_tolerance", .15))
    output_reserve = int(getattr(client, "extra", {}).get("max_completion_tokens", getattr(client, "extra", {}).get("max_tokens", config.max_tokens)))
    budget = state.get("budget") or ContextBudget(client, TOOLS, output_reserve)
    compact_before = False
    try:
        if state.get("context_reason"):
            raise ContextLimitReached(state["context_reason"])
        for index, task in enumerate(tasks):
            if control:
                control.check()
            task_ok, aborted = True, False
            dependency_ok, quality_credit, edit = True, 0.0, None
            target = task["target"]
            if compact_before:
                messages = [{"role": "system", "content": SYSTEM},
                            {"role": "user", "content": f"Start with task ID {target['id']}."}]
            padding_words = mix_control.padding
            padding = task["read_padding"] if padding_words == config.filler_words else " ".join(
                random.Random(task["padding_seed"]).choices(WORDS, k=padding_words))
            budget.check_task(messages, task, padding)
            attempted += 1
            task_request_start = len(requests)
            if ui:
                ui.lane(session, padding=padding_words)
            next_id = tasks[index + 1]["target"]["id"] if index + 1 < len(tasks) else None
            expected = [
                ("search_records", {"id": target["id"]}, {"path": task["path"]}),
                ("read_file", {"path": task["path"]}, {"contents": task["contents"], "read_padding": padding}),
                ("submit_result", {"id": target["id"], "revision": target["revision"], "text": transform(target["payload"])},
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
                             "padding_words": padding_words,
                             "tokens": {"available": False, "requests": 1}, "valid": False}
                    requests.append(entry)
                    request_started = time.perf_counter()
                    calls += 1
                    call = message = None
                    received = False
                    try:
                        operation = lambda: client.complete(messages, stage, config)
                        response = control.call(operation, getattr(client, "timeout", 60), stage) if control else operation()
                        received = True
                        entry["tokens"] = request_tokens(response, getattr(client, "cache_policy", "server-default"))
                        entry["phase_timing"] = phase_timing(response, entry["tokens"])
                        for key, value in (response.get("usage") or {}).items():
                            if isinstance(value, (int, float)):
                                usage[key] = usage.get(key, 0) + value
                            elif isinstance(value, dict):
                                for detail, count in value.items():
                                    if isinstance(count, (int, float)):
                                        name = f"{key}.{detail}"
                                        usage[name] = usage.get(name, 0) + count
                        choice = response["choices"][0]
                        message = choice["message"]
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
                    except RunCancelled:
                        entry["seconds"] = time.perf_counter() - request_started
                        entry["cancelled"] = True
                        entry["received_response"] = False
                        raise
                    except Exception as exc:
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
                        emit(client, f"{prefix} lane {session + 1} task {index + 1} {stage}: {issue['message']}")
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
                                 "Use exactly word[1:] + word[0] + 'ay' for every word, including vowels."}
                    replayable = (call is not None and isinstance(call.get("id"), str)
                                  and isinstance(call.get("function"), dict)
                                  and isinstance(call["function"].get("name"), str)
                                  and isinstance(call["function"].get("arguments"), str))
                    if replayable:
                        replay = {k: v for k, v in message.items() if k in {"content", "reasoning_content"}}
                        replay.update({"role": "assistant", "tool_calls": [{"id": call["id"], "type": "function",
                                       "function": {"name": call["function"]["name"], "arguments": call["function"]["arguments"]}}]})
                        messages.append(replay)
                        messages.append({"role": "tool", "tool_call_id": call["id"], "content": json.dumps(reply)})
                    else:
                        # Do not replay a malformed assistant envelope into the next API request.
                        messages.append({"role": "user", "content": "Harness feedback: " + json.dumps(reply)})
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
            task_results.append({"task": index + 1, "correct": task_ok, "load_completed": not aborted,
                                 "quality_credit": quality_credit if dependency_ok and not aborted else 0.0,
                                 "read_padding_words": padding_words, "history_compacted_before": compact_before,
                                 "text_quality": edit})
            event = mix_control.observe(token_mix(r["tokens"] for r in requests[task_request_start:]), index + 1)
            compact_before = config.history_mode == "task" or bool(event and event["compact_history"])
            if event and index + 1 < len(tasks) and (event["next_padding_words"] != padding_words or compact_before):
                emit(client, f"MIX lane {session + 1}: ratio {event['cumulative_ratio']:.2f}:1; "
                     f"next padding {padding_words} -> {event['next_padding_words']}; "
                     f"history {'compacted' if compact_before else 'retained'} ({event['reason']})")
            if aborted and next_id:
                messages.append({"role": "user", "content": f"Continue with task ID {next_id}."})
            if ui:
                ui.lane(session, completed=completed, finished=index + 1, failures=index + 1 - completed, status="ready")
    except ContextLimitReached as exc:
        context_limited = True
        context_reason = str(exc)
        emit(client, f"CONTEXT lane {session + 1}: {context_reason}; no further tasks added")
    except RunCancelled:
        interrupted = True
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if requests:
            requests[-1]["error"] = error
        if ui:
            ui.log(f"FAIL lane {session + 1}: {error}")
    if ui:
        ui.lane(session, completed=completed, finished=len(task_results) if interrupted else attempted,
                failures=attempted - completed - int((interrupted or context_limited) and attempted > len(task_results)),
                status="cancelled" if interrupted else "context limit" if context_limited else "stopped" if error else "completed")
    state.update({"messages": messages, "padding": mix_control.padding, "budget": budget, "context_reason": context_reason})
    return {"session": session, "seconds": time.perf_counter() - started, "planned_tasks": len(tasks),
            "attempted_tasks": attempted, "completed_tasks": completed,
            "failed_tasks": attempted - completed - int((interrupted or context_limited) and attempted > len(task_results)),
            "interrupted": interrupted, "unfinished_tasks": len(tasks) - len(task_results),
            "context_limited": context_limited, "context_reason": context_reason,
            "context_skipped_tasks": len(tasks) - attempted if context_limited else 0, "context_budget": budget.snapshot(),
            "cancelled_requests": sum(r.get("cancelled", False) for r in requests),
            "load_completed_tasks": exercised, "task_results": task_results,
            "mix_adjustments": mix_control.events,
            "quality_credit": sum(task["quality_credit"] for task in task_results),
            "calls": calls, "retries": retries,
            "response_failures": sum(not r["valid"] and not r.get("cancelled", False) for r in requests),
            "usage": usage, "token_mix": token_mix(r["tokens"] for r in requests),
            "observed_token_mix": token_mix(r["tokens"] for r in requests if r["tokens"].get("available")),
            "requests": requests, "error": error}


def run_profile(client, config, workload, lanes, warmup_tasks=0):
    if len(workload) != lanes:
        raise ValueError("Each profile requires exactly one session per lane")
    if getattr(client, "control", None) is None:
        client.control = RunControl()
    barrier = threading.Barrier(lanes)
    measurement_barrier = threading.Barrier(lanes)
    measurement_started_at = None
    warmup_started_at = time.perf_counter()
    warmup_sessions = []
    finished = threading.Event()
    lock = threading.Lock()
    remaining = lanes
    primary_finished_at = None
    filler_sessions = []
    started = time.perf_counter()
    def worker(index, tasks):
        nonlocal remaining, primary_finished_at, measurement_started_at
        barrier.wait()
        state = {}
        if warmup_tasks:
            warm_config = replace(config, tasks=warmup_tasks, filler=False)
            warm_client = PhaseClient(client, "warmup")
            warm = run_session(warm_client, warm_config, tasks[:warmup_tasks], index, state=state)
            with lock:
                warmup_sessions.append(warm)
        measurement_barrier.wait()
        with lock:
            if measurement_started_at is None:
                measurement_started_at = time.perf_counter()
        measured_client = PhaseClient(client, "measured", warmup_tasks)
        primary = run_session(measured_client, config, tasks[warmup_tasks:], index, state=state)
        with lock:
            remaining -= 1
            if not remaining:
                primary_finished_at = time.perf_counter()
                finished.set()
        if config.filler:
            filler_client = FillerClient(client, finished)
            sequence = 0
            padding = primary["mix_adjustments"][-1]["next_padding_words"] if primary["mix_adjustments"] else config.filler_words
            while not finished.is_set() and not stop_requested(client):
                seed = int.from_bytes(hashlib.sha256(f"{config.seed}:filler:{index}:{sequence}".encode()).digest()[:8], "big")
                filler_config = replace(config, seed=seed, tasks=1, filler=False, filler_words=padding)
                item = run_session(filler_client, filler_config, make_session(filler_config, index), index)
                item.update({"filler": True, "seed": seed})
                if item["mix_adjustments"]:
                    padding = item["mix_adjustments"][-1]["next_padding_words"]
                with lock:
                    filler_sessions.append(item)
                sequence += 1
                if item.get("context_limited"):
                    break
            ui = getattr(client, "progress", None)
            if ui:
                ui.lane(index, status="cancelled" if stop_requested(client) else "completed")
        return primary
    with ThreadPoolExecutor(max_workers=lanes) as pool:
        futures = [pool.submit(worker, index, tasks) for index, tasks in enumerate(workload)]
        sessions = [future.result() for future in futures]
    elapsed = (primary_finished_at or time.perf_counter()) - (measurement_started_at or started)
    warmup_seconds = (measurement_started_at or time.perf_counter()) - warmup_started_at
    valid = all(s["error"] is None and s["completed_tasks"] == config.tasks for s in sessions)
    credit = sum(s["load_completed_tasks"] for s in sessions) / config.tasks
    primary_mix = token_mix(s["token_mix"] for s in sessions)
    # A tail filler may be cancelled mid-response. Its unreturned usage is unknown;
    # retain that fact separately instead of inventing token counts.
    filler_observed = [s["observed_token_mix"] for s in filler_sessions if s["observed_token_mix"].get("available")]
    observed_mix = token_mix([primary_mix] + filler_observed) if filler_observed else primary_mix
    return {"lanes": lanes, "seconds": elapsed, "valid": valid,
            **termination_state(client), "interrupted": any(s["interrupted"] for s in sessions) or stop_requested(client),
            "score": score(credit, elapsed) if not config.strict or valid else None,
            "effective_sessions": credit, "sessions": sessions, "token_mix": observed_mix,
            "primary_token_mix": primary_mix, "filler_sessions": filler_sessions,
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
    return 1000 * math.asinh(60 * sessions / seconds) / math.asinh(1)


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
    planned = sum(s.get("planned_tasks", 0) for s in sessions)
    passed = sum(s.get("completed_tasks", 0) for s in sessions)
    failed = sum(s.get("failed_tasks", 0) for s in sessions)
    accuracy = passed / planned if planned else None
    context_skipped = sum(s.get("context_skipped_tasks", 0) for s in sessions)
    quality_tasks = planned - context_skipped
    quality = sum(s.get("quality_credit", s.get("completed_tasks", 0)) for s in sessions) / quality_tasks if quality_tasks else None
    edit_score = score(lanes * quality, mean) if scored and quality is not None else None
    return {"profile": f"RIPAL-{lanes}L", "valid": valid, "measured_runs": len(runs),
            "interrupted": interrupted,
            "complete_batches": sum(not run.get("interrupted", False) for run in runs),
            "cancelled_requests": sum(s.get("cancelled_requests", 0) for s in sessions),
            "unfinished_tasks": sum(s.get("unfinished_tasks", 0) for s in sessions),
            "correct": correct, "token_mix": mix, "mix_valid": mix_valid,
            "score_status": "interrupted_partial" if interrupted else "verified" if valid else "with_failures_or_unverified_mix",
            "planned_tasks": planned, "passed_tasks": passed, "failed_tasks": failed,
            "context_skipped_tasks": context_skipped,
            "context_limited_sessions": sum(s.get("context_limited", False) for s in sessions),
            "task_accuracy": accuracy,
            "phase_averages": average_phases(sessions, lanes),
            "api_errors": sum(api_error_counts(sessions).values()),
            "edit_quality": quality,
            "text_edit_distance": sum(task["text_quality"]["distance"] for s in sessions
                                      for task in s.get("task_results", []) if task.get("text_quality")),
            "response_failures": sum(s.get("response_failures", 0) for s in sessions),
            "retries": sum(s.get("retries", 0) for s in sessions),
            "load_completed_tasks": sum(s.get("load_completed_tasks", 0) for s in sessions),
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
    source.add_argument("--model")
    source.add_argument("--render", type=Path, help="Re-render a saved JSON datafile offline; leaves the datafile unchanged")
    parser.add_argument("--card-output", type=Path, help="Save the re-rendered plain-text card (requires --render)")
    parser.add_argument("--api-key-env", default="OPENAI_API_KEY",
                        help="API key environment variable (default OPENAI_API_KEY); never saved in results")
    parser.add_argument("--lanes", nargs="+", type=positive, default=[1, 2, 4])
    parser.add_argument("--repeats", type=positive, default=1)
    parser.add_argument("--warmup-tasks", "--warmups", dest="warmup_tasks", type=int, default=2,
                        help="Unscored tasks at the start of each session (default 2; also tunes the token mix). --warmups is an alias.")
    parser.add_argument("--bootstrap-samples", type=positive, default=5000)
    parser.add_argument("--calibration-rounds", type=int, default=0,
                        help="Optional extra calibration prepass; default 0 uses warmup tasks")
    parser.add_argument("--skip-calibration", action="store_true",
                        help="Skip optional extra calibration; warmup/adaptive feedback remains enabled")
    parser.add_argument("--ratio-tolerance", type=float, default=.15,
                        help="Relative tolerance around 4:1, default ±15%%")
    parser.add_argument("--filler-words", type=int, default=Config().filler_words)
    parser.add_argument("--context-window", type=positive,
                        help="Context limit override if /models does not advertise it; lower limits are allowed")
    parser.add_argument("--no-filler", dest="filler", action="store_false",
                        help="Let early-finishing lanes idle instead of running tail filler tasks")
    parser.add_argument("--fixed-mix", dest="adaptive_mix", action="store_false",
                        help="Disable runtime padding feedback (enabled by default)")
    parser.add_argument("--history", dest="history_mode", choices=["auto", "full", "task"], default=Config().history_mode,
                        help="full retains warmup + test history (default); auto/task permit compaction")
    parser.add_argument("--cache-policy", choices=["server-default", "enabled", "disabled"], default="server-default",
                        help="Declare server policy; disabled permits zero-cache accounting when absent. Configure the server separately.")
    parser.add_argument("--tasks", type=positive, default=Config().tasks,
                        help="Maximum measured tasks per lane/session (default 8); context limits may stop earlier")
    for name in ("records", "words", "max_tokens"):
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
    parser.add_argument("--output", type=Path, default=Path("ripal-results.json"))
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
    config = Config(**{name: getattr(args, name) for name in Config.__dataclass_fields__})
    client = Client(args.base_url, args.model, os.environ.get(args.api_key_env, ""),
                    args.timeout, args.extra_body, args.cache_policy)
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


def batch_schedule(lanes, repeats, warmup_tasks):
    """Measure each level before warming the next, potentially larger level."""
    for level in lanes:
        if warmup_tasks:
            yield True, 0, level
        yield False, 0, level
    for repeat in range(1, repeats):
        offset = repeat % len(lanes)
        for level in lanes[offset:] + lanes[:offset]:
            yield False, repeat, level


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
    if args.calibration_rounds and not args.skip_calibration:
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
    for _, repeat, lanes in batch_schedule(args.lanes, args.repeats, 0):
        if stop_requested(client):
            break
        deadline, previous_lanes, previous_max = profile_timeout(lanes, args.lanes, profile_maxima, args.timeout)
        client.timeout = deadline
        basis = f"4 × {previous_lanes}L max {previous_max:.2f}s" if previous_max is not None else "initial deadline"
        emit(client, f"RIPAL-{lanes}L response deadline {deadline:.2f}s ({basis})")
        seed_text = f"{args.seed}:session:{repeat}:{lanes}"
        batch_seed = int.from_bytes(hashlib.sha256(seed_text.encode()).digest()[:8], "big")
        batch_config = replace(config, seed=batch_seed, filler_words=profile_padding[lanes])
        ui = getattr(client, "progress", None)
        if ui:
            ui.begin(f"RIPAL-{lanes}L · {args.warmup_tasks} warmup + {config.tasks} measured · deadline {deadline:.1f}s",
                     lanes, args.warmup_tasks + config.tasks, batch_config.filler_words)
        workload_config = replace(batch_config, tasks=args.warmup_tasks + config.tasks)
        workload = [make_session(workload_config, index) for index in range(lanes)]
        run = run_profile(client, batch_config, workload, lanes, warmup_tasks=args.warmup_tasks)
        run["tool_timeout_seconds"] = deadline
        run["timeout_basis"] = {"previous_lanes": previous_lanes, "previous_max_seconds": previous_max, "multiplier": 4}
        observed_max = max_completed_response_seconds(run)
        if observed_max is not None:
            profile_maxima[lanes] = max(profile_maxima.get(lanes, 0), observed_max)
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
        emit(client, f"RIPAL-{lanes}L measured={repeat + 1} seconds={run['seconds']:.3f} provisional_load_score={points}")
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
        deviation = f"{summary['stdev_seconds']:.3f}s" if summary["stdev_seconds"] is not None else "unavailable"
        cv = f"{100 * summary['timing_cv']:.1f}%" if summary["timing_cv"] is not None else "unavailable"
        emit(client, f"{summary['profile']}: {points} points; 95% CI {confidence}; "
              f"time {summary['mean_seconds']:.3f}s ± {deviation}; CV {cv}; "
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
              "config": asdict(config), "cache_policy": args.cache_policy,
              "model_context": context_info,
              "requested_config": asdict(requested_config), "calibration": calibration,
              "target_fresh_per_output": TARGET_RATIO, "ratio_tolerance": args.ratio_tolerance,
              "measurement": {"warmup_tasks_per_lane": args.warmup_tasks,
                              "warmups_in_same_session": True, "repeats": args.repeats,
                              "bootstrap_samples": args.bootstrap_samples, "session_count": "equals lanes",
                              "initial_tool_timeout_seconds": args.timeout,
                              "tool_timeout_policy": "4x previous concurrency maximum completed response",
                              "max_completed_response_seconds_by_lanes": profile_maxima,
                              "mix_control": "adaptive" if config.adaptive_mix else "fixed", "history_mode": config.history_mode,
                              "seed_policy": "SHA256(base_seed:session:repeat:lanes), warmup and measured task histories concatenated"},
              "score_formula": "1000 * asinh(60 * mean_effective_sessions / mean_batch_seconds) / asinh(1)",
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


def render_saved_report(args):
    if args.card_output and args.card_output.resolve() == args.render.resolve():
        raise ValueError("Card output must differ from the input JSON datafile")
    report = json.loads(args.render.read_text())
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
    report["final_card"] = result_card(report, width=width)
    report["card_path"] = str(card_path.resolve())
    args.output.write_text(json.dumps(report, indent=2) + "\n")
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
