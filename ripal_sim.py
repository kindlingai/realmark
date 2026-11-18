"""RealMark simulator: an OpenAI-compatible HTTP server on a simulated clock.

Run it alone (python3 ripal_sim.py --port 8090 --sim-delay 2) or through ripal.py --simulator="--sim-delay 2", which
starts it on a free port with those arguments. --realtime SPEED paces every request on the wall clock instead, SPEED
simulated seconds to each real one, and drops the clock headers, so any client times it as an ordinary server. Its
responses carry X-Sim-Speed, which ripal.py uses to scale its clock back to simulated seconds. Every phase
of a reply has a knob. A knob takes a number (--sim-delay 2), a distribution (--sim-delay "gaussian(2,0.5)") or a
formula over the request (--sim-prefill "4000/(1+P/65536)"). See KNOBS and README.md.

Clock protocol. The clock moves only while every active lane waits on a request, so client work takes no simulated
time. A request names its lane in X-Sim-Lane, and may name in X-Sim-Checkpoint a simulated time it must see, such as
its response deadline. Responses carry X-Sim-Clock, and each stream event follows a ": clock=T" SSE comment, since
headers cannot change mid-stream. The clock stops at the earliest checkpoint. Each waiting lane then polls POST
/sim/clock and acks to let it go on, or cancels its request: the client decides, as it would against a real server.
X-Sim-Event on /sim/clock also sets the lanes of a level (profile), pauses a lane while it does untimed work (pause)
and resumes every lane (resume). A request without X-Sim-Lane is answered at once and takes no simulated time.
"""
import argparse
import ast
import json
import math
import random
import re
import sys
import threading
import time as real_time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# Each knob: name, default and what it sets. A request draws its own values when it arrives.
KNOBS = [
    ("delay", "0", "seconds a request waits in the server's queue before it starts"),
    ("overhead", "0", "seconds of fixed server work before prefill, at any request size"),
    ("prefill", "1000", "prefill tokens/s, shared by the requests prefilling at once"),
    ("prefill_half_context", "131072", "context at which a request's prefill rate halves; 0 makes prefill context-free"),
    ("think", "0", "reasoning tokens decoded before the answer"),
    ("decode", "50", "decode tokens/s, shared by the requests decoding at once"),
    ("decode_half_context", "131072", "context at which a request's decode rate halves; 0 makes decode context-free"),
    ("chunk", "32", "tokens in each stream chunk"),
    ("stall", "0", "chance a stream pauses after each chunk"),
    ("stall_seconds", "1", "seconds a stall lasts"),
    ("buffer", "0", "chance the server holds the whole reply and sends it in one burst"),
    ("http_429", "0", "chance a request gets HTTP 429 Too Many Requests"),
    ("http_500", "0", "chance a request gets HTTP 500"),
    ("drop", "0", "chance the stream ends before [DONE], at a random point"),
    ("hang", "0", "chance the server never answers"),
    ("hang_seconds", "600", "seconds before a hung request's connection times out, when response deadlines are off"),
    ("corrupt", "0", "chance each word of a transformed answer is garbled"),
    ("wrong_record", "0", "chance a submit uses a distractor record"),
    ("mixed_record", "0", "chance a submit pairs the target's ID with a distractor's text"),
    ("runaway", "0", "chance an answer runs on until max_tokens"),
    ("malformed", "0", "chance a tool call's arguments are cut short, so they are not valid JSON"),
    ("cache_miss", "0", "chance a request finds nothing in the prefix cache"),
]
# A run draws these once.
STATIC_KNOBS = [
    ("context", "1048576", "context window in tokens"),
    ("cache_block", "8192", "prefix-cache block size in tokens: a request re-prefills the tail past the last full block"),
    ("kv_tokens", "0", "prefix-cache capacity shared by all lanes, least recently used lanes evicted first; 0 is unlimited"),
]
TOKENIZERS = ("words", "gpt", "llama", "bytes")
VARIABLES = {
    "C": "lanes in the level", "P": "prompt tokens", "K": "cached prompt tokens", "F": "fresh prompt tokens",
    "O": "answer tokens", "A": "requests in flight, this one included", "L": "lane, from 0",
    "N": "request number in the lane, from 1", "T": "simulated seconds since the run started",
}


def _lognormal(rng, mean, sd):
    # Parameterised by the distribution's own mean and standard deviation.
    if mean <= 0:
        return 0.0
    sigma = math.sqrt(math.log(1 + (sd / mean) ** 2))
    return rng.lognormvariate(math.log(mean) - sigma * sigma / 2, sigma)


def _functions(rng):
    return {
        "gaussian": lambda mean, sd: rng.gauss(mean, sd), "normal": lambda mean, sd: rng.gauss(mean, sd),
        "uniform": lambda low, high: rng.uniform(low, high),
        "exponential": lambda mean: rng.expovariate(1 / mean) if mean > 0 else 0.0,
        "lognormal": lambda mean, sd: _lognormal(rng, mean, sd),
        "chance": lambda p: 1.0 if rng.random() < p else 0.0,
        "min": min, "max": max, "clamp": lambda x, low, high: max(low, min(high, x)),
        "log": math.log, "exp": math.exp, "sqrt": math.sqrt, "abs": abs,
    }


_ALLOWED = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant, ast.Name, ast.Load, ast.Call, ast.Compare,
            ast.IfExp, ast.BoolOp, ast.And, ast.Or, ast.Not, ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod,
            ast.FloorDiv, ast.USub, ast.UAdd, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Eq, ast.NotEq)


class Expression:
    """A knob value: a number, a distribution or a formula over VARIABLES. Only arithmetic, comparisons, conditionals
    and the functions in _functions are allowed."""
    def __init__(self, text):
        self.text = str(text).strip()
        tree = ast.parse(self.text, mode="eval")
        names = set(VARIABLES) | set(_functions(random.Random()))
        for node in ast.walk(tree):
            if not isinstance(node, _ALLOWED):
                raise ValueError(f"{self.text!r}: {type(node).__name__} is not allowed")
            if isinstance(node, ast.Name) and node.id not in names:
                raise ValueError(f"{self.text!r}: unknown name {node.id!r}")
            if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float)):
                raise ValueError(f"{self.text!r}: only numbers are allowed")
            if isinstance(node, ast.Call) and (not isinstance(node.func, ast.Name) or node.keywords):
                raise ValueError(f"{self.text!r}: call functions by name with plain arguments")
        self.code = compile(tree, "<knob>", "eval")
        self.constant = not any(isinstance(node, (ast.Name, ast.Call)) for node in ast.walk(tree))

    def __call__(self, rng, **variables):
        value = eval(self.code, {"__builtins__": {}}, {**_functions(rng), **variables})
        return float(value)


class Tokenizer:
    """Counts the tokens of the text RealMark sends. `words` is RealMark's weighted word count. `gpt` and `llama`
    split text the way BPE pre-tokenizers do and charge each piece by profile: a word from RealMark's vocabulary costs
    1, while transformed words, mixed case, hex IDs and digits cost more. `bytes` is UTF-8 bytes / 4."""
    PIECES = re.compile(r" ?[a-z]+| ?[A-Z][a-z]*| ?[0-9]+| ?[^\sA-Za-z0-9]+|\s+")

    def __init__(self, name, host):
        if name not in TOKENIZERS:
            raise ValueError(f"unknown tokenizer {name!r}; choose from {', '.join(TOKENIZERS)}")
        self.name, self.host = name, host
        self.vocabulary = set(host.WORDS) | {"id", "revision", "payload", "contents", "path", "text", "key", "ok",
                                             "true", "false", "null", "record", "role", "content", "tool", "user",
                                             "assistant", "function", "name", "arguments", "type", "task"}

    def count(self, text):
        if self.name == "words":
            return self.host.weighted_words(text)
        if self.name == "bytes":
            return len(text.encode()) / 4
        return sum(self._piece(piece) for piece in self.PIECES.findall(text))

    def _piece(self, piece):
        bare = piece.strip()
        if not bare:
            return 1 if "\n" in piece else 0
        if bare.isdigit():
            return math.ceil(len(bare) / 3) if self.name == "gpt" else len(bare)
        if bare.isalpha():
            if bare.lower() in self.vocabulary and (bare.islower() or bare[1:].islower()):
                return 1 if self.name == "gpt" or len(bare) <= 6 else 2
            return math.ceil(len(bare) / (4 if self.name == "gpt" else 3))
        return math.ceil(len(bare) / 2) if self.name == "gpt" else len(bare)


class VirtualClock:
    """The simulated server's time. A request's handler thread blocks in run() while the clock jumps to the next
    event: a wait ends, a prefill finishes, a token is due, a stall ends or a checkpoint is reached."""
    def __init__(self, realtime=None):
        # With realtime set, the clock follows the wall clock at `realtime` simulated seconds to each real one: every
        # request is paced in real seconds, and no lane waits for another.
        self.realtime, self.started = realtime, real_time.monotonic()
        self.now, self.epoch = 0.0, real_time.time()
        self.condition = threading.Condition()
        self.active = {0}
        self.jobs = {}
        self.lanes = 1
        self.checkpoint = None
        self.stopped_at = None
        self.acks = set()

    def event(self, name, lane=None, lanes=None, at=None, checkpoint=None):
        with self.condition:
            if name == "profile":
                self.lanes, self.active = lanes, set(range(lanes))
            elif name == "pause":
                self.active.discard(lane)
            elif name == "resume":
                self.active = set(range(self.lanes))
            elif name == "checkpoint":
                self.checkpoint = checkpoint
            elif name == "ack" and self.stopped_at is not None and at is not None and abs(at - self.stopped_at) < 1e-6:
                self.acks.add(lane)
            elif name == "cancel" and lane in self.jobs:
                self._finish(self.jobs[lane], "cancelled")
            elif name == "cancel-job" and lane in self.jobs:
                self._finish(self.jobs[lane], "cancelled")
            self._release_stop()
            self.condition.notify_all()

    def _release_stop(self):
        if self.stopped_at is not None and set(self.jobs) <= self.acks:
            self.stopped_at, self.acks = None, set()
            for job in self.jobs.values():
                if job["checkpoint"] is not None and job["checkpoint"] <= self.now + 1e-9:
                    job["checkpoint"] = None
            if self.checkpoint is not None and self.checkpoint <= self.now + 1e-9:
                self.checkpoint = None

    def run(self, job, send):
        """Runs one request through its phases. `send(event, at)` writes each stream event from this request's own
        handler thread. Returns "done", "dropped", "hung" or "cancelled"."""
        with self.condition:
            if self.realtime:
                self._catch_up()
            job["wait_until"], job["hang_until"] = self.now + job["wait_for"], self.now + job["hang_for"]
            self.jobs[job["key"]] = job
            self.condition.notify_all()
            while True:
                while job["outbox"]:
                    event, at = job["outbox"].pop(0)
                    send(event, at)
                if job["result"]:
                    return job["result"]
                if self.realtime:
                    self._catch_up()
                    if not job["outbox"] and not job["result"]:
                        self.condition.wait(min(.05, self._next_event() / self.realtime))
                elif self.stopped_at is None and self.active.issubset(self.jobs):
                    self._advance()
                else:
                    self.condition.wait(.05)

    def _catch_up(self):
        """Moves the clock to the wall clock, event by event, so a prefill that ends mid-way hands over to decode on
        time."""
        target = (real_time.monotonic() - self.started) * self.realtime
        while self.jobs and self.now < target - 1e-9:
            self._advance(limit=target)
        self.now = max(self.now, target)

    def _next_event(self):
        """Real seconds until the next event, at most a second."""
        return max(.001, min(1.0, self._candidates()[0] if self.jobs else 1.0))

    def _finish(self, job, result):
        job["result"] = result
        self.jobs.pop(job["key"], None)
        self.condition.notify_all()

    def _deliver(self, job, data, chars=0):
        if job["held"] is not None:
            job["held"].append((data, chars))
        else:
            job["outbox"].append((data, self.now))

    def _shares(self):
        """The jobs prefilling, the jobs decoding, and each one's share of its phase's capacity right now."""
        stalled = {j["key"] for j in self.jobs.values() if j["resume"] is not None and j["resume"] > self.now + 1e-9}
        prefill = [j for j in self.jobs.values() if j["phase"] == "prefill"]
        decode = [j for j in self.jobs.values() if j["phase"] == "decode" and j["key"] not in stalled]
        def rate(total, jobs, job, half):
            return total / max(1, len(jobs)) / (1 + job["context"] / half if half else 1)
        p = {j["key"]: rate(j["prefill_rate"], prefill, j, j["prefill_half"]) for j in prefill}
        d = {j["key"]: rate(j["decode_rate"], decode, j, j["decode_half"]) for j in decode}
        return stalled, prefill, decode, p, d

    def _candidates(self):
        """Seconds until each pending event, smallest first."""
        stalled, prefill, decode, p, d = self._shares()
        candidates = [max(0, j["prefill"]) / p[j["key"]] for j in prefill]
        candidates += [max(0, min(j["tokens"], ((j["produced"] - 1) // j["chunk"] + 1) * j["chunk"] + 1)
                           - j["produced"] - j["credit"]) / d[j["key"]] for j in decode]
        candidates += [max(0, j["wait_until"] - self.now) for j in self.jobs.values() if j["phase"] == "wait"]
        candidates += [max(0, j["hang_until"] - self.now) for j in self.jobs.values() if j["phase"] == "hang"]
        candidates += [j["resume"] - self.now for j in self.jobs.values() if j["key"] in stalled]
        candidates += [max(0, c - self.now) for c in self._checkpoints()]
        return sorted(candidates) or [1.0]

    def _checkpoints(self):
        checkpoints = [j["checkpoint"] for j in self.jobs.values() if j["checkpoint"] is not None]
        return checkpoints + ([self.checkpoint] if self.checkpoint is not None else [])

    def _advance(self, limit=None):
        """Moves the clock to the next event, or to `limit` if that comes first, and applies what happens there."""
        if not self.jobs:
            return
        stalled, prefill, decode, p, d = self._shares()
        checkpoints = self._checkpoints()
        delta = self._candidates()[0]
        if limit is not None:
            delta = min(delta, max(0.0, limit - self.now))
        for job in prefill:
            job["prefill"] -= p[job["key"]] * delta
        for job in decode:
            job["credit"] += d[job["key"]] * delta
        self.now += delta
        for job in list(self.jobs.values()):
            if job["phase"] == "wait" and self.now >= job["wait_until"] - 1e-9:
                job["phase"] = "prefill"
            elif job["phase"] == "hang" and self.now >= job["hang_until"] - 1e-9:
                self._finish(job, "hung")
        for job in prefill:
            if job["key"] in self.jobs and job["prefill"] <= 1e-7:
                job["phase"] = "decode"
                # Prefill produces logits for the first generated token. The remaining token intervals run at the
                # decode rate.
                self._emit_token(job)
        for job in decode:
            while (job["key"] in self.jobs and job["credit"] >= 1 - 1e-7 and job["produced"] < job["tokens"]
                   and (job["resume"] is None or job["resume"] <= self.now + 1e-9)):
                job["credit"] -= 1
                self._emit_token(job)
        if any(c <= self.now + 1e-9 for c in checkpoints) and self.jobs:
            # Every waiting lane decides what this time means for it, then acks or cancels.
            self.stopped_at, self.acks = self.now, set()
        self.condition.notify_all()

    def _flush(self, job, kind):
        if not job["pending"]:
            return
        text, first = job["pending"], job["chunks_sent"] == 0
        if kind == "reasoning":
            delta = {"reasoning_content": text}
        elif job["kind"] == "content":
            delta = {"content": text}
        else:
            message = job["body"]["choices"][0]["message"]
            function = {"arguments": text}
            call = {"index": 0, "function": function}
            if not job.get("named"):
                job["named"] = True
                function["name"] = message["tool_calls"][0]["function"]["name"]
                call.update(id=message["tool_calls"][0]["id"], type="function")
            delta = {"tool_calls": [call]}
        self._deliver(job, {"choices": [{"index": 0, "delta": delta}]}, len(text))
        job["pending"] = ""
        job["chunks_sent"] += 1
        if job["stalls"] and not first and job["stalls"][0] <= job["chunks_sent"]:
            job["stalls"].pop(0)
            job["resume"] = self.now + job["stall_seconds"]

    def _emit_token(self, job):
        n, total, think = job["produced"], job["tokens"], job["think"]
        kind = "reasoning" if n < think else "answer"
        if kind == "reasoning":
            source, index, size = job["reasoning"], n, think
        else:
            source, index, size = job["arguments"], n - think, total - think
        if kind != job["pending_kind"]:
            self._flush(job, job["pending_kind"])
            job["pending_kind"] = kind
        job["pending"] += source[len(source) * index // size:len(source) * (index + 1) // size]
        job["produced"] += 1
        if n == 0 or n % job["chunk"] == 0 or job["produced"] == total:
            self._flush(job, kind)
        if job["drop_at"] is not None and job["produced"] >= job["drop_at"]:
            self._release(job)
            self._finish(job, "dropped")
            return
        if job["produced"] == total:
            self._release(job)
            finish = job["body"]["choices"][0]["finish_reason"]
            self._deliver(job, {"choices": [{"index": 0, "delta": {}, "finish_reason": finish}]})
            if job["usage_chunk"]:
                self._deliver(job, {"choices": [], "usage": job["body"]["usage"]})
            self._deliver(job, "[DONE]")
            self._finish(job, "done")

    def _release(self, job):
        """Sends a buffered reply's held chunks in one burst."""
        held, job["held"] = job["held"], None
        for data, chars in held or []:
            self._deliver(job, data, chars)


class VirtualModel:
    """A perfect agent behind the simulated server, made imperfect by the content and error knobs."""
    def __init__(self, host, knobs, static, tokenizer, seed):
        self.host, self.knobs, self.seed = host, knobs, seed
        self.transform = host.transform
        self.context_window = int(static["context"])
        self.cache_block = max(1, int(static["cache_block"]))
        self.kv_tokens = int(static["kv_tokens"]) or None
        self.tokenizer = tokenizer
        self.previous_prompts = {}
        self.generic_prompts = {}
        self.message_units = {}
        self.requests = {}
        self.base_units = {}
        self.request_counter = 0
        self.lock = threading.Lock()

    def count_prompt_tokens(self, messages, tools):
        """Tokens of a request: the tools it sends, then its messages."""
        key = json.dumps(tools, ensure_ascii=False)
        if key not in self.base_units:
            self.base_units[key] = self.tokenizer.count(json.dumps({"messages": [], "tools": tools}, ensure_ascii=False))
        units = self.base_units[key] + .25 * max(0, len(messages) - 1)
        for message in messages:
            calls = tuple((call.get("id"), call.get("type"), (call.get("function") or {}).get("name"),
                           (call.get("function") or {}).get("arguments")) for call in message.get("tool_calls") or [])
            key = (tuple(sorted(message)), message.get("role"), message.get("content"),
                   message.get("tool_call_id"), message.get("reasoning_content"), calls)
            cached = self.message_units.get(key)
            if cached is None:
                cached = self.tokenizer.count(json.dumps(message, ensure_ascii=False))
                self.message_units[key] = cached
            units += cached
        return math.ceil(units)

    def _rng(self, lane, number, name, extra=""):
        return random.Random(f"{self.seed}:{lane}:{number}:{name}:{extra}")

    def _draw(self, name, lane, number, variables):
        return self.knobs[name](self._rng(lane, number, name), **variables)

    def _chance(self, name, lane, number, variables):
        probability = self.knobs[name](self._rng(lane, number, name, "p"), **variables)
        return self._rng(lane, number, name, "roll").random() < probability

    def _garble(self, word, rng):
        letters = list(word)
        rng.shuffle(letters)
        return "".join(letters[:max(1, len(letters) - rng.randrange(0, 2))])

    def _answer(self, messages, target, stage, lane, number, variables):
        """The tool call a perfect agent makes next, then the content knobs applied to it."""
        if stage == "auto":
            # The next tool follows from the last tool result, unless the client moved the agent on to a new task
            # after that result.
            latest = next((m for m in reversed(messages) if m["role"] == "tool" or (
                m["role"] == "user" and str(m.get("content", "")).startswith(("Start with task ID ",
                                                                              "Continue with task ID ")))),
                          {"role": "user"})
            last = json.loads(latest["content"]) if latest["role"] == "tool" else {}
            stage = "submit_result" if "submit_key" in last else "read_file" if "path" in last else "search_records"
        if stage == "search_records":
            return stage, {"id": target}
        if stage == "read_file":
            found = next(json.loads(m["content"]) for m in reversed(messages)
                         if m["role"] == "tool" and "path" in json.loads(m["content"]))
            return stage, {"path": found["path"]}
        found = next(json.loads(m["content"]) for m in reversed(messages)
                     if m["role"] == "tool" and "contents" in json.loads(m["content"]))
        records = [json.loads(line) for line in found["contents"].splitlines()]
        record = next(r for r in records if r["id"] == target)
        others = [r for r in records if r["id"] != target]
        chosen = text_from = record
        if others and self._chance("wrong_record", lane, number, variables):
            chosen = text_from = self._rng(lane, number, "wrong_record", "which").choice(others)
        elif others and self._chance("mixed_record", lane, number, variables):
            text_from = self._rng(lane, number, "mixed_record", "which").choice(others)
        words = self.transform(text_from["payload"], found["transformation"]).split()
        corrupt = self.knobs["corrupt"](self._rng(lane, number, "corrupt", "p"), **variables)
        if corrupt > 0:
            rng = self._rng(lane, number, "corrupt", "words")
            words = [self._garble(word, rng) if rng.random() < corrupt else word for word in words]
        arguments = {"id": chosen["id"], "revision": chosen["revision"], "text": " ".join(words)}
        if "submit_key" in found:
            arguments["key"] = found["submit_key"]
        return stage, arguments

    def plan(self, request, lane, clock):
        """The response body and the drawn knob values for one chat request, or an HTTP error code."""
        messages = request["messages"]
        tool_choice = request.get("tool_choice")
        stage = tool_choice["function"]["name"] if isinstance(tool_choice, dict) else "auto"
        target = None
        for message in messages:
            if message["role"] == "user" and str(message.get("content", "")).startswith(("Start with task ID ",
                                                                                         "Continue with task ID ")):
                target = message["content"].split()[-1].rstrip(".")
            elif message["role"] == "tool":
                result = json.loads(message["content"])
                if result.get("next_task_id"):
                    target = result["next_task_id"]
        prompt = self.count_prompt_tokens(messages, request.get("tools") or [])
        key = messages[1]["content"] if len(messages) > 1 else messages[0].get("content")
        with self.lock:
            number = self.requests[lane] = self.requests.get(lane, 0) + 1
            previous = self.previous_prompts.pop(key, 0)
            cached = min(prompt, max(0, previous - 4) // self.cache_block * self.cache_block)
            # Insertion order is recency order: evict the least recently used other lanes.
            self.previous_prompts[key] = prompt
            while self.kv_tokens and sum(self.previous_prompts.values()) > self.kv_tokens and len(self.previous_prompts) > 1:
                del self.previous_prompts[next(iter(self.previous_prompts))]
            self.request_counter += 1
            call_id = f"virtual-{self.request_counter}"
        variables = {"C": clock.lanes, "P": prompt, "K": cached, "F": prompt - cached, "O": 0,
                     "A": len(clock.jobs) + 1, "L": lane, "N": number, "T": clock.now}
        if self._chance("cache_miss", lane, number, variables):
            cached = 0
            variables.update(K=0, F=prompt)
        for code in (429, 500):
            if self._chance(f"http_{code}", lane, number, variables):
                return code
        stage, arguments = self._answer(messages, target, stage, lane, number, variables)
        text = json.dumps(arguments, ensure_ascii=False)
        tokens = max(1, math.ceil(self.tokenizer.count(text)))
        finish = "tool_calls"
        limit = int(request.get("max_completion_tokens") or request.get("max_tokens") or 2048)
        if self._chance("runaway", lane, number, variables):
            # The answer keeps going until max_tokens: its own words over and over, so the JSON never closes.
            words = (arguments.get("text") or text).split() or ["again"]
            while self.tokenizer.count(text) < limit:
                text += " " + " ".join(words)
            text = text[:max(1, len(text) * limit // max(1, math.ceil(self.tokenizer.count(text))))]
            tokens, finish = limit, "length"
        elif self._chance("malformed", lane, number, variables):
            text = text[:max(1, len(text) // 2)]
        variables["O"] = tokens
        think = 0 if thinking_disabled(request) else max(0, int(self._draw("think", lane, number, variables)))
        message = {"role": "assistant", "content": None,
                   "tool_calls": [{"id": call_id, "type": "function", "function": {"name": stage, "arguments": text}}]}
        usage = {"prompt_tokens": prompt, "completion_tokens": think + tokens,
                 "prompt_tokens_details": {"cached_tokens": cached}}
        if think:
            message["reasoning_content"] = " ".join(["thinking"] * think)
            usage["completion_tokens_details"] = {"reasoning_tokens": think}
        body = {"id": call_id, "object": "chat.completion", "model": "virtual-model",
                "choices": [{"index": 0, "finish_reason": finish, "message": message}], "usage": usage}
        return self.timed_job(body, lane, number, variables, usage_chunk=True)

    def timed_job(self, body, lane, number, variables, usage_chunk):
        """A clock job that delivers `body` through the timing knobs: queue, overhead, prefill, thinking, decode in
        chunks with stalls, and buffering, dropping or hanging."""
        draw = lambda name: max(0.0, self._draw(name, lane, number, variables))
        chance = lambda name: self._chance(name, lane, number, variables)
        message, usage = body["choices"][0]["message"], body["usage"]
        calls = message.get("tool_calls") or []
        answer = calls[0]["function"]["arguments"] if calls else message.get("content") or ""
        think = (usage.get("completion_tokens_details") or {}).get("reasoning_tokens", 0)
        total = max(1, usage["completion_tokens"])
        chunk = max(1, int(self._draw("chunk", lane, number, variables)))
        stall = self.knobs["stall"](self._rng(lane, number, "stall", "p"), **variables)
        rng = self._rng(lane, number, "stall", "chunks")
        cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
        return {"lane": lane, "key": lane, "body": body, "context": usage["prompt_tokens"],
                "prefill": usage["prompt_tokens"] - cached, "phase": "hang" if chance("hang") else "wait",
                "wait_for": draw("delay") + draw("overhead"), "hang_for": draw("hang_seconds"),
                "think": think, "tokens": total, "produced": 0, "credit": 0.0, "arguments": answer,
                "kind": "tool" if calls else "content", "reasoning": message.get("reasoning_content") or "thinking",
                "pending": "", "pending_kind": None, "held": [] if chance("buffer") else None,
                "drop_at": self._rng(lane, number, "drop", "at").randrange(1, total + 1) if chance("drop") else None,
                "resume": None, "prefill_rate": draw("prefill") or 1e-9, "decode_rate": draw("decode") or 1e-9,
                "chunk": chunk, "prefill_half": draw("prefill_half_context"), "decode_half": draw("decode_half_context"),
                "stall_seconds": draw("stall_seconds"),
                "stalls": [i for i in range(1, total // chunk + 2) if stall > 0 and rng.random() < stall],
                "chunks_sent": 0, "checkpoint": None, "outbox": [], "result": None, "usage_chunk": usage_chunk}


def thinking_disabled(request):
    """Whether a request turns thinking off the ways real chat templates accept."""
    kwargs = request.get("chat_template_kwargs") or {}
    return (kwargs.get("enable_thinking") is False or kwargs.get("thinking") is False
            or str(request.get("reasoning_effort", "")).lower() in ("none", "no_think", "off"))


def _task_conversation(messages):
    """Whether a request continues a RealMark task: it names a task, and its last message is a tool result or one of
    the harness's own user turns. A probe's side question after a task's history is not one."""
    last = messages[-1] if messages else {}
    harness = ("Start with task ID ", "Continue with task ID ", "Harness feedback: ")
    return (any(str(m.get("content", "")).startswith(harness[:2]) for m in messages if m.get("role") == "user")
            and (last.get("role") == "tool" or str(last.get("content", "")).startswith(harness)))


class GenericModel:
    """Answers chat outside RealMark's task conversations, untimed: a plain reply, a tool call filled from its schema,
    or a record lookup when the prompt names a record ID and holds JSON lines with it. The answer, error, thinking and
    cache knobs apply, drawn for lane "probe"."""
    RECORD_ID = re.compile(r"record-[0-9a-f]{16}")

    def __init__(self, model):
        self.model = model

    def answer(self, request, clock=None):
        """(body, request number, knob variables) for one request, or an HTTP error code. `clock` gives C, A and T
        for a request on the clock."""
        model = self.model
        messages, tools = request.get("messages") or [], request.get("tools") or []
        prompt = model.count_prompt_tokens(messages, tools)
        key = json.dumps(messages[:1])
        with model.lock:
            number = model.requests["probe"] = model.requests.get("probe", 0) + 1
            # The longest run of leading messages shared with a recent prompt on the same first message is cached, so
            # requests that branch off one prefix share only the prefix.
            recent = model.generic_prompts.pop(key, [])
            shared = max((next((i for i, (a, b) in enumerate(zip(earlier, messages)) if a != b),
                                min(len(earlier), len(messages))) for earlier in recent), default=0)
            shared = min(shared, len(messages) - 1)
            previous = model.count_prompt_tokens(messages[:shared], tools) if shared else 0
            cached = min(prompt, max(0, previous - 4) // model.cache_block * model.cache_block)
            model.generic_prompts[key] = (recent + [messages])[-8:]
            model.request_counter += 1
            call_id = f"virtual-{model.request_counter}"
        variables = {"C": clock.lanes if clock else 1, "P": prompt, "K": cached, "F": prompt - cached, "O": 0,
                     "A": len(clock.jobs) + 1 if clock else 1, "L": 0, "N": number, "T": clock.now if clock else 0}
        chance = lambda name: model._chance(name, "probe", number, variables)
        if chance("cache_miss"):
            cached = 0
        for code in (429, 500):
            if chance(f"http_{code}"):
                return code
        limit = int(request.get("max_completion_tokens") or request.get("max_tokens") or 2048)
        message = {"role": "assistant", "content": None}
        tool_choice = request.get("tool_choice")
        if tools and tool_choice != "none":
            names = [t["function"]["name"] for t in tools]
            name = tool_choice["function"]["name"] if isinstance(tool_choice, dict) else names[0]
            tool = next(t for t in tools if t["function"]["name"] == name)
            text = json.dumps(self._arguments(tool, messages, number), ensure_ascii=False)
            if chance("malformed"):
                text = text[:max(1, len(text) // 2)]
            message["tool_calls"] = [{"id": call_id, "type": "function", "function": {"name": name, "arguments": text}}]
            finish = "tool_calls"
        else:
            # "Write exactly N words" asks for a long reply, which gives a client something to time decode on.
            asked = re.search(r"Write exactly (\d+) words", str((messages[-1] if messages else {}).get("content", "")))
            if asked:
                rng = model._rng("probe", number, "words")
                text = " ".join(rng.choice(model.host.WORDS) for _ in range(min(int(asked.group(1)), 20000)))
            else:
                text = "OK"
            message["content"] = text
            finish = "stop"
        tokens = max(1, math.ceil(model.tokenizer.count(text)))
        if chance("runaway"):
            words = text.split() or ["OK"]
            while model.tokenizer.count(text) < limit:
                text += " " + " ".join(words)
            if message["content"] is not None:
                message["content"] = text
            else:
                message["tool_calls"][0]["function"]["arguments"] = text
            tokens, finish = limit, "length"
        think = 0 if thinking_disabled(request) else max(0, int(model._draw("think", "probe", number, variables)))
        usage = {"prompt_tokens": prompt, "completion_tokens": think + tokens,
                 "total_tokens": prompt + think + tokens, "prompt_tokens_details": {"cached_tokens": cached}}
        if think:
            message["reasoning_content"] = " ".join(["thinking"] * think)
            usage["completion_tokens_details"] = {"reasoning_tokens": think}
        variables["O"] = think + tokens
        return ({"id": call_id, "object": "chat.completion", "model": "virtual-model",
                 "choices": [{"index": 0, "finish_reason": finish, "message": message}], "usage": usage},
                number, variables)

    def _arguments(self, tool, messages, number):
        """Fills a tool's parameters. A record ID in the last user message picks that record's fields from JSON lines
        in the prompt, unless the wrong_record knob picks another."""
        text = "\n".join(str(m.get("content") or "") for m in messages)
        last = next((str(m.get("content") or "") for m in reversed(messages) if m.get("role") == "user"), "")
        records = []
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("{") and '"id"' in line:
                try:
                    records.append(json.loads(line))
                except ValueError:
                    pass
        wanted = self.RECORD_ID.search(last)
        record = next((r for r in records if wanted and r.get("id") == wanted.group(0)), None)
        variables = {"C": 1, "P": 0, "K": 0, "F": 0, "O": 0, "A": 1, "L": 0, "N": number, "T": 0}
        if record and len(records) > 1 and self.model._chance("wrong_record", "probe", number, variables):
            record = self.model._rng("probe", number, "wrong_record", "which").choice([r for r in records if r is not record])
        properties = (tool["function"].get("parameters") or {}).get("properties") or {}
        values = {}
        for name, schema in properties.items():
            if record and name in record:
                values[name] = record[name]
            elif schema.get("type") in ("integer", "number"):
                values[name] = 0
            elif schema.get("type") == "boolean":
                values[name] = True
            else:
                # The last word of the request, as in "Call report_word with the word amber."
                words = re.findall(r"[A-Za-z0-9_-]+", last)
                values[name] = words[-1] if words else "OK"
        return values


class Handler(BaseHTTPRequestHandler):
    """The HTTP face of the simulator. server.sim holds the clock, model and settings."""
    def log_message(self, *args):
        pass

    def _clock_headers(self):
        sim = self.server.sim
        if sim.clock.realtime:
            # A real-time simulator is timed like any other server. X-Sim-Speed lets a client report simulated time.
            return {"X-Sim-Speed": repr(sim.clock.realtime)}
        return {"X-Sim-Clock": repr(sim.clock.now), "X-Sim-Epoch": repr(sim.clock.epoch),
                "X-Sim-Deadlines": "1" if sim.response_timeouts else "0",
                "X-Sim-Stopped": repr(sim.clock.stopped_at) if sim.clock.stopped_at is not None else ""}

    def _json(self, status, payload, extra=None):
        data = json.dumps(payload).encode()
        self.send_response(status)
        for name, value in {**self._clock_headers(), **(extra or {})}.items():
            self.send_header(name, value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length) or b"{}")

    def _number(self, name, cast=float):
        value = self.headers.get(name)
        return cast(value) if value not in (None, "") else None

    def do_GET(self):
        sim = self.server.sim
        if self.path.rstrip("/").endswith("/models"):
            self._json(200, {"object": "list", "data": [{"id": "virtual-model", "object": "model",
                                                         "max_model_len": sim.model.context_window}]})
        else:
            self._json(404, {"error": {"message": f"no route {self.path}"}})

    def do_POST(self):
        sim = self.server.sim
        path = self.path.rstrip("/")
        if path.endswith("/sim/clock"):
            self._body()
            event = self.headers.get("X-Sim-Event") or "poll"
            if event != "poll":
                sim.clock.event(event, lane=self._number("X-Sim-Lane", int), lanes=self._number("X-Sim-Lanes", int),
                                at=self._number("X-Sim-At"), checkpoint=self._number("X-Sim-Checkpoint"))
            self._json(200, {"clock": sim.clock.now})
        elif path.endswith("/tokenize"):
            body = self._body()
            self._json(200, {"count": sim.model.count_prompt_tokens(body.get("messages", []), body.get("tools") or [])})
        elif path.endswith("/chat/completions"):
            try:
                self._chat(self._body())
            except Exception as exc:
                # A simulator bug becomes an HTTP 500, so the client fails that one request and goes on.
                lane = self._number("X-Sim-Lane", int)
                if lane is not None:
                    sim.clock.event("cancel", lane=lane)
                try:
                    self._json(500, {"error": {"message": f"simulator error: {type(exc).__name__}: {exc}"}})
                except OSError:
                    pass
                raise
        else:
            self._json(404, {"error": {"message": f"no route {self.path}"}})

    def _chat(self, request):
        sim = self.server.sim
        lane = self._number("X-Sim-Lane", int)
        task = _task_conversation(request.get("messages") or [])
        if sim.clock.realtime:
            # Every request is timed. Requests without a lane draw their knobs as one stream of requests.
            with sim.lock:
                sim.counter += 1
                key = f"realtime-{sim.counter}"
            if task:
                job = sim.model.plan(request, "realtime" if lane is None else lane, sim.clock)
            else:
                answer = GenericModel(sim.model).answer(request, sim.clock)
                job = answer if isinstance(answer, int) else sim.model.timed_job(
                    answer[0], "probe", answer[1], answer[2],
                    usage_chunk=bool((request.get("stream_options") or {}).get("include_usage")))
            if not isinstance(job, int):
                job["key"] = key
        elif lane is None:
            # Untimed: the first-request check, an untimed probe, or a client outside the clock protocol.
            self._generic(request)
            return
        elif task:
            job = sim.model.plan(request, lane, sim.clock)
        else:
            # A general request on a lane, such as a probe's timing test, runs on the clock like a task.
            answer = GenericModel(sim.model).answer(request, sim.clock)
            job = answer if isinstance(answer, int) else sim.model.timed_job(
                answer[0], lane, answer[1], answer[2],
                usage_chunk=bool((request.get("stream_options") or {}).get("include_usage")))
        if isinstance(job, int):
            self._json(job, {"error": {"message": "Too Many Requests" if job == 429 else "Internal Server Error"}})
            return
        job["checkpoint"] = None if sim.clock.realtime else self._number("X-Sim-Checkpoint")
        if not request.get("stream"):
            result = sim.clock.run(job, lambda event, at: None)
            if result == "done":
                self._json(200, job["body"])
            else:
                self._json(504 if result == "hung" else 502, {"error": {"message": f"virtual model {result}"}})
            return
        self.send_response(200)
        for name, value in self._clock_headers().items():
            self.send_header(name, value)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        comment = not sim.clock.realtime
        def send(event, at):
            data = event if isinstance(event, str) else json.dumps(event)
            try:
                self.wfile.write(((f": clock={at!r}\n" if comment else "") + f"data: {data}\n\n").encode())
                self.wfile.flush()
            except OSError:
                sim.clock.event("cancel-job", lane=job["key"])
        sim.clock.run(job, send)


    def _generic(self, request):
        answer = GenericModel(self.server.sim.model).answer(request)
        body = answer if isinstance(answer, int) else answer[0]
        if isinstance(body, int):
            self._json(body, {"error": {"message": "Too Many Requests" if body == 429 else "Internal Server Error"}})
            return
        if not request.get("stream"):
            self._json(200, body)
            return
        # The whole reply goes out at once, at the current simulated time.
        self.send_response(200)
        for name, value in self._clock_headers().items():
            self.send_header(name, value)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        at = repr(self.server.sim.clock.now)
        message, choice = body["choices"][0]["message"], body["choices"][0]
        base = {"id": body["id"], "object": "chat.completion.chunk", "model": body["model"]}
        deltas = [{"role": "assistant"}]
        if message.get("reasoning_content"):
            deltas.append({"reasoning_content": message["reasoning_content"]})
        if message.get("content"):
            deltas.append({"content": message["content"]})
        for call in message.get("tool_calls") or []:
            deltas.append({"tool_calls": [{"index": 0, "id": call["id"], "type": "function",
                                           "function": {"name": call["function"]["name"],
                                                        "arguments": call["function"]["arguments"]}}]})
        events = [{**base, "choices": [{"index": 0, "delta": delta}]} for delta in deltas]
        events.append({**base, "choices": [{"index": 0, "delta": {}, "finish_reason": choice["finish_reason"]}]})
        if (request.get("stream_options") or {}).get("include_usage"):
            events.append({**base, "choices": [], "usage": body["usage"]})
        for event in events + ["[DONE]"]:
            data = event if isinstance(event, str) else json.dumps(event)
            self.wfile.write(f": clock={at}\ndata: {data}\n\n".encode())
        self.wfile.flush()


class Simulator:
    def __init__(self, model, response_timeouts, realtime=None):
        self.model, self.clock, self.response_timeouts = model, VirtualClock(realtime), response_timeouts
        self.lock, self.counter = threading.Lock(), 0


def _speed(text):
    try:
        value = float(text)
    except ValueError:
        value = math.nan
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive number, such as 1 for real time or 4 for 4x: {text!r}")
    return value


def add_arguments(parser):
    """Adds a --sim-<knob> flag for every knob, plus --sim-tokenizer, --sim-seed and --sim-response-timeouts."""
    group = parser.add_argument_group("simulator", "Knobs for the simulated server. Each takes a number, a distribution "
                                      "such as gaussian(1000,200), or a formula over " +
                                      ", ".join(f"{k} ({v})" for k, v in VARIABLES.items()) + ".")
    for name, default, text in KNOBS + STATIC_KNOBS:
        group.add_argument("--sim-" + name.replace("_", "-"), dest="sim_" + name, default=default,
                           help=f"{text} (default {default})")
    group.add_argument("--sim-tokenizer", choices=TOKENIZERS, default="words",
                       help="token counting: words is RealMark's weighted word count; gpt and llama emulate BPE "
                            "tokenizers on RealMark's vocabulary; bytes is UTF-8 bytes / 4 (default words)")
    group.add_argument("--sim-seed", type=int, default=0, help="seed for every random draw (default 0)")
    group.add_argument("--sim-response-timeouts", action="store_true",
                       help="Tell clients to apply response deadlines to simulated requests")
    group.add_argument("--realtime", type=_speed, metavar="SPEED",
                       help="Run on the wall clock at SPEED simulated seconds to each real one: 1 is real time, 4 runs "
                            "4 times as fast. Every request is paced in real seconds and no X-Sim headers go out, so "
                            "any client sees an ordinary server")


def build(args, host):
    """The simulator for parsed args. Raises ValueError for a bad knob."""
    knobs = {}
    for name, _, _ in KNOBS:
        try:
            knobs[name] = Expression(getattr(args, "sim_" + name))
        except (SyntaxError, ValueError) as exc:
            raise ValueError(f"--sim-{name.replace('_', '-')}: {exc}") from None
    static = {}
    for name, _, _ in STATIC_KNOBS:
        expression = Expression(getattr(args, "sim_" + name))
        value = expression(random.Random(f"{args.sim_seed}:static:{name}"), **{k: 0 for k in VARIABLES})
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"--sim-{name.replace('_', '-')}: must be a finite number of 0 or more")
        static[name] = value
    model = VirtualModel(host, knobs, static, Tokenizer(args.sim_tokenizer, host), args.sim_seed)
    return Simulator(model, args.sim_response_timeouts, args.realtime)


def serve(simulator, host="127.0.0.1", port=0):
    """Starts the simulator's HTTP server on a daemon thread and returns it. server.server_address has the port."""
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.sim = simulator
    threading.Thread(target=server.serve_forever, daemon=True, name="ripal-sim").start()
    return server


def main():
    # Importing ripal.py would otherwise leave a __pycache__ folder beside it.
    sys.dont_write_bytecode = True
    import ripal
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    add_arguments(parser)
    args = parser.parse_args()
    try:
        simulator = build(args, ripal)
    except ValueError as exc:
        parser.error(str(exc))
    server = serve(simulator, args.host, args.port)
    print(f"RealMark simulator on http://{args.host}:{server.server_address[1]}/v1", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
