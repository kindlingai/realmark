# Realmark

Realmark gives you a RIPAL (Reasoning Independent Pseudo Agentic Load) score for LLM models based
on their performance running reasoning-independent interactive tasks.

This performance is based on the model's agentic interactivity and gives you an idea of how "good"
a model is to work with.

![RealMark result card for glm-5.3-flash with low reasoning effort](examples/realmark-glm-5.3-flash.png)

This card comes from [examples/realmark-glm-5.3-flash.json](examples/realmark-glm-5.3-flash.json).
To render it again in a terminal:

```bash
python3 ripal.py --render examples/realmark-glm-5.3-flash.json
```

## RIPAL Tests

The RIPAL tests are reasoning-independent, which means they do not measure an agent's general intelligence,
only its ability to perform basic non-copying transformations and "needle-like" lookups. This does
not count 

**NOTE**: Realmark runs in your server's default thinking mode and in some cases, this default thinking
mode may be excessive for simpler tasks. Setting a forced tool choice, or `low`, `none` or `0` for thinking level may
help if the model overthinks these tasks.

Tasks include:

 - Letter transposition (like "pig latin")
 - Word reversing
 - Vowel removal (`aeiou`), and
 - Case inversion (e.g. `Hello` -> `hELLO`)

### A Note on Tests

"_Pig Latin??_ that's not something I use in my coding agent!"

That's true. Realmark is designed to be reasoning independent and tests the quality of your models'
weight and quantization. There are many benchmarks designed to test coding ability and completion of
reasoning-dependent tests. Realmark focuses on the important basics: agentic ability to manipulate
tokens at a _word level_ (which is surprisingly difficult - see the _how many r's in strawberry_ test)
and its baseline turnaround timing for interactive turns.

## RIPAL Score

RIPAL scores are calibrated so a model with a score of 1,000 can complete most interactive agentic
tasks, including those with large tool calls, in a reasonable amount of time. This is based on somewhat
subjective experience. RIPAL scores are also not linear- a model that prefills 2x as fast will not receive
a doubled score.

Note that RIPAL scores are not based on tokens as tokenization varies between models. Instead, RIPAL uses words,
and tasks based around transformations of whole words. Tasks are calibrated against the input and output word/token
counts of real agentic work streams. 

The score is defined as:

$$
S_C = 1000 \cdot \frac{\sinh^{-1}\left(\dfrac{W_C}{10\,(T_C - \frac{1}{2}\Theta_C)}\right)}{\sinh^{-1}(1)}
$$

where

- $W_C$ is the weighted visible output words all lanes' finished tasks (each punctuation mark or symbol counts as $0.25$),
  across reply text and tool-call arguments. Thinking is excluded.
- $T_C$ is the wall-clock time in seconds, from the start of non-warmup work until the last lane finishes.
- $\Theta_C$ is the wall-clock time spent thinking: for each request, the seconds from its first reasoning output to
  its first visible output, summed over all lanes and divided by $C$. Thinking time counts at half weight, so a
  model that thinks longer still scores lower, but not one for one.

By default, RIPAL measures four levels of concurrency (C=1, 2 and 4), but this can be overridden on
the command-line.

The headline score is the geometric mean over the concurrency levels $\mathcal{C}$ (by default $\{1, 2, 4\}$):

$$
S = \left( \prod_{C \in \mathcal{C}} S_C \right)^{1/|\mathcal{C}|}
$$

### A Note on Thinking

The prompts ask the model not to think, but some models require a minimum
amount of thinking to return correct answers. If the model does end up thinking to answer, the thinking time
is penalized half as much as generation time - the penalty is designed to factor in thinking time without
putting all non-thinking models above all thinking models. $0.5$ was effectively picked as the half-way compromise between
"thinking should not be penalized" and "thinking should be fully penalized"

### Prefill Fit

A request that adds a few hundred tokens to a long cached history spends most of its prefill time on what the server
does for any request, so fresh tokens over prefill time can read far below the server's real prefill rate. The card
fits every C=1 request, warmup and measured (one lane, so no queueing), to the seconds each further fresh token
costs, as a rate, and the seconds a request pays at any size.

A queue or a busy server delays a request whatever its size, and on a shared endpoint most requests can wait. A delay
only adds time, so the fit takes the low edge of the points: the line with 10% of requests below it, with the fixed
cost held at zero or above. Each request's time above that line is its wait, and the card reports the median. The
fixed cost's 95% interval comes from 100 seeded resamples. The card shows the fit when under 5% of resamples put the
per-token cost at zero.

Measured tasks add similar amounts of fresh input, too narrow a range to separate the two costs on a fast server.
`--probe` measures them apart, at the benchmark's own depth (see Probe).

One request past its response deadline fails its task, and the run goes on. So does one measured reply that runs on
to max_tokens without thinking. A second of either in the same level stops the run, and so does a reply that reaches
max_tokens while thinking, since a model that thinks past its budget will keep doing it.

The Fixed (s) column is each level's fixed cost: the fit's, or that level's median (prefill seconds less fresh tokens at
the fit's rate) where that is smaller, since some servers spend less a request under load. The prefill column and the
prefill charts leave it out. Without a fit, the column shows a dash and the prefill rates include it.

## Quality Score

Each run also gets a quality score as a letter grade. While the tasks are reasoning-independent, model and quantization quality
tends to be inversely proportional to the final score. Stronger models tend to be able to perform multi-token tasks with
greater ease, and make fewer mistakes. The quality score also measures the model's ability to locate specific content
in larger toolcall responses, which similarly tracks the quality of an engine's KV implementation and its quantization.






## Probe

`ripal.py --probe` probes a server and shows what it finds in a view shaped like the result card. Then it runs the
benchmark on the same server, and the result card adds the probe's lines. `--probe-only` stops after the probe and
saves it to `NAME.probe.json`. When the server cannot answer a chat request, the benchmark is skipped. The probe's
stages run in order:

- API baseline, a grid of 14 checks: whether `/models` gives a context limit, whether `/tokenize` works, which usage
  fields come back, whether the model thinks by default, whether streaming returns usage, which extra timing fields
  the server sends, and whether auto and forced tool choice and parallel tool calls work. It also checks that the
  model answers "OK" briefly, makes a valid tool call, and finds one record among 12 without pasting it, and counts
  how many tokens a word of each of RealMark's kinds of text costs.
- Idle baseline: 16 tiny uncached requests, one after another, for the median and 90th percentile time to first
  output.
- Prefill curve: fresh prompts of 2K and 16K tokens alone, then the same task conversation a benchmark lane opens with
  (system prompt, tools and seeded history, behind a nonce so no lane shares its cache), primed once. Side questions
  of 512, 2K, 8K and 32K fresh tokens follow it, so each reuses the cached conversation and pays only for its own
  tokens. Each size runs 3 times.
- Prefix cache: a prefix of 1/8 and 1/2 of the task depth, and the task conversation itself, each primed and then hit
  3 times, for the cost of restoring cached tokens. The first hit on the task conversation is the repeat-hit check.
  With `--probe-only`, also the cache's block size, from a conversation that grows past a block boundary: the hits
  land on block multiples.
- Decode: a 300-word answer at shallow context. With `--probe-only`, also at 32K and how the stream arrives.
- Concurrency, with `--probe-only` only: each level of `--lanes` runs its lanes at once, two requests a lane. A table
  of decode and prefill (totals, with the per-lane rate in brackets), the median time to first output and errors
  replaces the API grid. A benchmark measures all of this itself.

From these the probe fits a model of a lone request's time to first output:

    first output = fixed + a × cached + b × fresh + c × fresh × cached

The model is fitted to the fastest request of each condition, since delays only add time. The fastest of 3 still
carries the least of 3 delays, so the idle requests' spread above their fastest sets how much to take off. A term
that comes out negative, or within two standard errors of zero, is left out. On the simulator with a 4 s overhead,
4,000 t/s prefill and exponential delays averaging 3 s, the fit finds the prefill rate within 1% and the fixed cost
within about 0.7 s.

After the benchmark, 6 more idle requests check whether the server's idle cost moved. The card then shows the model,
how long each level's requests waited beyond it (each measured request's first output less the model's prediction
for its cached and fresh tokens, as a median and 90th percentile), and the idle cost before and after. At C=1 a wait
is the server's own delay. Above C=1 it adds the time spent behind other lanes. `--render` rebuilds these lines from
the saved results.

The timing stages show each test as a progress bar, faded until it runs. Their size comes from the API baseline's
tokens per word. On a simulator on simulated time, the timing stages run on its clock, so a probe takes seconds.

A probe request that gets HTTP 429 or 5xx retries twice, and the probe reports the retries. The probe sends your
`--extra-body` with every request.

```bash
python3 ripal.py --probe --base-url http://localhost:8000/v1 --model my-model
```

## Simulator

`ripal_sim.py` is an OpenAI-compatible HTTP server on a simulated clock: `/v1/chat/completions`, `/v1/models` and
`/tokenize`. `ripal.py --simulator="ARGS"` starts it on a free port with the simulator flags in ARGS, as one quoted
string (`--simulator` alone uses the defaults). To run the server alone and point any client at it:

```bash
python3 ripal_sim.py --port 8090 --sim-delay "exponential(2)"
```

```bash
python3 ripal.py --base-url http://127.0.0.1:8090/v1 --model virtual-model
```

`ripal.py` recognises the simulator by its `X-Sim-Clock` header. A run that takes an hour against a real server takes
seconds. The defaults model an always-correct agent with shared prefill and decode capacity and block-aligned prefix
caching. Every phase of a reply has a knob:

| Phase | Knobs |
|---|---|
| Before prefill | `--sim-delay` (queue seconds), `--sim-overhead` (fixed seconds of server work) |
| Prefill | `--sim-prefill` (tokens/s, shared by requests prefilling at once), `--sim-prefill-half-context` |
| Thinking | `--sim-think` (reasoning tokens before the answer) |
| Decode | `--sim-decode` (tokens/s, shared), `--sim-decode-half-context`, `--sim-chunk` (tokens a chunk), `--sim-stall`, `--sim-stall-seconds` |
| Delivery | `--sim-buffer` (whole reply in one burst), `--sim-drop` (stream ends before `[DONE]`) |
| Errors | `--sim-http-429`, `--sim-http-500`, `--sim-hang`, `--sim-hang-seconds`, `--sim-response-timeouts` |
| Answer | `--sim-corrupt` (chance per word), `--sim-wrong-record`, `--sim-mixed-record`, `--sim-runaway`, `--sim-malformed` |
| Cache | `--sim-cache-block`, `--sim-kv-tokens`, `--sim-cache-miss`, `--sim-context` |
| Tokens | `--sim-tokenizer words`, `gpt`, `llama` or `bytes` |

A knob takes a number (`--sim-delay 2`), a distribution (`--sim-delay "gaussian(2,0.5)"`) or a formula. Formulas can
use `C` (lanes in the level), `P` (prompt tokens), `K` (cached tokens), `F` (fresh tokens), `O` (answer tokens), `A`
(requests in flight), `L` (lane), `N` (request number in the lane) and `T` (simulated seconds), with `+ - * / **`,
comparisons and `x if cond else y`. Functions: `gaussian(mean, sd)`, `uniform(low, high)`, `exponential(mean)`,
`lognormal(mean, sd)`, `chance(p)`, `min`, `max`, `clamp(x, low, high)`, `log`, `exp`, `sqrt` and `abs`. Chances are
probabilities from 0 to 1. Each request draws its own values when it arrives, from `--sim-seed`, its lane, its request
number and the knob, so a run repeats exactly. `--sim-context`, `--sim-cache-block` and `--sim-kv-tokens` are drawn
once a run.

For example, a shared endpoint whose queue grows with load and whose prefill slows with context:

```bash
python3 ripal.py --simulator='--sim-delay "exponential(2*C)" --sim-overhead 3.5 --sim-prefill "4000/(1+P/65536)" --sim-prefill-half-context 0'
```

The tokenizers count the text RealMark sends. `words` is RealMark's weighted word count. `gpt` and `llama` split text
as BPE pre-tokenizers do: a word from RealMark's vocabulary is 1 token, while transformed words, mixed case, hex IDs
and digits cost more, as they do in real tokenizers. `bytes` is UTF-8 bytes / 4.

### Real time

`--realtime SPEED` runs the simulator on the wall clock, at SPEED simulated seconds to each real one: `--realtime 1`
is real time, and `--realtime 4` runs four times as fast. Every request, from any client, is paced in real seconds
through the same knobs, and concurrent requests share prefill and decode capacity as they do on simulated time. The
server leaves out the clock headers and comments of simulated time, so other clients time it as an
ordinary server. At a SPEED above 1, other clients see every rate SPEED times higher and every delay SPEED times
shorter than the knobs set. Its responses carry `X-Sim-Speed`, and `ripal.py` scales its clock by it, so its
timers, rates and scores come out in simulated seconds, as the knobs set them. The client's own work scales too, so
a high SPEED inflates times by SPEED times the client's overhead. Lanes on the wall clock drift apart, so at C=4 one
lane can prefill while another decodes, and a real-time run can score above the same run on simulated time:

```bash
python3 ripal.py --probe --simulator="--realtime 1 --sim-overhead 3.5 --sim-delay exponential(2)"
```

### Clock protocol

The simulated clock moves only while every active lane waits on a request, so simulated time covers only server work.

- A request names its lane in `X-Sim-Lane`, and can give a simulated time to stop at in `X-Sim-Checkpoint`, such as
  its response deadline or the level's time budget. A request without `X-Sim-Lane`, such as the first-request check,
  gets its answer at once, at the current simulated time.
- Every response carries `X-Sim-Clock`, along with `X-Sim-Epoch` and `X-Sim-Deadlines` (1 when the server was started
  with `--sim-response-timeouts`). A stream sends a `: clock=T` SSE comment before each event, since headers cannot
  change mid-stream. Other clients ignore the comments.
- The clock stops at the earliest checkpoint and reports the time in `X-Sim-Stopped`. Each waiting lane polls
  `POST /sim/clock`, decides what that time means for it, and then sends `X-Sim-Event: ack` with `X-Sim-At` to let the
  clock go on, or `cancel` to drop its request. The client makes every decision, as it would against a real server.
- `X-Sim-Event` on `/sim/clock` also sets the lanes of a level (`profile` with `X-Sim-Lanes`), pauses a lane while it
  does untimed work (`pause` with `X-Sim-Lane`) and resumes every lane (`resume`).


## Agent Skill

An [Agent Skill](skills/realmark/SKILL.md) is included in `skills/realmark/` so AI coding agents run RealMark consistently (default `C=1, 2, 4` concurrency sweep, `--probe` preflight, 3-run statistical rigor, buffered-stream verification, and color PNG/PDF card rendering via `skills/realmark/scripts/render_cards.py`).

To install it locally for Claude Code, Codex, or compatible agents, copy `skills/realmark/` into your agent's skills directory (for example `~/.claude/skills/realmark/`).
