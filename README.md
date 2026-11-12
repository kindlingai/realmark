# Realmark

Realmark gives you a RIPAL (Reasoning Independent Pseudo Agentic Load) score for LLM models based
on their performance running reasoning-independent interactive tasks.

This performance is based on the model's agentic interactivity and gives you an idea of how "good"
a model is to work with.

Python 3.10+, standard library only.
Requires an OpenAI-compatible server with native tool calling.

```sh
python3 ripal.py --base-url http://localhost:8000/v1 --model MODEL --lanes 1 2 4
python3 ripal.py --simulate --sim-prefill 1000 --sim-decode 50 --lanes 1 2 4
python3 ripal.py --render realmark-MODEL.json
```

Set `OPENAI_API_KEY` for authentication; `--api-key-env NAME` selects another variable.

Each lane retains two warmups followed by measured work. Defaults:
`--task-schedule 1:16 2:8 4:4`; entries are minimum concurrency thresholds.
`--tasks N` uses N at every level; `--warmup-tasks N` controls warmups.
Tasks cycle through Pig Latin, reverse words, remove vowels, and invert case.
The 293-word vocabulary excludes vowel-initial words for Pig Latin only.
Generation starts at five words (`--words N`) and adapts with padding toward
4:1 fresh-input/decode **tokens**. `--fixed-mix` freezes both. Response budgets grow
with the requested text. Context capacity is read from the model; use
`--context-window TOKENS` when unavailable or to impose a smaller cap.

**Score 1,000 = 1,000 weighted generated words in 100 seconds.**
Words/identifiers count as 1; punctuation/symbols count as 0.25. Generated tool
arguments count; transport metadata and hidden reasoning do not. The global curve
is `1000 * asinh(weighted_words_per_second / 10) / asinh(1)`.
Only completed measured tasks earn credit. Warmups, filler, and advanced timeout
tails are excluded. Match grades measure correctness separately.
Mix **w/t** shows newly supplied visible words/output words and fresh/decode tokens.
The word ratio and server prefill ratio measure different things.

Measured-stage budgets forecast fresh tokens / 1,000 tok/s plus warmup decode time,
rounded up to a minute. At the cap, the slowest in-flight task cohort finishes;
no new tasks start. Advanced tails are cancelled when that cohort finishes.
`--stage-timeout SECONDS` overrides this (`0` disables it).
Early lanes run unscored filler unless `--no-filler` is set.
Ctrl-C saves partial data. HTTP 429 or response deadlines stop the run.
Response deadlines start at 60s, then use 4× the previous concurrency maximum;
larger submissions scale that budget. Stream stalls still time out.

JSON stores configuration and timestamped raw lane telemetry under
`runs["c=2"].lanes[n].telemetry`: warmup/run/filler/partial event arrays,
including requests, streamed chunks, responses, and tool messages. Scores and
charts are recomputed on render. Old processed-result files are unsupported.
Defaults to `realmark-MODEL.json` and `realmark-MODEL.card.txt`; model names are
sanitized for filenames. `--output PATH` overrides the filename. The `.card.txt`
contains the rendered result.
Graph bars use completed tasks only. RIPAL Score shows ±1 task-adjusted SD:
within each concurrency, seconds per weighted output word are multiplied by the
overall task mean / transformation mean. Normalized times produce task scores;
pooled within-transformation score residuals use N − G degrees of freedom
(G = observed transformation types). No residual degrees of freedom means no bar.
The overall score is unchanged; adjusted SD describes within-type variation, not
uncertainty in the overall score. Decode/prefill show ±1 sample SD. Prefill/decode samples
combine token totals and phase durations across each task’s requests. Fewer than
two completed task samples omit the SD; warmups and partial tasks are excluded.
Y bounds round outward to two significant digits, with the minimum clamped to zero.
Points/error bars at that boundary overwrite the horizontal axis.

The simulator is always correct and uses a discrete-event clock, with no real
model waits. Prefill/decode capacities are shared across active requests per phase.
`--sim-cache-block` (8192) and `--sim-context` (1,048,576) configure its virtual model.
Simulator stage/response timeouts are off by default so full schedules finish.
Use `--stage-timeout N` and `--sim-response-timeouts` to exercise those limits.
Use `--help` for other options.

Card notes flag long output loops, major expected-word corruption, unexpected
scripts/possible language switches, suspected cache pressure, and inferred cache-hit
granularity. Superscript references annotate the relevant table cells. These are
offline diagnostics, not score adjustments. Cache thrashing needs server-side
confirmation; block alignment cannot prove partial-tail recomputation.
