# RIPAL Mark

Reasoning Independent Pseudo Agentic Load: a local AI prefill/decode benchmark.
Python 3.10+, standard library only. Requires an OpenAI-compatible server with tool calling.

```sh
python3 ripal.py --base-url http://localhost:8000/v1 --model MODEL --lanes 1 2 4
```

For authentication, set `OPENAI_API_KEY` (or select another variable with `--api-key-env NAME`).

Tasks cycle through Pig Latin, reverse each word, remove vowels (`aeiou`), and invert case.
The 293-word vocabulary excludes vowel-initial words for Pig Latin only; case tasks use mixed capitalization.

Defaults: each lane keeps one continuous history with two warmups, followed by 16 measured tasks at C=1,
8 at C=2, and 4 at C≥4. Set `--task-schedule 1:16 2:8 4:4` to change the concurrency thresholds;
the last entry applies to higher levels. `--tasks N` instead uses N at every level.
`--warmup-tasks N` controls warmups separately.
Add `--no-filler` to prevent extra unscored tasks on early-finishing lanes.
Timing and prefill/decode statistics exclude warmups. Each concurrency finishes before the next starts.
Adaptive padding targets 4:1 fresh-input/decode; early lanes run unscored filler.
Reads the model context limit from `/models` and stops admitting tasks before estimated overflow,
reserving room for generation. Uses `/tokenize` when available, otherwise a conservative size estimate.
Use `--context-window TOKENS` if the server omits its limit, or to set a smaller cap.
Reports load score, edit-distance match %, errors, and autoscaled braille charts with axes.
Streams responses to estimate prefill/decode tok/s and average completion time per concurrency.
Rates show estimated total (per lane).
Prefill estimates include queue/network time; streaming chunks may buffer tokens.
Saves `ripal-results.json` and `ripal-results.card.txt`.
Re-render offline: `python3 ripal.py --render ripal-results.json` (`--card-output card.txt` saves it).

Measured stages have a warmup-calibrated budget: forecast fresh tokens / 1,000 tok/s + streamed decode allowance when available,
rounded up to a minute. Caps cancel pending work and continue to the next concurrency level.
Completed work determines the score; full completion times are extrapolated and labeled as estimates.
Extrapolation assumes the observed completed-task rate continues.
Scores use one fixed eight-task work unit. `--stage-prefill-rate N` adjusts the floor;
`--stage-timeout SECONDS` sets a fixed cap (`0` disables it).

Ctrl-C saves partial results. HTTP 429 or a response deadline stops the run.
Deadlines start at 60s, then use 4× the previous concurrency's longest completed response.
Use `--help` for options, including `--timeout`, `--repeats`, and `--fixed-mix`.
