# RIPAL Mark

Reasoning Independent Pseudo Agentic Load: a local AI prefill/decode benchmark.
Python 3.10+, standard library only. Requires an OpenAI-compatible server with tool calling.

```sh
python3 ripal.py --base-url http://localhost:8000/v1 --model MODEL --lanes 1 2 4
```

Defaults: two warmup tasks, then one measured pass of four tasks per lane.
Each concurrency level is warmed and measured before starting the next.
Adaptive padding/history target 4:1 fresh-input/decode; early lanes run unscored filler.
Reports load and edit-distance-adjusted scores, errors, and a braille concurrency plot.
Saves `ripal-results.json` and `ripal-results.card.txt`.

Ctrl-C saves partial results. HTTP 429 or a 60-second response deadline stops the run.
Use `--help` for options, including `--timeout`, `--repeats`, and `--fixed-mix`.
