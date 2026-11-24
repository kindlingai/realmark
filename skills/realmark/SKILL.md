---
name: realmark
description: Standard playbook for running, verifying, and presenting RealMark (RIPAL) LLM load benchmarks — default C=1, 2, 4 multi-lane sweeps, server sizing and preflight, --probe diagnostics, thinking-model parity, buffered-stream and missing-graph checks, 3-run statistical rigor, and true-color PNG/PDF card rendering.
---

# RealMark (RIPAL) Benchmark Skill

Use this skill whenever running, verifying, or reporting [RealMark](https://github.com/kindlingai/realmark) (`ripal.py`) benchmarks against any OpenAI-compatible inference server.

## 1. Standard Concurrency Is `C=1, 2, 4`

RealMark's standard sweep runs **three concurrency levels on a single card: `C=1`, `C=2`, and `C=4`** (`--lanes 1 2 4`, the default in `ripal.py`).

- **Default task schedule:** `1:16 2:8 4:4` (16 measured tasks on 1 lane, 8 per lane on 2 lanes, 4 per lane on 4 lanes), plus 2 unscored warmup tasks per lane (`--warmup-tasks 2`).
- **Headline RIPAL Score:** Geometric mean of `S_1`, `S_2`, and `S_4`, paired with a letter quality grade (`A+` through `F`).
- **Do not silently restrict to `--lanes 1`.** A `C=1`-only run omits the concurrency scaling curves and is not comparable to standard `C=1, 2, 4` RealMark cards.
- **When higher concurrency cannot fit:** Attempt `C=1, 2, 4` first. If `C=2` or `C=4` hits an OOM or hard slot limit, record the exact error and memory watermark, then clearly label any fallback run (e.g., `C=1 only` or `C=1, 2 only`) with the reason.

## 2. Server Sizing & Preflight

Each active lane opens with ~65K tokens of prior conversation history (`--history-words`, default ~65K tokens) and grows to **~95K–105K tokens** during the measured run:

1. **Concurrent slots & context:** Configure the server with **at least 4 concurrent slots** and **>= 110K–139K context per slot** so all 4 lanes keep full history without premature eviction or compaction.
2. **Prefix / prompt caching:** Keep prefix caching enabled (RealMark reuses >98–99% of prompt tokens across multi-turn tool loops).
3. **Clean baseline & memory tracking:** Verify no competing workloads are running on the GPU(s), clear OS page caches if measuring memory footprint on unified-memory systems, and record free/available memory before boot, after model load, minimum during the run, and at completion.
4. **Record exact versions:** Always record `git rev-parse HEAD` of `realmark`, exact server version/commit/image digest, exact model/quant revision, and full launch flags.

## 3. Standard Benchmark Command

Pass `--probe` on any new engine, release, or configuration so RealMark runs its pre-benchmark API & timing probe (saving `NAME.probe.json` alongside `NAME.json`).

For decision-grade or head-to-head results, run **3 full `C=1, 2, 4` runs** and report **mean ± SD** alongside every individual run:

```bash
for i in 1 2 3; do
  python3 -I ripal.py \
    --base-url http://127.0.0.1:8000/v1 \
    --model "<served-model-name>" \
    --lanes 1 2 4 \
    --probe \
    --ui plain --color never \
    --output "${OUT_DIR}/run${i}.json" \
    2>&1 | tee "${OUT_DIR}/run${i}.log"

  # Re-render clean plain-text and true-color ANSI cards offline from saved JSON
  python3 -I ripal.py --render "${OUT_DIR}/run${i}.json" --color never \
    --card-output "${OUT_DIR}/run${i}.card.txt" >/dev/null
  python3 -I ripal.py --render "${OUT_DIR}/run${i}.json" --color always \
    > "${OUT_DIR}/run${i}.ansi.txt"
done
```

### Handling Thinking Models & Head-to-Head Parity
- RealMark runs in the server's default thinking mode. If a model spends its `max_tokens` budget inside reasoning tokens during warmup (triggering RealMark's thinking auto-stop), pass a minimal documented override such as `--extra-body '{"chat_template_kwargs": {"enable_thinking": false}}'`, a low reasoning effort setting, or `--forced-tool-choice true`.
- In any head-to-head comparison, pass **identical** `--extra-body`, `--temperature`, `--forced-tool-choice`, and `--lanes` flags to every engine, and state them explicitly in the report.

## 4. Mandatory Post-Run Verification

Always inspect the raw `.json`, `.probe.json`, and `.card.txt` outputs before reporting numbers:

1. **Check for `"Buffered stream"` / missing right-hand graphs:**
   - Look for `Buffered stream: N of M replies arrived in one burst` or `No measured data available` on `Total decode~`, `Total prefill~`, and `Prefill by context`.
   - RealMark turns are tool calls. Servers whose tool-call parser buffers `<tool_call>...</tool_call>` until closing tag emit the entire reply in one burst after decode finishes. RealMark can only split prefill vs. decode on buffered streams if the server returns timing fields (e.g., `timings.prompt_ms` / `timings.predicted_ms`).
   - Never report cards with missing graphs without noting why. When comparing an engine with buffered streams against one that reports server timings, also compute apples-to-apples **Prefix Cache Hit %** (`sum(cached_tokens) / sum(prompt_tokens)`) and **Effective Completion tok/s** directly from `run*.json`.
2. **Verify completion, errors, and quality footnotes:**
   - Check `Done` counts at every `C` level, `API errors`, `Match (%)`, letter grade, and all numbered footnotes (lookup misses, mixed records, major word corruption).
3. **Verify `mix_valid` and stage time caps:**
   - Confirm `mix_valid=True` and note whether any level hit its measured time cap (`projected full batch`).
4. **Use `--resume` rather than discarding partial runs** if a long multi-level run was interrupted cleanly (note: `--resume` only resumes matching protocol versions).

## 5. Rendering Clean PNG & PDF Cards

To share verbatim RealMark cards with intact box borders, Braille plots, and RealMark's official warm-dark ANSI color palette (`--color always`), use the bundled script (`playwright` optional dependency):

```bash
python3 skills/realmark/scripts/render_cards.py \
  --out-dir ./cards \
  --title "Model Name — RealMark (C=1, 2, 4)" \
  --subtitle "1x GPU · Engine A vs Engine B" \
  --card "engine-a-r1|Engine A · Run 1|./raw/run1.json" \
  --card "engine-b-r1|Engine B · Run 1|./raw/run2.json"
```

This generates 2x Retina PNGs, per-card PDFs, a combined landscape PDF (`realmark-all-cards.pdf`), and an `index.html` gallery. Always inspect at least one rendered PNG to verify aligned borders and plots, and strip local filesystem paths before sharing externally.

## 6. Dry-Runs with the Built-In Simulator

Before running against expensive hardware or when validating pipelines/parsers, test with the built-in simulator:

```bash
python3 -I ripal.py --probe --simulator="--realtime 4" --output /tmp/sim-test.json
```
