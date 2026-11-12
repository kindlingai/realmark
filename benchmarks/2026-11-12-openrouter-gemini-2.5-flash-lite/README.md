# OpenRouter: Gemini 2.5 Flash-Lite

Recorded 2026-11-12 using Realmark `e486749` (protocol `ripal-v11`).
Google AI Studio priority endpoint, pinned with fallbacks disabled; reasoning off.
Synthetic benchmark prompts only. The API key is not part of this dataset.

```sh
python3 ripal.py \
  --base-url https://openrouter.ai/api/v1 \
  --model google/gemini-2.5-flash-lite \
  --api-key-env OPENROUTER_API_KEY \
  --lanes 1 2 4 \
  --extra-body '{"provider":{"only":["google-ai-studio/priority"],"allow_fallbacks":false},"reasoning":{"enabled":false}}' \
  --output realmark-google-gemini-2.5-flash-lite.json
```

Defaults: two warmup tasks per lane, measured tasks per lane 16/8/4 at C=1/2/4,
seed 42, one repeat, adaptive 4:1 fresh/decode token target, continuous histories,
no validation retries, and calibrated stage caps. Filler is enabled.

Re-render the raw session log with the current script:

```sh
python3 ripal.py --render benchmarks/2026-11-12-openrouter-gemini-2.5-flash-lite/realmark-google-gemini-2.5-flash-lite.json
```

The adjacent `.card.txt` is a display artifact. JSON contains the original config
and raw lane transcripts. Rates are client-side streaming estimates and include
OpenRouter/provider transport and queueing; they are not isolated hardware rates.

Endpoint pricing at run time: $0.18/M input, $0.72/M output tokens.
Source: https://openrouter.ai/api/v1/models/google/gemini-2.5-flash-lite/endpoints

An earlier GPT-OSS 120B/Cerebras attempt stopped on HTTP 429 before C=2 and C=4;
it is not included as the stored reference run.

## Observed result

| C | RIPAL Score | Completed cycles | Match | Measured time |
|---|---:|---:|---:|---:|
| 1 | 89.8 | 9/16 | 0% | 187.47 s |
| 2 | 1,318.7 | 13/16 | 0% | 164.34 s |
| 4 | 815.3 | 16/16 | 0% | 85.09 s |

This is a failure/interoperability reference, not a clean speed baseline:

- Every measured task has zero match credit because its routing, submission, or
  transformation failed. Completed cycles still count toward the load score.
- The raw upstream chunks contain doubled `.jsonl` path suffixes. Many submissions
  end with native `MALFORMED_FUNCTION_CALL`, no tool call, and no usage counters.
- C=1 reached its stage cap; C=2 had context-limited work. The C=1 card time is an
  extrapolation; the table above gives actual observed time instead.
- Some outputs loop or contain major word corruption; the card includes notes.
- Buffered tool-argument chunks inflate decode-rate estimates. Do not interpret
  those displayed rates as the model's token-generation speed. Missing usage
  also leaves fresh/decode token mix unknown and prevents some adaptive updates.

Recorded elapsed time including warmups/filler: 460.27 s.

SHA-256 of the script used: `2be233ffae7a9a59f3e1d93ff5d9171af643321b716aa75048642fc6798fd69a`.

Reported usage cost for the stored run: **$0.16056**. This is a lower bound: 36
responses omitted usage/cost, and their generation metadata was unavailable. The
excluded Cerebras attempt reported an additional $0.01065; tiny endpoint probes
are also excluded.
