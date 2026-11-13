# Word-based workload (protocol v13)

The default load targets **2 weighted words of newly supplied input per expected
visible output word**. Words/identifiers count as 1; non-whitespace punctuation
and symbols count as 0.25. Expected output counts native tool argument JSON;
input counts new message content and tool results. Repeated history is not counted
again. Transport metadata, tool schemas, and hidden reasoning are excluded.

Source payloads are fixed at **256 words** (`--words N`). The target record has
that many words; distractor records together have approximately half that many.
Before sending a task, the harness computes its expected input/output and adds
padding to bring the cumulative session mix to 2:1. Integer padding may introduce
less than one word of rounding error. Warmups and measured work have separate
word budgets; measured timing excludes warmups and retains their context.
Filler continues the lane history but earns no score credit.

This construction uses no model token counters or ratio-calibration requests.
Errors, loops, and thinking cannot enlarge later tasks. Thinking receives no word
credit, but its time is included. The card's word mix reports actual received
output, so failures or cancelled filler tails can differ from the intended mix.
Token ratios and cache statistics remain separate diagnostics. Output counts can
support decode estimates even when cached-token counts are absent; fresh prefill
remains unknown. Buffered streaming can still make token-rate estimates unreliable.

`--fixed-mix` instead uses `--words` and `--filler-words` literally. Small source
payloads or many distractors may have an input-overhead floor above 2:1; padding
then bottoms out at zero and the card annotates an out-of-tolerance planned mix.
The source count is never silently increased. `--ratio-tolerance` controls the
word-mix verification tolerance (default 15%). The old token-ratio calibration and
`--calibration-rounds` prepass have been removed.

The archived Flash-Lite benchmark uses protocol v11's token-based sizing. Its raw
log and recorded card are retained unchanged; it is not a v12 reference run.

## Seeded history (v13)

Each lane's conversation opens with about **40,000 weighted words** (`--history-words`,
about 65K GLM-5.3 tokens) of earlier completed tasks: the same three tool calls per task,
with correct answers and `ok: true` results. Real agent sessions run on long cached
contexts: across 30 sampled Claude Code sessions, the median of per-session median context
was 188K tokens (10th percentile 60K), and cache reads were 98.6% of input. Without a
history, RealMark contexts stayed at a few thousand to tens of thousands of tokens, so
attention cost and prefix-cache capacity barely mattered.

Each lane gets its own history, so lanes share only the system prompt in a prefix cache.
The 2:1 word mix and the score both exclude the history. The first warmup request of
each lane prefills it, and that request's deadline grows by 2 tokens per history word
times the lane count, divided by the stage prefill floor (1,000 tokens/s by default). With `--warmup-tasks 0` the measured time includes that prefill. Compaction
(`--history task`) keeps the history. `--history-words 0` restores the v12 workload,
although v13 task seeds differ.

The card notes when a lane's cache hits cover under half of its previous prompt
(prompts of 32,768 tokens and more): the server is re-prefilling the history, usually
because the lanes' contexts exceed its cache.

## Simulator context costs

`--simulate` now slows a request as its context grows: its prefill and decode rates halve
at `--sim-prefill-half-context` and `--sim-decode-half-context` tokens (131,072 each by
default; 0 turns the effect off). `--sim-kv-tokens` caps the prefix cache shared by all
lanes and evicts the least recently used lanes; 0 (the default) is unlimited.
