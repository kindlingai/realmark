# Word-based workload (protocol v12)

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
