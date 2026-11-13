# Realmark

Realmark gives you a RIPAL (Reasoning Independent Pseudo Agentic Load) score for LLM models based
on their performance running reasoning-independent interactive tasks.

This performance is based on the model's agentic interactivity and gives you an idea of how "good"
a model is to work with

## RIPAL Tests

The RIPAL tests are reasoning-independent, which means they do not measure an agent's general intelligence,
only its ability to perform basic non-copying transformations and "needle-like" lookups. This does
not count 

Tasks include:

 - Pig Latin
 - Word reversing
 - Vowel removal (`aeiou`), and
 - Case inversion (e.g. `Hello` -> `hELLO`)

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

## Quality Score

Each run also gets a quality score as a letter grade. While the tasks are reasoning-independent, model and quantization quality
tends to be inversely proportional to the final score. Stronger models tend to be able to perform multi-token tasks with
greater ease, and make fewer mistakes. The quality score also measures the model's ability to locate specific content
in larger toolcall responses, which similarly tracks the quality of an engine's KV implementation and its quantization.





