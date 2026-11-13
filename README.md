# Realmark

Realmark gives you a RIPAL (Reasoning Independent Pseudo Agentic Load) score for LLM models based
on their performance running reasoning-independent interactive tasks.

This performance is based on the model's agentic interactivity and gives you an idea of how "good"
a model is to work with

## RIPAL Score

RIPAL scores are calibrated so a model with a score of 1,000 can complete most interactive agentic
tasks, including those with large tool calls, in a reasonable amount of time. Note that RIPAL scores are not
based on tokens as tokenization varies between models. Instead, RIPAL uses words, and tasks based around
transformations of whole words.

The score is defined as:

$$
S_C = 1000 \cdot \frac{\operatorname{asinh}\!\left(\dfrac{W_C}{10\,T_C}\right)}{\operatorname{asinh}(1)}
$$

where

- $W_C$ is the weighted visible output words all lanes' finished tasks (each punctuation mark or symbol counts as $0.25$),
  across reply text and tool-call arguments. Thinking is excluded.
- $T_C$ is the wall-clock time in seconds, from the start of non-warmup work until the last lane finishes.

By default, RIPAL measures four levels of concurrency (C=1, 2 and 4), but this can be overridden on
the command-line.

## RIPAL Tests

The RIPAL tests are reasoning-independent, which means they do not measure an agent's general intelligence,
only its ability to perform basic non-copying transformations and "needle-like" lookups. This does
not count 

Tasks include:

 - Pig Latin
 - Word reversing
 - Vowel removal (`aeiou`), and
 - Case inversion (e.g. `Hello` -> `hELLO`)


