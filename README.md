# Realmark

Realmark gives you a RIPAL (Reasoning Independent Pseudo Agentic Load) score for LLM models based
on their performance running reasoning-independent interactive tasks.

This performance is based on the model's agentic interactivity and gives you an idea of how "good"
a model is to work with.

## RIPAL Score

RIPAL scores are calibrated so a model with a score of 1,000 can complete most interactive agentic
tasks, including those with large tool calls, in a reasonable amount of time. RIPAL scores are not
based on tokens as tokenization varies between models.

By default, RIPAL measures four levels of concurrency (C=1, 2 and 4), but this can be overridden on
the command-line.

## RIPAL Tests

The RIPAL tests are reasoning-independent, which means they do not measure an agent's general intelligence,
only its ability to perform basic non-copying transformations and "needle-like" lookups.

Tasks include:

 - Pig Latin
 - Word reversing
 - Vowel removal (`aeiou`), and
 - Case inversion (e.g. `Hello` -> `hELLO`)


