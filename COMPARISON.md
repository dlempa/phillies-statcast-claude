# Codex vs Claude Code — head-to-head log

This repo is the Claude-Code variant of [dlempa/phillies-statcast](https://github.com/dlempa/phillies-statcast). Both repos start from the same baseline commit and evolve in parallel — the original under OpenAI Codex, this one under Claude Code. The point is to compare how the two tools collaborate over time on a real, ongoing project.

Each wave below corresponds to one focused PR per repo addressing the same brief. Capture is intentionally light-touch: enough signal to compare, not enough to become a chore.

## Wave log

| Wave                       | Tool         | Prompts | Elapsed | LOC ± | Subjective notes |
| -------------------------- | ------------ | ------: | ------: | ----: | ---------------- |
| Wave 1 (bootstrap)         | Claude Code  |         |         |       |                  |

Subsequent waves (visual polish, new features, data-layer reliability) get added as their PRs land — one row per tool per wave.

## Defects

Post-merge bugs caught later go here, with which tool produced the regression and how it was found. Empty so far.

## Method

- **Prompts**: rough count of user turns to land the wave. Don't sweat off-by-one.
- **Elapsed**: wall-clock from kickoff to merged PR — including waiting on the user.
- **LOC ±**: net diff of the merged PR vs the prior baseline (`git diff --shortstat`).
- **Subjective notes**: one line on what felt different about the collaboration. This is the actually-useful column — code metrics tend to look similar.
