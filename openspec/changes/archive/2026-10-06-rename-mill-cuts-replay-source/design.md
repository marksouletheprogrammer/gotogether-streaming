# Design

## Context

The at-least-once cut pipeline's source topic `mill-cuts-replay-source` is referenced in `compose.yaml` (topic init loop, `CUT_TOPIC` for `mill-cuts-at-least-once-producer` and `mill-cuts-idempotent-consumer`), `ConsumerLagExporter.java` (line ~38), `scripts/register_cut_schemas.py` (line ~12), `scripts/smoke_check.py` (expected topics, line ~39), `tests/test_compose_contract.py` (lines ~112, 156), `ConsumerLagExporterTest.java` (lines ~28, 47), `README.md` (lines ~65, 122), and the `pipeline-visualization` spec scenario. See proposal.md for motivation.

## Goals / Non-Goals

**Goals:**
- Replace every live reference to `mill-cuts-replay-source` with `mill-cuts-atleastonce-source`.
- Keep the rename a single mechanical pass so it is trivially reviewable and revertable.

**Non-Goals:**
- Renaming producer/consumer service names, client IDs, consumer group IDs, database tables, or any other topic.
- Editing archived change documents under `openspec/changes/archive/` — they are historical records.
- Deleting an orphaned old topic/subject left in existing local volumes; teardown/recreate handles that.

## Decisions

- **Literal rename, no alias/transition period.** Kafka has no topic aliasing; dual-publishing would add complexity for zero benefit in a disposable local demo. The old topic simply stops being created.
- **New name is `mill-cuts-atleastonce-source`,** matching the requested spelling. Alternative `mill-cuts-at-least-once-source` was rejected to keep the topic name shorter while still reading clearly; `atleastonce` still distinguishes it from the producer service `mill-cuts-at-least-once-producer` only by the `-source` suffix. Assumption recorded: if hyphenated `at-least-once` is preferred, only the literal differs and every location below changes identically.
- **Schema subject follows the topic.** `register_cut_schemas.py` registers `<topic>-value`, so the subject becomes `mill-cuts-atleastonce-source-value` automatically; verify the script derives the subject from the topic name rather than hardcoding it, and update any hardcoded subject string.
- **Lag exporter topic list.** `ConsumerLagExporter` hardcodes the topics it reports lag for; update its constant so the topic-labeled lag series uses the new name.
- **Update the main spec via delta, not by editing `openspec/specs/` directly.** The `pipeline-visualization` delta carries the scenario change; archive/sync applies it.

## Risks / Trade-offs

- [Existing local Kafka/Postgres volumes retain the old topic and `mill-cuts-replay-source-value` subject after restart] → Harmless leftovers in a demo stack; README teardown (`down -v`) or a fresh `up` recreates clean state. Note this in README if a section covers persisted state.
- [A missed reference produces a consumer stuck on a nonexistent topic or a smoke-check failure] → Mechanical grep for `mill-cuts-replay-source` across the repo (excluding `openspec/changes/archive/`) before and after the edit; smoke check and contract tests catch leftovers.

## Migration Plan

1. Apply the rename across compose, Java, scripts, tests, and docs in one commit.
2. Restart the stack (or `down -v && up`) so topics/subjects are recreated under the new name.
3. Verify via smoke check and the AKHQ/StreamLens topology per the proposal's "How to Know It's Working".
4. Rollback: revert the commit and restart.
