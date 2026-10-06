# Proposal

## Why

The at-least-once cut pipeline's source topic is named `mill-cuts-replay-source`, but nothing else in the system uses the word "replay" — the matching producer is `mill-cuts-at-least-once-producer`, the consumer group is `mill-cuts-idempotent-consumer`, and the documentation describes the path as the "at-least-once" pipeline. The mismatched topic name makes the demo harder to explain and the topology harder to read. Renaming the topic to `mill-cuts-atleastonce-source` aligns the topic identifier with the pipeline's stated delivery guarantee and the naming convention used by the rest of the system.

## What Changes

- Rename the at-least-once cut source topic from `mill-cuts-replay-source` to `mill-cuts-atleastonce-source` in topic creation (`compose.yaml` init step). **BREAKING**: the old topic name no longer exists; persisted topic data under the old name is abandoned, which is acceptable for a local demo stack.
- Update the `CUT_TOPIC` environment variable for `mill-cuts-at-least-once-producer` and `mill-cuts-idempotent-consumer` in `compose.yaml`.
- Update the topic reference in `ConsumerLagExporter` (`src/main/java/.../ConsumerLagExporter.java`).
- Update schema registration for the `<topic>-value` subject in `scripts/register_cut_schemas.py` (subject becomes `mill-cuts-atleastonce-source-value`).
- Update `scripts/smoke_check.py` expected topic list.
- Update contract/unit tests that reference the topic (`tests/test_compose_contract.py`, `ConsumerLagExporterTest.java`).
- Update `README.md` pipeline diagram and AKHQ inspection steps.
- Update the `pipeline-visualization` spec scenario that names the topic.
- Archived change documents under `openspec/changes/archive/` are intentionally left unchanged — they are historical records.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `pipeline-visualization`: the topology scenario names `mill-cuts-replay-source` as the at-least-once producer's source topic; it must name `mill-cuts-atleastonce-source`.

## Impact

- **Code**: `compose.yaml` (topic init, two `CUT_TOPIC` env vars), `ConsumerLagExporter.java`, `scripts/register_cut_schemas.py`, `scripts/smoke_check.py`.
- **Tests**: `tests/test_compose_contract.py`, `src/test/java/.../ConsumerLagExporterTest.java`, possibly `scripts/smoke_check.py` expectations.
- **Docs**: `README.md` (pipeline diagram, AKHQ topology expectations).
- **Specs**: `openspec/specs/pipeline-visualization/spec.md` scenario text.
- **Runtime**: after restart, the old `mill-cuts-replay-source` topic and its `mill-cuts-replay-source-value` schema subject may linger in local Kafka/Registry state until volumes are reset; nothing reads them.

## Rollback Plan

Revert the rename commit. Because the topic name only exists in configuration, one Java constant, scripts, tests, and docs, a single revert restores the old name. If the stack was restarted under the new name, restart again after revert; orphaned `mill-cuts-atleastonce-source` topic/subject are harmless leftovers.

## How to Know It's Working

- `scripts/smoke_check.py` passes, confirming `mill-cuts-atleastonce-source` exists and all services start cleanly.
- Contract tests pass (`tests/test_compose_contract.py`, monitoring contract tests).
- AKHQ topology shows `mill-cuts-at-least-once-producer` → `mill-cuts-atleastonce-source` → `mill-cuts-idempotent-consumer`.
- Schema Registry serves `mill-cuts-atleastonce-source-value`.
- No remaining references to `mill-cuts-replay-source` outside `openspec/changes/archive/`.
