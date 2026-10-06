# Tasks

## 1. Update Tests First

- [x] 1.1 Update `tests/test_compose_contract.py` expectations for the new topic name (`mill-cuts-atleastonce-source` in topic init and `CUT_TOPIC` assertions, ~lines 112 and 156) and confirm the test fails against the unmodified `compose.yaml`
- [x] 1.2 Update `src/test/java/com/improving/gotogether/cuts/ConsumerLagExporterTest.java` (~lines 28, 47) to expect `mill-cuts-atleastonce-source` and confirm the test fails against the unmodified exporter
- [x] 1.3 Update the expected-topics list in `scripts/smoke_check.py` (~line 39) to `mill-cuts-atleastonce-source`

## 2. Apply the Rename

- [x] 2.1 In `compose.yaml`, rename the topic in the init loop (~line 151) and the `CUT_TOPIC` env vars of `mill-cuts-at-least-once-producer` (~line 271) and `mill-cuts-idempotent-consumer` (~line 308); verify `tests/test_compose_contract.py` now passes
- [x] 2.2 Update the hardcoded topic in `src/main/java/com/improving/gotogether/cuts/ConsumerLagExporter.java` (~line 38); verify `ConsumerLagExporterTest` passes
- [x] 2.3 Update `scripts/register_cut_schemas.py` (~line 12) so the at-least-once topic/subject is `mill-cuts-atleastonce-source` / `mill-cuts-atleastonce-source-value`; verify no hardcoded `mill-cuts-replay-source` remains in the script

## 3. Update Documentation and Specs

- [x] 3.1 Update `README.md` pipeline diagram (~line 65) and AKHQ topology expectations (~line 122) to `mill-cuts-atleastonce-source`
- [x] 3.2 Sync the `pipeline-visualization` delta to `openspec/specs/pipeline-visualization/spec.md` so the "Cut pipelines visible" scenario names `mill-cuts-atleastonce-source` (spec scenario: Cut pipelines visible)

## 4. Verify

- [x] 4.1 Grep the repo for `mill-cuts-replay-source` and confirm zero matches outside `openspec/changes/archive/`
- [x] 4.2 Run the Python contract tests and Java unit tests and confirm all pass
- [x] 4.3 With user permission, restart the stack (`docker compose down -v && docker compose up`) and run `scripts/smoke_check.py`; confirm `mill-cuts-atleastonce-source` exists and the AKHQ/StreamLens topology shows `mill-cuts-at-least-once-producer` → `mill-cuts-atleastonce-source` → `mill-cuts-idempotent-consumer`
