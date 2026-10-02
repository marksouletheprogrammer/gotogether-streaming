# CNC milling data for a predictive-maintenance demo

This document describes **one simulated CNC milling cell**, not every signal a real machine can expose. The JSON Schema is an illustrative demo contract, **not** an MTConnect, OPC UA, or ISO implementation. Real machines differ by controller, sensors, and accessories; see [Standards and further reading](#standards-and-further-reading) for their actual information models.

## Demo assumptions

- One three-axis mill (`mill-1`), one cutter type, one material (42CrMo4 steel), fixed cutting settings (8,000 rpm and 600 mm/min), and coolant commanded on for every cut. These are **invented demo settings**, not recommended machining parameters. Holding them fixed lets us compare successive cuts without modeling changing operating conditions.
- One `cut` record **after each completed cutting cycle**. An edge aggregator supplies three nonnegative measurements: cutting-phase vibration RMS in `g`, mean spindle current in `A`, and mean coolant flow in `L/min`. The demo does not stream raw waveforms or model idle, rapid, or warmup phases. Missing readings are rejected/flagged rather than recorded as zero.
- Two simulated, independently inspectable faults: `chipped_cutter` and `low_coolant_flow`. The demo knows scripted outcomes for evaluation, but an assessment may use **only** cuts and earlier events available at its `as_of` time. If both signatures appear, diagnosis may say `unknown` rather than claim a single cause.
- Each physical cutter has a stable `tool_instance_id`; its `cut_index` starts at 1 and increases by one for each completed cut. Simulate multiple cutter lifecycles, not just one failure, for a meaningful training/test split.
- A simulated technician reports installations, inspections, and maintenance using `event` records. An inspection or work order confirms a fault (or explicitly says `none`/`unknown`). `planned: true` maintenance is **not** a confirmed unplanned failure. Do not assume a tool-life warning or unusual sensor value proves a fault.

A real [published milling dataset](https://www.nature.com/articles/s41597-025-04923-y) collected much faster raw vibration and motor-current signals and organized them into cutting cycles. Per-cut aggregation is a **demo simplification** of that pattern, not a claim that real machines emit precisely these three precomputed numbers.

## Three record types

- `cut`: one completed cut and its three measurements. These are the **input telemetry**.
- `event`: installation, technician inspection, or maintenance. These are the **outcomes and tool history**, not continuously available sensor features.
- `assessment`: a derived anomaly, fault hypothesis, or advance warning made using only data known as of the referenced cut. This is **model/rule output**, not a machine measurement.

The common envelope ties records to `mill-1`, a cutter instance, and an event time. `event_id` permits deduplication; `occurred_at` is event time rather than arrival time. This is [JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12); `description` and `$comment` are documentation, not JavaScript-style JSON comments. A validator must enable `format` checking to enforce RFC 3339 timestamps. Cross-record rules (cut order, `as_of` ordering, installation before use, and whether faults were actually confirmed) also require application checks.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Simulated CNC milling demo record",
  "description": "Illustrative per-cut demo contract, not a machine-vendor or standards schema.",
  "type": "object",
  "required": ["schema_version", "event_id", "kind", "occurred_at", "machine_id", "tool_instance_id", "payload"],
  "properties": {
    "schema_version": {"const": "demo-1"},
    "event_id": {"$ref": "#/$defs/id", "description": "Stable record ID for replay/deduplication."},
    "kind": {"enum": ["cut", "event", "assessment"]},
    "occurred_at": {"$ref": "#/$defs/timestamp", "description": "Source event time, UTC in the examples."},
    "machine_id": {"const": "mill-1"},
    "tool_instance_id": {"$ref": "#/$defs/id", "description": "Physical cutter ID, not tool slot number."},
    "payload": {"type": "object"}
  },
  "oneOf": [
    {"properties": {"kind": {"const": "cut"}, "payload": {"$ref": "#/$defs/cut"}}},
    {"properties": {"kind": {"const": "event"}, "payload": {"$ref": "#/$defs/event"}}},
    {"properties": {"kind": {"const": "assessment"}, "payload": {"$ref": "#/$defs/assessment"}}}
  ],
  "additionalProperties": false,
  "$defs": {
    "id": {"type": "string", "minLength": 1},
    "timestamp": {"type": "string", "format": "date-time"},
    "cut": {
      "type": "object",
      "required": ["cut_index", "vibration_rms_g", "spindle_current_a", "coolant_flow_l_min"],
      "properties": {
        "cut_index": {"type": "integer", "minimum": 1, "description": "Completed cuts for this tool instance, starting at 1."},
        "vibration_rms_g": {"type": "number", "minimum": 0, "description": "Cutting-phase vibration root-mean-square acceleration in g."},
        "spindle_current_a": {"type": "number", "minimum": 0, "description": "Mean spindle motor current over this cut, amperes."},
        "coolant_flow_l_min": {"type": "number", "minimum": 0, "description": "Mean coolant flow over this cut, liters per minute."}
      },
      "additionalProperties": false
    },
    "event": {
      "type": "object",
      "required": ["event_type"],
      "properties": {
        "event_type": {"enum": ["tool_installed", "inspection", "maintenance"]},
        "cut_index": {"type": "integer", "minimum": 1, "description": "Most recent completed cut for this tool, when applicable."},
        "component": {"enum": ["cutter", "coolant_system"], "description": "Component inspected or serviced."},
        "confirmed_fault": {
          "enum": ["none", "chipped_cutter", "low_coolant_flow", "unknown"],
          "description": "Technician finding; unknown is not a negative training label."
        },
        "planned": {"type": "boolean", "description": "Whether maintenance was scheduled; relevant only to maintenance."},
        "replacement_tool_instance_id": {"$ref": "#/$defs/id", "description": "New cutter if this event replaces the old one."}
      },
      "allOf": [
        {"if": {"properties": {"event_type": {"const": "inspection"}}, "required": ["event_type"]}, "then": {"required": ["component", "confirmed_fault"]}},
        {"if": {"properties": {"event_type": {"const": "maintenance"}}, "required": ["event_type"]}, "then": {"required": ["component", "confirmed_fault", "planned"]}}
      ],
      "additionalProperties": false
    },
    "assessment": {
      "type": "object",
      "required": ["task", "as_of", "cut_index", "based_on_event_id", "result"],
      "properties": {
        "task": {"enum": ["anomaly", "diagnosis", "forecast"]},
        "as_of": {"$ref": "#/$defs/timestamp", "description": "Latest allowed input time; never use future inspections/maintenance."},
        "cut_index": {"type": "integer", "minimum": 1, "description": "Cut on which this assessment was made."},
        "based_on_event_id": {"$ref": "#/$defs/id", "description": "ID of the assessed cut record."},
        "result": {
          "enum": ["normal", "unusual", "chipped_cutter", "low_coolant_flow", "unknown", "no_alert", "inspect_tool_next_stop"],
          "description": "Interpret in the context of task; a diagnosis is a hypothesis until inspection."
        },
        "score": {"type": "number", "description": "Rule/model score, NOT necessarily a calibrated probability."},
        "horizon_cuts": {"type": "integer", "minimum": 1, "description": "Forecast lookahead in completed cuts."},
        "minimum_lead_cuts": {"type": "integer", "minimum": 1, "description": "Earliest future cut counted as actionable failure."}
      },
      "allOf": [
        {"if": {"properties": {"task": {"const": "forecast"}}, "required": ["task"]}, "then": {"required": ["horizon_cuts", "minimum_lead_cuts"]}}
      ],
      "$comment": "Also check that minimum_lead_cuts <= horizon_cuts, as_of is no later than this assessment, and based_on_event_id points to a cut from the same tool.",
      "additionalProperties": false
    }
  }
}
```

### Example: a cut, a forecast, and its later outcome

All readings, names, and timestamps below are **invented**. The forecast is emitted before the maintenance event; the later result is shown only to explain retrospective evaluation.

```json
{
  "schema_version": "demo-1", "event_id": "cut-40", "kind": "cut",
  "occurred_at": "2026-03-04T10:15:00Z", "machine_id": "mill-1",
  "tool_instance_id": "cutter-45",
  "payload": {"cut_index": 40, "vibration_rms_g": 0.27,
    "spindle_current_a": 11.4, "coolant_flow_l_min": 12.0}
}
```

```json
{
  "schema_version": "demo-1", "event_id": "forecast-40", "kind": "assessment",
  "occurred_at": "2026-03-04T10:15:02Z", "machine_id": "mill-1",
  "tool_instance_id": "cutter-45",
  "payload": {"task": "forecast", "as_of": "2026-03-04T10:15:00Z",
    "cut_index": 40, "based_on_event_id": "cut-40",
    "result": "inspect_tool_next_stop", "score": 0.82,
    "horizon_cuts": 10, "minimum_lead_cuts": 2}
}
```

```json
{
  "schema_version": "demo-1", "event_id": "service-45", "kind": "event",
  "occurred_at": "2026-03-04T10:47:00Z", "machine_id": "mill-1",
  "tool_instance_id": "cutter-45",
  "payload": {"event_type": "maintenance", "cut_index": 45,
    "component": "cutter", "confirmed_fault": "chipped_cutter",
    "planned": false, "replacement_tool_instance_id": "cutter-46"}
}
```

`score: 0.82` is an illustrative score, **not an 82% failure probability**. A separate `tool_installed` event for `cutter-46` begins the next cutter lifecycle. An `inspection` event may confirm `none` or `unknown`; neither should be silently treated as a confirmed breakdown.

## Using the records

### 1. Failure forecasting: will this cutter be damaged soon?

1. Choose the target: **confirmed, unplanned `chipped_cutter` maintenance** on this tool between cuts `t+2` and `t+10` (illustrative lead and horizon). A `low_coolant_flow` service event is **not** this target.
2. For each completed cut `t`, use only that tool's cuts and already-known events to calculate recent vibration/current/flow trends and cuts since installation. Never use future maintenance or inspection findings as inputs.
3. Label historical examples **after** the fact: damage at cut 45 makes cut 40 positive; exclude cut 44 because a two-cut minimum warning was no longer possible. A planned replacement or unknown outcome ends observation (**censored**), not a negative example. A negative requires follow-up through the full horizon without the target event or censoring.
4. Train on multiple simulated cutter lifecycles; hold out whole physical tool instances rather than randomly splitting adjacent cuts. Compare with a simple cut-count-only warning. Report detected failures, unnecessary alerts per tool, and warning lead in cuts—not only accuracy.
5. At cut 40 the demo may emit `inspect_tool_next_stop`. It is an operational suggestion for a simulated operator, never a CNC control instruction. The subsequent service record is used only to evaluate the warning.

### 2. Fault diagnosis: what seems wrong now?

1. Establish normal ranges from known-good cuts under the **fixed** job settings. Label supervised examples only from technician-confirmed events, not from the stream's apparent symptoms.
2. Compare signatures: normal coolant flow plus unusual cutting vibration suggests `chipped_cutter`; unusually low coolant flow suggests `low_coolant_flow`. Current provides a cross-check. These are **scripted demo hypotheses**, not generally reliable thresholds or proof of causality on a real mill.
3. If both readings are abnormal, a sensor is unavailable, or neither signature fits, return `unknown`; request inspection. The technician's later `event.confirmed_fault` can confirm or overturn the guess. Track confusion by fault type.

### 3. Anomaly detection: is this cut unusual?

1. Establish a baseline from previously known-good cuts of this fixed material, tool type, and settings. No failure labels are needed to flag a departure from that baseline.
2. As an **illustrative calculation**, if baseline vibration RMS has mean `0.12 g` and standard deviation `0.03 g`, then `0.27 g` gives `z = (0.27 - 0.12) / 0.03 = 5`. Check flow/current as well and require persistence across several cuts before surfacing a warning; a `z` score is not a universal safety threshold.
3. Investigate the finding. An unusual cut may indicate a fault, a changed setup, or a faulty sensor; it is **not** itself a forecast or confirmed diagnosis.

## Standards and further reading

- [MTConnect device model](https://model.mtconnect.org/Version2.5/DeviceInformationModel/) and [observation model](https://model.mtconnect.org/Version2.5/ObservationInformationModel/): standardized manufacturing component/data-item structures and Samples, Events, and Conditions. This demo's JSON fields are **not** MTConnect fields.
- [OPC UA for Machine Tools (OPC 40501-1)](https://reference.opcfoundation.org/specs/OPC-40501-1/full): a standard machine-tool interface for monitoring, equipment, and jobs. The demo does not implement an OPC UA server.
- [ISO 13374-2](https://www.iso.org/standard/36645.html) and [ISO 13381-1](https://www.iso.org/standard/88029.html): condition-monitoring processing and prognostics guidance (full ISO texts may require purchase).
- [NIST's milling-tool signal-processing example](https://www.nist.gov/publications/generalized-method-featurization-manufacturing-signals-application-tool-condition) and a [real open milling tool-life dataset](https://www.nature.com/articles/s41597-025-04923-y): examples of how raw signals and cutting cycles relate to tool condition. Neither is represented as the source of this invented demo data.

**Scope limit:** a working simulator can demonstrate data flow, feature calculation, labeling, and alert timing. Good results on these scripted faults do **not** establish predictive performance, diagnosis accuracy, or safe maintenance decisions for a real mill. Real deployment would need varied jobs, sensor validation, trustworthy maintenance records, and field testing.
