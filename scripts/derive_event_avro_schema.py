import argparse
import json
from pathlib import Path

if __package__:
    from .derive_cut_avro_schema import avro_type, resolve_schema
    from .schema_contract import parse_data_contract
else:
    from derive_cut_avro_schema import avro_type, resolve_schema
    from schema_contract import parse_data_contract


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "DATA.md"
SCHEMA_PATH = ROOT / "src" / "main" / "resources" / "avro" / "event-record.avsc"
AVRO_NAMESPACE = "com.improving.gotogether.events"


def derive_event_schema(json_schema):
    event_variant = next(
        (
            variant
            for variant in json_schema["oneOf"]
            if variant.get("properties", {}).get("kind", {}).get("const") == "event"
        ),
        None,
    )
    if event_variant is None:
        raise ValueError("Canonical schema has no event record variant")

    properties = {**json_schema["properties"], **event_variant["properties"]}
    payload_schema = resolve_schema(properties["payload"], json_schema)
    required_payload = set(payload_schema["required"])
    payload_fields = []
    for name, field_schema in payload_schema["properties"].items():
        field_type = avro_type(resolve_schema(field_schema, json_schema))
        field = {"name": name, "type": field_type}
        if name not in required_payload:
            field["type"] = ["null", field_type]
            field["default"] = None
        payload_fields.append(field)

    payload = {"type": "record", "name": "EventPayload", "fields": payload_fields}
    fields = []
    for name in json_schema["required"]:
        if name == "payload":
            fields.append({"name": name, "type": payload})
        else:
            fields.append({"name": name, "type": avro_type(resolve_schema(properties[name], json_schema))})
    return {
        "type": "record",
        "name": "CncEventRecord",
        "namespace": AVRO_NAMESPACE,
        "fields": fields,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    canonical_schema, _ = parse_data_contract(DATA_PATH.read_text())
    derived = derive_event_schema(canonical_schema)
    serialized = json.dumps(derived, indent=2) + "\n"

    if args.check:
        if not SCHEMA_PATH.is_file() or json.loads(SCHEMA_PATH.read_text()) != derived:
            raise SystemExit(f"Avro schema is stale; regenerate {SCHEMA_PATH}")
        return

    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(serialized)


if __name__ == "__main__":
    main()
