import argparse
import json
from pathlib import Path

if __package__:
    from .schema_contract import parse_data_contract
else:
    from schema_contract import parse_data_contract


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "DATA.md"
SCHEMA_PATH = ROOT / "src" / "main" / "resources" / "avro" / "cut-record.avsc"
AVRO_NAMESPACE = "com.improving.gotogether.cuts"
TYPE_MAP = {
    "string": "string",
    "integer": "int",
    "number": "double",
    "boolean": "boolean",
}


def resolve_schema(property_schema, root_schema):
    if "$ref" not in property_schema:
        return property_schema
    target = property_schema["$ref"]
    if not target.startswith("#/"):
        raise ValueError(f"Unsupported JSON Schema reference: {target}")
    resolved = root_schema
    for part in target[2:].split("/"):
        resolved = resolved[part.replace("~1", "/").replace("~0", "~")]
    return resolved


def avro_type(schema):
    if "const" in schema or "enum" in schema:
        return "string"
    schema_type = schema.get("type")
    if schema_type not in TYPE_MAP:
        raise ValueError(f"No Avro mapping for JSON Schema type: {schema_type}")
    return TYPE_MAP[schema_type]


def record_fields(properties, required, schema):
    if set(properties) != set(required):
        missing = set(required) - set(properties)
        raise ValueError(f"Required fields missing from properties: {sorted(missing)}")
    return [
        {"name": name, "type": avro_type(resolve_schema(properties[name], schema))}
        for name in required
    ]


def derive_cut_schema(json_schema):
    cut_variant = next(
        (
            variant
            for variant in json_schema["oneOf"]
            if variant.get("properties", {}).get("kind", {}).get("const") == "cut"
        ),
        None,
    )
    if cut_variant is None:
        raise ValueError("Canonical schema has no cut record variant")

    properties = {**json_schema["properties"], **cut_variant["properties"]}
    payload_schema = resolve_schema(properties["payload"], json_schema)
    payload = {
        "type": "record",
        "name": "CutPayload",
        "fields": record_fields(
            payload_schema["properties"], payload_schema["required"], json_schema
        ),
    }
    fields = []
    for name in json_schema["required"]:
        if name == "payload":
            fields.append({"name": name, "type": payload})
        else:
            fields.append({"name": name, "type": avro_type(resolve_schema(properties[name], json_schema))})
    return {
        "type": "record",
        "name": "CncCutRecord",
        "namespace": AVRO_NAMESPACE,
        "fields": fields,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    canonical_schema, _ = parse_data_contract(DATA_PATH.read_text())
    derived = derive_cut_schema(canonical_schema)
    serialized = json.dumps(derived, indent=2) + "\n"

    if args.check:
        if not SCHEMA_PATH.is_file() or json.loads(SCHEMA_PATH.read_text()) != derived:
            raise SystemExit(f"Avro schema is stale; regenerate {SCHEMA_PATH}")
        return

    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(serialized)


if __name__ == "__main__":
    main()
