import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SUBJECT = "cnc-demo-records-value"
REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
SCHEMA_PATH = Path(os.environ.get("SCHEMA_PATH", ROOT / "schemas" / "cnc-demo-records.schema.json"))


def build_registration_payload(schema_text):
    schema = json.loads(schema_text)
    if not isinstance(schema, dict):
        raise ValueError("The schema asset must contain a JSON object")
    return json.dumps({"schemaType": "JSON", "schema": schema_text}).encode("utf-8")


def register_or_verify(schema_text):
    payload = build_registration_payload(schema_text)
    canonical_schema = json.loads(schema_text)
    subject_url = f"{REGISTRY_URL}/subjects/{SUBJECT}/versions/latest"
    try:
        with urlopen(subject_url, timeout=10) as response:
            current = json.load(response)
    except HTTPError as error:
        status = error.code
        error.close()
        if status != 404:
            raise
        current = None
    else:
        if not isinstance(current, dict):
            raise ValueError(f"Schema Registry returned an unexpected response for {SUBJECT}: {current}")

    if current is not None:
        try:
            matches = current["schemaType"] == "JSON" and json.loads(current["schema"]) == canonical_schema
        except (KeyError, TypeError, json.JSONDecodeError):
            matches = False
        if not matches:
            raise ValueError(f"Existing schema for {SUBJECT} differs from the canonical asset; refusing to replace it")
        schema_id = current.get("id")
        if not isinstance(schema_id, int):
            raise RuntimeError(f"Schema Registry returned no schema id: {current}")
        return schema_id, False

    request = Request(
        f"{REGISTRY_URL}/subjects/{SUBJECT}/versions",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        result = json.load(response)
    if not isinstance(result.get("id"), int):
        raise RuntimeError(f"Schema Registry returned no schema id: {result}")
    return result["id"], True


def main():
    try:
        schema_id, created = register_or_verify(SCHEMA_PATH.read_text())
    except (OSError, ValueError, RuntimeError) as error:
        print(f"Schema registration failed for {SUBJECT}: {error}", file=sys.stderr)
        return 1

    action = "Registered" if created else "Verified existing"
    print(f"{action} {SUBJECT} as schema id {schema_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
