import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
CUT_TOPICS = (
    "mill-cuts-transactional-source",
    "mill-cuts-atleastonce-source",
    "mill-cuts-committed",
)
CUT_SUBJECTS = tuple(f"{topic}-value" for topic in CUT_TOPICS)
REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081").rstrip("/")
SCHEMA_PATH = Path(
    os.environ.get("SCHEMA_PATH", ROOT / "src" / "main" / "resources" / "avro" / "cut-record.avsc")
)


def build_registration_payload(schema_text):
    schema = json.loads(schema_text)
    if not isinstance(schema, dict) or schema.get("type") != "record":
        raise ValueError("The cut Avro schema asset must contain a record")
    return json.dumps({"schemaType": "AVRO", "schema": schema_text}).encode("utf-8")


def register_or_verify(subject, schema_text, registry_url=REGISTRY_URL):
    payload = build_registration_payload(schema_text)
    canonical_schema = json.loads(schema_text)
    subject_url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    try:
        with urlopen(subject_url, timeout=10) as response:
            current = json.load(response)
    except HTTPError as error:
        status = error.code
        error.close()
        if status != 404:
            raise
        current = None

    if current is not None:
        try:
            matches = current["schemaType"] == "AVRO" and json.loads(current["schema"]) == canonical_schema
        except (KeyError, TypeError, json.JSONDecodeError):
            matches = False
        if not matches:
            raise ValueError(f"Existing schema for {subject} differs from the canonical cut Avro schema")
        schema_id = current.get("id")
        if not isinstance(schema_id, int):
            raise RuntimeError(f"Schema Registry returned no schema id: {current}")
        return schema_id, False

    request = Request(
        f"{registry_url.rstrip('/')}/subjects/{subject}/versions",
        data=payload,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        result = json.load(response)
    if not isinstance(result.get("id"), int):
        raise RuntimeError(f"Schema Registry returned no schema id: {result}")
    return result["id"], True


def register_all(schema_text, registry_url=REGISTRY_URL):
    return tuple(
        (subject, *register_or_verify(subject, schema_text, registry_url))
        for subject in CUT_SUBJECTS
    )


def main():
    try:
        results = register_all(SCHEMA_PATH.read_text())
    except (OSError, ValueError, RuntimeError) as error:
        print(f"Cut schema registration failed: {error}", file=sys.stderr)
        return 1

    for subject, schema_id, created in results:
        action = "Registered" if created else "Verified existing"
        print(f"{action} {subject} as schema id {schema_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
