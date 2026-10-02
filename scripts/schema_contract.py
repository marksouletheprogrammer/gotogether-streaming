import json
import re


SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
JSON_BLOCK_PATTERN = re.compile(r"```json\s*(.*?)\s*```", re.DOTALL)


def parse_data_contract(markdown):
    documents = []
    for index, block in enumerate(JSON_BLOCK_PATTERN.findall(markdown), start=1):
        try:
            documents.append(json.loads(block))
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid JSON in fenced block {index}: {error}") from error

    schemas = [document for document in documents if "$schema" in document]
    if len(schemas) != 1:
        raise ValueError(f"Expected exactly one JSON Schema block; found {len(schemas)}")
    if schemas[0].get("$schema") != SCHEMA_DIALECT:
        raise ValueError(f"Expected a Draft 2020-12 schema using {SCHEMA_DIALECT}")

    return schemas[0], [document for document in documents if "$schema" not in document]
