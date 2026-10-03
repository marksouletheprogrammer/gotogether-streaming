import io
import json
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from scripts import register_event_schemas


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "src" / "main" / "resources" / "avro" / "event-record.avsc"
SCHEMA = json.loads(SCHEMA_PATH.read_text())


class EventSchemaRegistrationTests(unittest.TestCase):
    def test_event_topics_have_only_their_avro_value_subjects(self):
        self.assertEqual(
            register_event_schemas.EVENT_SUBJECTS,
            tuple(f"{topic}-value" for topic in register_event_schemas.EVENT_TOPICS),
        )
        self.assertNotIn("cnc-demo-records-value", register_event_schemas.EVENT_SUBJECTS)
        self.assertNotIn("mill-cuts-transactional-source-value", register_event_schemas.EVENT_SUBJECTS)
        payload = json.loads(register_event_schemas.build_registration_payload(json.dumps(SCHEMA)))
        self.assertEqual(payload["schemaType"], "AVRO")
        self.assertEqual(json.loads(payload["schema"]), SCHEMA)

    def test_matching_subject_is_reused_without_posting(self):
        subject = register_event_schemas.EVENT_SUBJECTS[0]
        existing = {"id": 17, "schemaType": "AVRO", "schema": json.dumps(SCHEMA)}
        with patch("scripts.register_event_schemas.urlopen", return_value=io.BytesIO(json.dumps(existing).encode())) as opener:
            self.assertEqual(register_event_schemas.register_or_verify(subject, json.dumps(SCHEMA)), (17, False))
        opener.assert_called_once()

    def test_missing_subject_is_registered_once(self):
        requests = []
        subject = register_event_schemas.EVENT_SUBJECTS[0]

        def open_request(request, timeout):
            requests.append(request)
            if isinstance(request, str):
                raise HTTPError(request, 404, "Not found", None, None)
            return io.BytesIO(b'{"id": 19}')

        with patch("scripts.register_event_schemas.urlopen", side_effect=open_request):
            self.assertEqual(register_event_schemas.register_or_verify(subject, json.dumps(SCHEMA)), (19, True))
        self.assertEqual(len(requests), 2)
        self.assertEqual(json.loads(requests[1].data)["schemaType"], "AVRO")

    def test_mismatched_subject_fails_without_posting(self):
        subject = register_event_schemas.EVENT_SUBJECTS[0]
        existing = {"id": 17, "schemaType": "AVRO", "schema": json.dumps({"type": "record", "name": "Wrong"})}
        with patch("scripts.register_event_schemas.urlopen", return_value=io.BytesIO(json.dumps(existing).encode())) as opener:
            with self.assertRaisesRegex(ValueError, "differs"):
                register_event_schemas.register_or_verify(subject, json.dumps(SCHEMA))
        opener.assert_called_once()

    def test_register_all_visits_only_both_event_subjects(self):
        with patch.object(register_event_schemas, "register_or_verify", return_value=(1, True)) as register:
            self.assertEqual(len(register_event_schemas.register_all(json.dumps(SCHEMA))), 2)
        self.assertEqual(
            [call.args[0] for call in register.call_args_list],
            list(register_event_schemas.EVENT_SUBJECTS),
        )


if __name__ == "__main__":
    unittest.main()
