import hashlib
from pathlib import Path
from urllib.request import urlopen


VERSION = "1.6.0"
ARTIFACT = f"jmx_prometheus_javaagent-{VERSION}.jar"
URL = f"https://github.com/prometheus/jmx_exporter/releases/download/{VERSION}/{ARTIFACT}"
SHA256 = "a95983fd96e865d2bcdf911cc500e7c82808c27ab9fd226bf96732b6c3d8c46e"
OUTPUT = Path("/opt") / ARTIFACT


with urlopen(URL, timeout=60) as response:
    artifact = response.read()
actual_sha256 = hashlib.sha256(artifact).hexdigest()
if actual_sha256 != SHA256:
    raise SystemExit(f"JMX exporter checksum mismatch: expected {SHA256}, got {actual_sha256}")
OUTPUT.write_bytes(artifact)
