"""Процесс Execution Controller: остановка по SIGTERM не ждёт прощания с Kafka."""

import os
import signal
import subprocess
import sys
import time

STARTUP_SECONDS = 3
STOP_SECONDS = 10


def test_sigterm_stops_controller_at_once_even_without_kafka() -> None:
    env = os.environ | {
        "E3_KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
        # Недоступный OTLP: досылка спанов не должна держать остановку дольше своего лимита.
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:2",
    }
    process = subprocess.Popen(
        [sys.executable, "-c", "from e3.execution.main import main; main()"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(STARTUP_SECONDS)

    process.send_signal(signal.SIGTERM)

    assert process.wait(timeout=STOP_SECONDS) == 128 + signal.SIGTERM
