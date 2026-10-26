"""Run Orchestrator: PipelineRun, NodeRun, NodeOutput и обмен с Execution Plane.

Публичный интерфейс — `RunOrchestrator`, `router`, фоновые шаги `background`
и Kafka-транспорт. Таблицы, outbox/inbox и правила продвижения запуска —
внутренности модуля.
"""

from n3.runs.api import router
from n3.runs.kafka import EventConsumer, ensure_topics, event_consumer, kafka_publisher
from n3.runs.orchestrator import RunOrchestrator
from n3.runs.outbox import OutboxRelay, Publisher
from n3.runs.workers import background

__all__ = [
    "EventConsumer",
    "OutboxRelay",
    "Publisher",
    "RunOrchestrator",
    "background",
    "ensure_topics",
    "event_consumer",
    "kafka_publisher",
    "router",
]
