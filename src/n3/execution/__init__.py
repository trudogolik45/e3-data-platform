"""Execution Controller — Execution Plane (ADR-0001).

Отдельный процесс `n3-execution-controller`: команды из Kafka превращает в
`SparkApplication`, их состояния — в события. Импортирует только `n3.shared`,
к PostgreSQL Control Plane доступа не имеет (закреплено `lint-imports`).
"""
