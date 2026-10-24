# E3 Data Platform — Target Architecture v1.0

Greenfield · MVP Architecture

Цель первой версии E3: пользователь загружает датасет, собирает DAG из вычислительных блоков, запускает распределённую обработку в Spark, получает версионированный результат в Data Lake и исследует его через Superset.

Архитектуру проектируем с возможностью последующего расширения до ML/AI Platform, но без преждевременного внедрения ML-инфраструктуры.

## 1. Архитектурная схема

E3 Web UI / SDK

Datasets · Pipeline Editor · Executions · Analytics

CONTROL PLANE

FastAPI · Modular Monolith

API · Auth · RBAC

Dataset Catalog

Metadata · Versions

Block Registry

Contracts · Versions

Pipeline Service

DAG · Validation

Run Orchestrator

Scheduling · State

PostgreSQL

Transactional state · Outbox · Metadata

Kafka: commands / events

EXECUTION PLANE

Independent deployment

Execution Controller

Submission · Monitoring · Reconciliation

Spark on Kubernetes

Distributed transforms

Python Runner

Lightweight jobs

DATA PLANE

MinIO / S3

Parquet / Iceberg

Dataset Manifest

Trino SQL Engine

Apache Superset

SQL exploration · Dashboards

Основные правила взаимодействия:

- REST — синхронные пользовательские операции.
- Kafka — асинхронные команды и события выполнения.
- PostgreSQL — источник истины для метаданных и состояний.
- S3 — передача больших данных между вычислительными компонентами.
- Spark — вычислительный движок, не хранилище и не оркестратор.
- Trino — SQL-доступ к таблицам для Superset.

## 2. Границы модулей Control Plane

| Модуль                | Владеет                                       | Публичные операции                        |
| --------------------- | --------------------------------------------- | ----------------------------------------- |
| Identity & Access     | Организации, проекты, роли, политики доступа  | `authorize`, `grant_access`               |
| Dataset Catalog       | Dataset, версии, схемы, публикация            | `register`, `resolve`, `publish`          |
| Block Registry        | Спецификации блоков, версии исполняемого кода | `register`, `get_spec`                    |
| Pipeline Service      | DAG, ревизии, валидация графа                 | `create`, `validate`, `publish`           |
| Run Orchestrator      | PipelineRun, NodeRun, зависимости, retries    | `start`, `cancel`, `get_status`           |
| Analytics Integration | Связь датасетов с Trino/Superset              | `expose_dataset`, `create_dashboard_link` |

Правило владения: только модуль-владелец может изменять свои доменные сущности. Остальные обращаются к его публичному интерфейсу, даже если все модули работают в одном процессе FastAPI.

Для MVP у них общая PostgreSQL-инстанция, но отдельные схемы или явно изолированные таблицы. Прямые межмодульные записи запрещены.

## 3. Доменная модель

Упрощённая логическая модель. Связи отражают зависимости и принадлежность, а не точные внешние ключи.

Ключевые сущности:

| Сущность           | Назначение                                        |
| ------------------ | ------------------------------------------------- |
| `Dataset`          | Логическая коллекция данных                       |
| `DatasetVersion`   | Неизменяемая опубликованная версия данных         |
| `BlockSpec`        | Версионированный контракт вычислительного блока   |
| `PipelineVersion`  | Неизменяемая версия DAG                           |
| `PipelineRun`      | Один запуск конкретной версии DAG                 |
| `NodeRun`          | Логическое выполнение узла внутри PipelineRun     |
| `ExecutionAttempt` | Одна физическая попытка запуска                   |
| `Artifact`         | Результат вычисления, manifest, лог или иной файл |

У каждого `PipelineRun` есть свой ID. У каждого `NodeRun` — свой `execution_id`, сохраняющийся при повторных попытках. У каждой попытки — отдельный `attempt_id`.

## 4. Контракты Backend ↔ DS

Контракты определяем на Pydantic v2, публикуем как JSON Schema. Это позволит валидировать конфигурацию DAG до запуска.

### `DatasetRef`

```
from pydantic import BaseModelfrom uuid import UUIDclass DatasetRef(BaseModel):    dataset_id: UUID    version_id: UUID
```

Здесь нет S3 URL. Это принципиально: DS-блок не должен зависеть от физического расположения датасета.

Перед запуском Runtime разрешает `DatasetRef` в конкретный storage location с учётом прав доступа.

### `BlockSpec`

```
from typing import Literalfrom pydantic import BaseModelclass PortSpec(BaseModel):    name: str    data_type: Literal["dataset", "scalar", "artifact"]    required: bool = Trueclass BlockSpec(BaseModel):    name: str    version: str    runtime: Literal["spark", "python"]    inputs: list[PortSpec]    outputs: list[PortSpec]    parameters_schema: dict    executable_ref: str
```

`executable_ref` должен указывать на неизменяемую версию кода, например OCI image digest, а не на плавающий тег `latest`.

Для production добавим ресурсные требования, совместимость схем, ограничения параллелизма и политику обработки ошибок.

### `ExecutionContext` и `ExecutionResult`

```
from typing import Anyfrom uuid import UUIDfrom pydantic import BaseModelclass ExecutionContext(BaseModel):    execution_id: UUID    attempt_id: UUID    inputs: dict[str, DatasetRef]    parameters: dict[str, Any]    output_prefix: strclass ExecutionResult(BaseModel):    execution_id: UUID    attempt_id: UUID    outputs: dict[str, str]    metrics: dict[str, float] = {}
```

`outputs` здесь содержит ссылки на подготовленные manifests, а не автоматически опубликованные `DatasetVersion`.

Это разделяет два события:

1. Вычисление завершилось.
2. Результат проверен и опубликован в каталоге.

Только после второго события downstream-узлы получают опубликованные входы.

## 5. REST API v1

| Метод  | Endpoint                                             | Назначение                     |
| ------ | ---------------------------------------------------- | ------------------------------ |
| `POST` | `/api/v1/datasets`                                   | Создать датасет                |
| `POST` | `/api/v1/datasets/{id}/uploads`                      | Начать загрузку                |
| `POST` | `/api/v1/datasets/{id}/uploads/{upload_id}/complete` | Завершить и проверить загрузку |
| `GET`  | `/api/v1/datasets/{id}/versions`                     | Получить версии                |
| `GET`  | `/api/v1/blocks`                                     | Список доступных блоков        |
| `POST` | `/api/v1/pipelines`                                  | Создать pipeline               |
| `POST` | `/api/v1/pipelines/{id}/versions`                    | Опубликовать версию DAG        |
| `POST` | `/api/v1/pipeline-runs`                              | Запустить DAG                  |
| `GET`  | `/api/v1/pipeline-runs/{id}`                         | Статус выполнения              |
| `GET`  | `/api/v1/pipeline-runs/{id}/nodes`                   | Состояния узлов                |
| `POST` | `/api/v1/pipeline-runs/{id}/cancel`                  | Запросить отмену               |

Запуск DAG:

```

POST /api/v1/pipeline-runs
Idempotency-Key: run-request-001

{
  "pipeline_id": "7e0c...",
  "pipeline_version": 3,
  "inputs": {
    "source": {
      "dataset_id": "a91b...",
      "version_id": "f42d..."
    }
  }
}

```

Ответ `202 Accepted` возвращает `pipeline_run_id`, ссылку на ресурс статуса и начальное состояние `PENDING`.

Для загрузки файлов API выдаёт presigned S3 URL. Содержимое файла не проходит через FastAPI.

## 6. Kafka: команды и события

Kafka используется только там, где требуется асинхронная граница между Control Plane и Execution Plane.

| Topic                      | Направление               | Содержание                                    |
| -------------------------- | ------------------------- | --------------------------------------------- |
| `e3.execution.commands.v1` | Orchestrator → Controller | `SubmitExecution`, `CancelExecution`          |
| `e3.execution.events.v1`   | Controller → Orchestrator | `Started`, `Succeeded`, `Failed`, `Cancelled` |

Каждое сообщение имеет envelope:

```

{
  "event_id": "uuid",
  "schema_version": 1,
  "execution_id": "uuid",
  "attempt_id": "uuid",
  "trace_id": "trace-context",
  "occurred_at": "2026-10-24T12:00:00Z",
  "type": "ExecutionSucceeded",
  "payload": {
    "result_manifest": "s3://e3-artifacts/..."
  }
}

```

Гарантии:

- Kafka доставляет сообщения как минимум один раз.
- Повторная команда с тем же `attempt_id` не создаёт второй workload.
- Orchestrator применяет события только к соответствующей попытке и допустимому переходу состояния.
- Transactional outbox исключает разрыв между записью состояния в PostgreSQL и публикацией команды.
- Inbox/deduplication защищает от повторной обработки.
- Reconciliation восстанавливает состояние после сбоев и пропущенных событий.

Не закладываем Kafka RPC между доменными модулями. Внутри модульного монолита они взаимодействуют через обычные Python-интерфейсы.

## 7. Жизненный цикл вычислений

PipelineRun / NodeRun state machine

Отмена — запрос, а не мгновенный terminal state. При получении `cancel` система переводит операцию в `CANCELLING`, останавливает workload и только после подтверждения фиксирует `CANCELLED`.

Если Spark успешно завершил вычисление, но публикация результата не состоялась, операция не должна ошибочно переходить в `SUCCEEDED`. Для этого вводим промежуточный этап `FINALIZING`.

## 8. Data Lake и публикация результатов

Рекомендуемая структура объектного хранилища:

```

s3://e3/
  staging/
    uploads/{upload_id}/
    executions/{attempt_id}/
  datasets/
    {dataset_id}/
      {version_id}/
        data/
        manifest.json
  artifacts/
    {execution_id}/{attempt_id}/
  system/
    checkpoints/

```

Публикация версии:

1. Runtime пишет данные в `staging`.
2. Проверяет целостность и формирует manifest.
3. Dataset Catalog валидирует схему и доступность объектов.
4. Результат переносится или копируется в неизменяемое расположение либо регистрируется как immutable snapshot.
5. PostgreSQL-транзакция фиксирует опубликованную версию.
6. Orchestrator разрешает downstream-узлы.

Для Iceberg вместо копирования файлов нужно использовать транзакционный commit таблицы через соответствующий catalog. Поэтому Parquet datasets и Iceberg tables должны иметь разные стратегии публикации, хотя могут использовать общий логический интерфейс Dataset Catalog.

## 9. Безопасность и изоляция

Для self-hosted E3 это обязательная часть архитектуры, а не последующая доработка.

- OIDC для аутентификации, проектные роли и проверки доступа на каждом API.
- Изолированные workload-процессы/контейнеры для пользовательского DS-кода.
- Краткоживущие credentials и ограниченные S3 permissions.
- Запрет доступа вычислительных задач к PostgreSQL Control Plane.
- Resource quotas для CPU, RAM и параллельных запусков.
- Audit log для доступа к датасетам и запуска вычислений.
- Отдельная политика доступа к Trino/Superset, чтобы аналитический слой не обходил права E3.

## 10. Что реально войдёт в MVP

Обязательно

FastAPI Control Plane, PostgreSQL, MinIO, Block Registry, Dataset Catalog, DAG validation, Run Orchestrator, Execution Controller, Spark, Kafka, end-to-end tracing.

Следующая итерация

Iceberg, Trino, Superset, dataset lineage, продвинутые политики доступа, расписания запусков.

Будущее расширение

ML model registry, feature pipelines, vLLM, inference endpoints, AI agents.

## 11. Первое архитектурное решение (ADR-001)

Решение: E3 строится как модульный Control Plane на FastAPI с отдельным Execution Plane, Kafka как транспортом команд/событий, PostgreSQL как источником истины и S3 как основным каналом обмена вычислительными данными.

Причина: такое разделение минимизирует распределённую связанность, даёт единое управление жизненным циклом и позволяет независимо масштабировать вычисления.

Первый acceptance test: загрузить CSV → создать DAG `Read → Filter → Aggregate` → выполнить на Spark → сохранить Parquet → зарегистрировать новую версию датасета → получить статус и результат через API. Повторный запуск и перезапуск сервисов не должны приводить к дублированию опубликованных результатов или потере состояния.
