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

Lightweight jobs · следующая итерация

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
| Pipeline Service      | PipelineDraft, PipelineVersion, валидация DAG | `create`, `validate`, `publish`           |
| Run Orchestrator      | PipelineRun, NodeRun, NodeOutput, retries     | `start`, `cancel`, `get_status`           |
| Analytics Integration | Связь датасетов с Trino/Superset              | `expose_dataset`, `create_dashboard_link` |

Правило владения: только модуль-владелец может изменять свои доменные сущности. Остальные обращаются к его публичному интерфейсу, даже если все модули работают в одном процессе FastAPI.

Для MVP у них общая PostgreSQL-инстанция, но отдельные схемы или явно изолированные таблицы. Прямые межмодульные записи запрещены.

## 3. Доменная модель

Упрощённая логическая модель. Связи отражают зависимости и принадлежность, а не точные внешние ключи.

Ключевые сущности:

| Сущность           | Назначение                                                        |
| ------------------ | ----------------------------------------------------------------- |
| `Project`          | Владелец датасетов, пайплайнов и запусков; единица RBAC           |
| `Dataset`          | Логическая коллекция данных; можно архивировать, нельзя удалить   |
| `DatasetVersion`   | Неизменяемая версия данных: UUID, `version_number`, схема, формат |
| `BlockSpec`        | Неизменяемая версия контракта вычислительного блока               |
| `PipelineDraft`    | Единственная изменяемая заготовка графа                           |
| `PipelineVersion`  | Неизменяемая версия DAG со слотами входов и выходов               |
| `PipelineRun`      | Один запуск PipelineVersion с зафиксированными привязками слотов  |
| `NodeRun`          | Логическое выполнение узла внутри PipelineRun                     |
| `ExecutionAttempt` | Одна физическая попытка выполнить BLOCK-узел                      |
| `NodeOutput`       | Принятый логический выход NodeRun (READY)                         |
| `Artifact`         | Физический объект хранения: файл данных, manifest, лог            |

У каждого `PipelineRun` есть свой ID. У каждого `NodeRun` — свой `node_run_id`, сохраняющийся при повторных попытках. У каждой попытки — отдельный `attempt_id`. Имя `node_run_id` единое во всех контрактах E3, включая Kafka.

Правила модели:

- Узлы DAG бывают двух видов: `BLOCK` (ссылается на точную версию `BlockSpec`) и `SYSTEM` (встроен в платформу, без Block Registry и без `ExecutionAttempt`). В MVP единственный системный узел — `Publish Dataset`.
- **Late Binding**: `PipelineVersion` знает только Input Slots и Output Slots, возможно со значениями по умолчанию. Значение по умолчанию для входа — точная `DatasetVersion` или «последняя версия Dataset».
- При создании `PipelineRun` все привязки разрешаются в точные значения и больше не меняются, даже если запуск ждёт в очереди.
- Схема живёт в `DatasetVersion`. Dataset схему не навязывает.
- Формат `DatasetVersion`: `CSV` (результат Upload) или `PARQUET` (результат публикации).
- Один `Dataset` может быть и входом, и выходом одного запуска.
- Архивный Dataset нельзя использовать в новых запусках и публикациях. Уже созданные запуски продолжают работать.

## 4. Контракты Backend ↔ DS

Контракты определяем на Pydantic v2, публикуем как JSON Schema. Это позволит валидировать конфигурацию DAG до запуска.

### Ссылки на данные и `ResolvedInput`

```python
from typing import Annotated, Literal
from pydantic import BaseModel, Field
from uuid import UUID


class DatasetVersionRef(BaseModel):
    kind: Literal["dataset_version"] = "dataset_version"
    dataset_id: UUID
    version_id: UUID


class NodeOutputRef(BaseModel):
    kind: Literal["node_output"] = "node_output"
    node_run_id: UUID
    port: str


DataRef = Annotated[DatasetVersionRef | NodeOutputRef, Field(discriminator="kind")]


class ResolvedInput(BaseModel):
    manifest_uri: str
    format: Literal["CSV", "PARQUET"]
    schema: dict
    source: DataRef
```

Логические ссылки (`DataRef`) в физические `manifest_uri` разрешает только Run Orchestrator, перед `SubmitExecution` и с учётом прав доступа. У Execution Plane нет доступа к PostgreSQL, поэтому в каталог он не ходит.

Блок получает одинаковый `ResolvedInput` для любого источника и не знает, откуда пришли данные. Поле `source` нужно только для логов и lineage. Идентичность `NodeOutput` — пара `(node_run_id, port)`. `attempt_id` в неё не входит: у `NodeRun` не более одного принятого результата.

### `Manifest`

У `NodeOutput` и `DatasetVersion` один формат manifest v1: список файлов, формат, схема, число строк и контрольные суммы. Публикация переносит manifest как есть, ничего не пересчитывая.

### `BlockSpec`

```python
from typing import Literal
from pydantic import BaseModel


class PortSpec(BaseModel):
    name: str
    data_type: Literal["dataset", "scalar", "artifact"]
    required: bool = True


class BlockSpec(BaseModel):
    name: str
    version: str
    runtime: Literal["spark"]
    inputs: list[PortSpec]
    outputs: list[PortSpec]
    parameters_schema: dict
    executable_ref: str
```

В MVP единственный runtime — Spark; Python Runner — следующая итерация. Блоки регистрирует только команда платформы.

`executable_ref` должен указывать на неизменяемую версию кода, например OCI image digest, а не на плавающий тег `latest`.

Для production добавим ресурсные требования, совместимость схем, ограничения параллелизма и политику обработки ошибок, включая retry на уровне блока.

### `ExecutionContext` и `ExecutionResult`

```python
from typing import Any
from uuid import UUID
from pydantic import BaseModel


class ExecutionContext(BaseModel):
    node_run_id: UUID
    attempt_id: UUID
    inputs: dict[str, ResolvedInput]
    parameters: dict[str, Any]
    output_prefix: str


class ExecutionResult(BaseModel):
    node_run_id: UUID
    attempt_id: UUID
    outputs: dict[str, str]
    metrics: dict[str, float] = {}
```

Вход BLOCK-узла — это либо `DatasetVersion` из Input Slot, либо принятый `NodeOutput` upstream-узла. В обоих случаях он приходит как `ResolvedInput`. `outputs` — это `port → manifest_uri` подготовленных manifests, а не `DatasetVersion`.

Это разделяет три события:

1. Вычисление попытки завершилось.
2. Orchestrator проверил manifest и принял `NodeOutput` (READY). Только такой выход потребляют downstream-узлы.
3. Если выход подключён к `Publish Dataset`, Dataset Catalog публикует его как новую `DatasetVersion`.

## 5. REST API v1

| Метод  | Endpoint                                             | Назначение                     |
| ------ | ---------------------------------------------------- | ------------------------------ |
| `POST` | `/api/v1/datasets`                                   | Создать датасет                |
| `POST` | `/api/v1/datasets/{id}/uploads`                      | Начать загрузку                |
| `POST` | `/api/v1/datasets/{id}/uploads/{upload_id}/complete` | Завершить и проверить загрузку |
| `GET`  | `/api/v1/datasets/{id}/versions`                     | Получить версии                |
| `GET`  | `/api/v1/blocks`                                     | Список доступных блоков        |
| `POST` | `/api/v1/pipelines`                                  | Создать pipeline               |
| `PUT`  | `/api/v1/pipelines/{id}/draft`                       | Сохранить PipelineDraft        |
| `POST` | `/api/v1/pipelines/{id}/versions`                    | Опубликовать Draft как версию  |
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
  },
  "outputs": {
    "result": {
      "dataset_id": "c03e..."
    }
  }
}

```

Слоты, не переданные в запросе, берутся из значений по умолчанию `PipelineVersion`. Слот без значения — ошибка `422`.

Ответ `202 Accepted` возвращает `pipeline_run_id`, ссылку на ресурс статуса, начальное состояние `PENDING` и итоговые точные привязки всех слотов.

При создании запуска синхронно проверяются права: чтение входов и запись в выходы (`403`), а также что датасеты не в архиве. Повторная проверка прав на запись делается перед каждой публикацией.

У проекта не более 5 активных `PipelineRun`. Остальные ждут в `PENDING` в FIFO-очереди.

Загрузка в MVP: один CSV-файл с заголовком на один Upload. Загрузка не запускает вычислений: после `complete` файл проверяется (целостность, схема по заголовку) и становится `DatasetVersion` формата `CSV`.

Физического удаления нет: Dataset и Pipeline только архивируются.

Роли проекта: `Viewer`, `Editor`, `Admin`. ACL на отдельные датасеты — следующая итерация.

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
  "node_run_id": "uuid",
  "attempt_id": "uuid",
  "trace_id": "trace-context",
  "occurred_at": "2026-10-24T12:00:00Z",
  "type": "ExecutionSucceeded",
  "payload": {
    "result_manifest": "s3://e3-lake/artifacts/..."
  }
}

```

Гарантии:

- Kafka доставляет сообщения как минимум один раз.
- Повторная команда с тем же `attempt_id` не создаёт второй workload.
- Orchestrator применяет события только к Current Attempt и допустимому переходу состояния. События остальных попыток игнорируются.
- Retry решает только Orchestrator: 3 ретрая, максимум 4 попытки на NodeRun. Controller сам не перезапускает.
- У NodeRun не более одной активной попытки. Новая стартует только после того, как старая доказуемо мертва: `SparkApplication` терминален или удалён, нет pod'ов с её `attempt_id`, Kubernetes не может её восстановить. Иначе reconciliation по таймауту помечает попытку `LOST` после проверки этих условий. См. ADR-0003.
- Transactional outbox исключает разрыв между записью состояния в PostgreSQL и публикацией команды.
- Inbox/deduplication защищает от повторной обработки.
- Reconciliation восстанавливает состояние после сбоев и пропущенных событий.

Не закладываем Kafka RPC между доменными модулями. Внутри модульного монолита они взаимодействуют через обычные Python-интерфейсы.

## 7. Жизненный цикл вычислений

`NodeRun`: `PENDING → QUEUED → RUNNING → FINALIZING → SUCCEEDED`, а также `FAILED`, `CANCELLING → CANCELLED`, `SKIPPED` (не запускался, потому что upstream упал).

`PipelineRun`: `PENDING → RUNNING → SUCCEEDED | FAILED`, а также `CANCELLING → CANCELLED`. Своего `FINALIZING` у `PipelineRun` нет.

- `FINALIZING` принадлежит `NodeRun`: Spark закончил, Orchestrator проверяет manifest и принимает `NodeOutput`. Состояние `READY` принадлежит `NodeOutput`.
- `PipelineRun` завершается только тогда, когда все узлы в терминальных состояниях.
- Fail-fast: когда у узла кончились retry, `PipelineRun` уходит в `FAILED`. Работающие узлы отменяются, остальные становятся `SKIPPED`.
- Отмена — запрос, а не мгновенный terminal state. При `cancel` операция переходит в `CANCELLING`, workload останавливается, и только после подтверждения фиксируется `CANCELLED`.
- Начатую публикацию (`Publish Dataset`) отменить нельзя. Она доходит до конца, потом запуск завершается.
- Уже опубликованные `DatasetVersion` не откатываются при `FAILED` или `CANCELLED` запуска.

## 8. Data Lake и публикация результатов

Рекомендуемая структура объектного хранилища:

```

s3://e3-lake/
  staging/
    uploads/{upload_id}/
    executions/{attempt_id}/
  datasets/
    {dataset_id}/
      {version_id}/
        data/
        manifest.json
  artifacts/
    {node_run_id}/{attempt_id}/
  system/
    checkpoints/

```

Публикуются только выходы, подключённые к системному конечному узлу `Publish Dataset`. Публикация идёт сразу, как только upstream-узел готов, не дожидаясь всего DAG (см. ADR-0002). Она всегда направлена в существующий неархивный Dataset того же проекта.

Публикация версии:

1. Runtime пишет данные в `staging/executions/{attempt_id}/`, проверяет целостность и формирует manifest.
2. Orchestrator принимает `NodeOutput` (READY) от Current Attempt.
3. Orchestrator запускает `NodeRun` узла `Publish Dataset`. Он выполняется в Control Plane, без Spark и Execution Controller.
4. Dataset Catalog повторно проверяет права на запись, валидирует схему и доступность объектов.
5. Результат переносится или копируется в неизменяемое расположение либо регистрируется как immutable snapshot.
6. PostgreSQL-транзакция фиксирует `DatasetVersion` и выдаёт следующий `version_number` внутри Dataset. Параллельные публикации в один Dataset не конфликтуют, а нумеруются по порядку фиксации.

Неопубликованные данные из `staging` хранятся 7 суток после завершения `PipelineRun`, диагностические Artifact'ы — 30 суток.

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

## 11. Архитектурные решения и acceptance test

Решения записаны в `docs/adr/`: 0001 — Control Plane и Execution Plane, 0002 — публикация через `Publish Dataset`, 0003 — одна активная попытка на NodeRun. Термины — в `GLOSSARY.md`.

Первый acceptance test: загрузить CSV → создать DAG `Read → Filter → Aggregate → Publish Dataset` → выполнить на Spark → сохранить Parquet → зарегистрировать новую версию датасета → получить статус и результат через API. Повторный запуск и перезапуск сервисов не должны приводить к дублированию опубликованных результатов или потере состояния.
