# N3 Data Platform

[![CI](https://github.com/trudogolik45/e3-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/trudogolik45/e3-data-platform/actions/workflows/ci.yml)
[![E2E](https://github.com/trudogolik45/e3-data-platform/actions/workflows/e2e.yml/badge.svg)](https://github.com/trudogolik45/e3-data-platform/actions/workflows/e2e.yml)

Платформа обработки данных: пользователь загружает датасет, собирает из готовых блоков граф обработки (DAG), запускает его на Spark и получает новую версию датасета в Data Lake.

Архитектура рассчитана на рост до ML/AI-платформы, но ML-инфраструктуру не внедряем раньше времени.

## Как это работает

```mermaid
flowchart LR
    user([Пользователь / SDK]) -- REST --> cp

    subgraph cp [Control Plane · FastAPI]
        catalog[Dataset Catalog]
        registry[Block Registry]
        pipelines[Pipeline Service]
        runs[Run Orchestrator]
    end

    cp --- pg[(PostgreSQL<br/>состояние · outbox)]
    cp <-- команды / события --> kafka[[Kafka]]
    kafka <--> ec

    subgraph ep [Execution Plane]
        ec[Execution Controller] --> spark[Spark on Kubernetes]
    end

    spark <--> s3[(MinIO / S3<br/>Parquet)]
    catalog <--> s3
```

- **Control Plane** — модульный монолит на FastAPI. Хранит датасеты, блоки, пайплайны и запуски. PostgreSQL — источник истины.
- **Execution Plane** — развёртывается отдельно. Execution Controller запускает Spark-задания в Kubernetes и следит за ними.
- **Kafka** передаёт между плоскостями только команды и события. Сами данные идут через S3.
- Каждый результат — неизменяемая версия датасета. Повторный запуск и рестарт сервисов не дублируют и не теряют опубликованные данные.

Подробнее: [PRD.md](PRD.md), решения — в [docs/adr](docs/adr), термины — в [GLOSSARY.md](GLOSSARY.md).

## Что уже работает

- Загрузка CSV и версии датасетов.
- Регистрация блоков и пайплайнов, проверка DAG до запуска.
- Многоузловой DAG `Read → Filter → Aggregate → Publish Dataset` на настоящем Spark в Kubernetes.
- Ретраи с гарантией одной активной попытки на узел и восстановление после потери задания.
- CI на каждый PR и e2e-прогон на kind на каждый push в `main`.

Дальше по плану: отмена запусков, доступ по ролям (OIDC), лимиты, изоляция workload'ов, сквозная трассировка, затем Iceberg, Trino и Superset. План — в [Issues](https://github.com/trudogolik45/e3-data-platform/issues).

## Стек

Python 3.13, FastAPI, SQLAlchemy, PostgreSQL, Kafka (KRaft), MinIO / S3, Apache Spark, Kubeflow Spark Operator, Kubernetes, uv, ruff, pyright, pytest, testcontainers.

## Быстрый старт

Нужны Docker и [uv](https://docs.astral.sh/uv/).

```sh
docker compose up -d --build   # API на :8000, MinIO на :9000, Kafka на :9092
```

Документация API — http://localhost:8000/docs.

Вход — через локальный OIDC-провайдер `mock-oauth2-server` на :8080: он выдаёт токен любому
`client_id`. Organization и Project создаёт команда платформы токеном `N3_PLATFORM_TOKEN`
(локально `dev-platform-token`), первым Admin становится уже входивший пользователь:

```sh
TOKEN=$(curl -s localhost:8080/n3/token -d grant_type=client_credentials -d client_id=alice \
  -d client_secret=x | jq -r .access_token)
curl -s localhost:8000/api/v1/me -H "Authorization: Bearer $TOKEN"     # первый вход создаёт User
ORG=$(curl -s localhost:8000/api/v1/organizations -H "Authorization: Bearer dev-platform-token" \
  -H 'Content-Type: application/json' -d '{"name":"demo"}' | jq -r .organization_id)
curl -s localhost:8000/api/v1/projects -H "Authorization: Bearer dev-platform-token" \
  -H 'Content-Type: application/json' \
  -d "{\"organization_id\":\"$ORG\",\"name\":\"demo\",\"admin\":{\"email\":\"alice@n3.local\"}}"
```

## Проверки

```sh
uv run ruff check . && uv run ruff format --check . && uv run lint-imports   # линтер
uv run pyright                                                               # типы
uv run pytest                                                                # unit и integration (нужен Docker)
scripts/block-tests.sh                                                       # Spark-блоки в образе apache/spark
scripts/e2e.sh                                                               # e2e на Spark: kind + Spark Operator
```

## Лицензия

© 2026 trudogolik45. Все права защищены. Код открыт только для просмотра — см. [LICENSE](LICENSE).
