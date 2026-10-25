## Agent skills

### Issue tracker

Задачи живут в GitHub Issues (`trudogolik45/e3-data-platform`), через `gh`. См. `docs/agents/issue-tracker.md`.

### Triage labels

Стандартные метки: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. См. `docs/agents/triage-labels.md`.

### Domain docs

Single-context: один `GLOSSARY.md` и `docs/adr/` в корне. См. `docs/agents/domain.md`.

## Команды

- Поднять всё локально: `docker compose up -d --build` (API на `:8000`, MinIO на `:9000`, Kafka на `:9092`).
- Lint: `uv run ruff check . && uv run ruff format --check . && uv run lint-imports`
- Типы: `uv run pyright`
- Тесты: `uv run pytest` (integration требует Docker: testcontainers поднимает PostgreSQL, MinIO и `apache/kafka`; Spark заменяет Fake Execution Controller из `tests/fakes`).
- JSON Schema контрактов Backend ↔ DS и сообщений Kafka (PRD §4, §6, включая Manifest v1): `uv run e3-contracts-schema`.
- Зарегистрировать блок: `E3_API_URL=http://localhost:8000 E3_PLATFORM_TOKEN=... uv run e3-register-block spec.yaml`.
- Execution Controller (Execution Plane, вне Control Plane): `uv run e3-execution-controller` (kubeconfig из `KUBECONFIG`, настройки `E3_*` в `e3.execution.settings`; на тесном кластере `E3_SPARK_CORE_REQUEST=250m` — доля ядра на driver и executor).
- Тесты Spark-блоков: `scripts/block-tests.sh` — `tests/blocks` в образе `apache/spark` (нужен только Docker; в `uv run pytest` их нет, локально pyspark не ставится).
- E2E на настоящем Spark: `scripts/e2e.sh` — kind + локальный registry `localhost:5001` + Kubeflow Spark Operator, compose-стенд, образы `blocks/Dockerfile` (`--build-arg BLOCK=read|filter|aggregate`), DAG `Read → Filter → Aggregate → Publish Dataset`, Attempt Fence и ретрай удалённой посреди запуска попытки из `tests/e2e` (`E3_RECONCILE_AFTER_SECONDS=15`); в конце всё разбирает (`E3_E2E_KEEP=1` — оставить). Нужны Docker, helm, uv; kind (и kubectl) качаются в `.cache/bin`. Порты как у `docker compose up`. В CI — workflow `E2E` на push в `main` и вручную.
- Официального `minio/minio` на Docker Hub больше нет, используем `cgr.dev/chainguard/minio`.
