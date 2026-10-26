## Agent skills

### Issue tracker

Задачи живут в GitHub Issues (`trudogolik45/n3-data-platform`), через `gh`. См. `docs/agents/issue-tracker.md`.

### Triage labels

Стандартные метки: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. См. `docs/agents/triage-labels.md`.

### Domain docs

Single-context: один `GLOSSARY.md` и `docs/adr/` в корне. См. `docs/agents/domain.md`.

## Команды

- Поднять всё локально: `docker compose up -d --build` (API на `:8000`, MinIO на `:9000`, Kafka на `:9092`, OIDC `mock-oauth2-server` на `:8080`; API в `N3_AUTH_MODE=oidc`).
- Токен локально: `curl -s localhost:8080/n3/token -d grant_type=client_credentials -d client_id=<логин> -d client_secret=x` → `access_token` (`sub` = логин, email `<логин>@n3.local`). Organization и Project — `POST /api/v1/organizations` и `/api/v1/projects` с `Bearer dev-platform-token`; пример в `README`. `N3_AUTH_MODE=dev` (один Admin dev-Project без токена) — только при `N3_ENVIRONMENT=dev`, его используют integration-тесты.
- Lint: `uv run ruff check . && uv run ruff format --check . && uv run lint-imports`
- Типы: `uv run pyright`
- Тесты: `uv run pytest` (integration требует Docker: testcontainers поднимает PostgreSQL, MinIO и `apache/kafka`; Spark заменяет Fake Execution Controller из `tests/fakes`).
- JSON Schema контрактов Backend ↔ DS и сообщений Kafka (PRD §4, §6, включая Manifest v1): `uv run n3-contracts-schema`.
- Зарегистрировать блок: `N3_API_URL=http://localhost:8000 N3_PLATFORM_TOKEN=... uv run n3-register-block spec.yaml`.
- Execution Controller (Execution Plane, вне Control Plane): `uv run n3-execution-controller` (kubeconfig из `KUBECONFIG`, настройки `N3_*` в `n3.execution.settings`; на тесном кластере `N3_SPARK_CORE_REQUEST=250m` — доля ядра на driver и executor).
- Тесты Spark-блоков: `scripts/block-tests.sh` — `tests/blocks` в образе `apache/spark` (нужен только Docker; в `uv run pytest` их нет, локально pyspark не ставится).
- E2E на настоящем Spark: `scripts/e2e.sh` — kind + локальный registry `localhost:5001` + Kubeflow Spark Operator, compose-стенд, образы `blocks/Dockerfile` (`--build-arg BLOCK=read|filter|aggregate`), DAG `Read → Filter → Aggregate → Publish Dataset`, Attempt Fence, ретрай удалённой посреди запуска попытки и отмена запуска из `tests/e2e` (каждый тест входит через `mock-oauth2-server` и создаёт свой Project) (`N3_RECONCILE_AFTER_SECONDS=15`); в конце всё разбирает (`N3_E2E_KEEP=1` — оставить). Нужны Docker, helm, uv; kind (и kubectl) качаются в `.cache/bin`. Порты как у `docker compose up`. В CI — workflow `E2E` на push в `main` и вручную.
- Официального `minio/minio` на Docker Hub больше нет, используем `cgr.dev/chainguard/minio`.
