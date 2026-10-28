## Agent skills

### Issue tracker

Задачи живут в GitHub Issues (`trudogolik45/n3-data-platform`), через `gh`. См. `docs/agents/issue-tracker.md`.

### Triage labels

Стандартные метки: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. См. `docs/agents/triage-labels.md`.

### Domain docs

Single-context: один `GLOSSARY.md` и `docs/adr/` в корне. См. `docs/agents/domain.md`.

## Команды

- Поднять всё локально: `docker compose up -d --build` (API на `:8000`, MinIO на `:9000`, Kafka на `:9092`, OIDC `mock-oauth2-server` на `:8080`; API в `E3_AUTH_MODE=oidc`).
- Токен локально: `curl -s localhost:8080/e3/token -d grant_type=client_credentials -d client_id=<логин> -d client_secret=x` → `access_token` (`sub` = логин, email `<логин>@e3.local`). Organization и Project — `POST /api/v1/organizations` и `/api/v1/projects` с `Bearer dev-platform-token`; пример в `README`. `E3_AUTH_MODE=dev` (один Admin dev-Project без токена) — только при `E3_ENVIRONMENT=dev`, его используют integration-тесты.
- Lint: `uv run ruff check . && uv run ruff format --check . && uv run lint-imports`
- Типы: `uv run pyright`
- Тесты: `uv run pytest` (integration требует Docker: testcontainers поднимает PostgreSQL, MinIO и `apache/kafka`; Spark заменяет Fake Execution Controller из `tests/fakes`).
- JSON Schema контрактов Backend ↔ DS и сообщений Kafka (PRD §4, §6, включая Manifest v1): `uv run e3-contracts-schema`.
- Зарегистрировать блок: `E3_API_URL=http://localhost:8000 E3_PLATFORM_TOKEN=... uv run e3-register-block spec.yaml`.
- Execution Controller (Execution Plane, вне Control Plane): `uv run e3-execution-controller` (kubeconfig из `KUBECONFIG`, настройки `E3_*` в `e3.execution.settings`). Размер pod'а попытки задаёт Control Plane и шлёт в `SubmitExecution`: `E3_SPARK_CORE_REQUEST` (на тесном кластере `250m` — доля ядра на driver и executor) и `E3_SPARK_MEMORY_MIB`; от него же считается Project Quota (`E3_PROJECT_QUOTA_*`, ADR-0005).
- Тесты Spark-блоков: `scripts/block-tests.sh` — `tests/blocks` в образе `apache/spark` (нужен только Docker; в `uv run pytest` их нет, локально pyspark не ставится).
- E2E на настоящем Spark: `scripts/e2e.sh` — kind + локальный registry `localhost:5001` + Kubeflow Spark Operator, compose-стенд, образы `blocks/Dockerfile` (`--build-arg BLOCK=read|filter|aggregate`), DAG `Read → Filter → Aggregate → Publish Dataset`, Attempt Fence, ретрай удалённой посреди запуска попытки, отмена запуска, повторный `POST /pipeline-runs` с тем же `Idempotency-Key` и рестарт посреди запуска каждого из `api`, Execution Controller, `kafka`, `postgres` и `minio` (одна DatasetVersion с верными агрегатами, попыток не больше лимита) из `tests/e2e` (каждый тест входит через `mock-oauth2-server` и создаёт свой Project) (`E3_RECONCILE_AFTER_SECONDS=15`; сервисы тесты перезапускают через `docker compose restart`, контроллер — сами, по pid из `E3_E2E_CONTROLLER_PID_FILE`); в конце всё разбирает (`E3_E2E_KEEP=1` — оставить). Нужны Docker, helm, uv; kind (и kubectl) качаются в `.cache/bin`. Порты как у `docker compose up`. В CI — workflow `E2E` на push в `main` и вручную.
- S3-ключи `E3_S3_ACCESS_KEY` и `E3_S3_SECRET_KEY` обязательны и у API, и у Execution Controller: значений по умолчанию нет, без них сервис не стартует. Compose и `scripts/e2e.sh` задают их явно.
- Execution Controller ходит в MinIO не root, а пользователем `e3-execution-controller`. Его заводит `minio-init` в compose (образ `cgr.dev/chainguard/minio-client`), политика — `scripts/minio/execution-controller-policy.json` (запись в `staging/executions/*` и `artifacts/*`). Этими ключами контроллер через MinIO STS `AssumeRole` выдаёт каждой попытке Attempt Credentials (ADR-0006) в Secret `e3-{attempt_id}-s3`. Его владелец — `SparkApplication` попытки. Срок жизни — `E3_ATTEMPT_CREDENTIALS_SECONDS` (по умолчанию 43200, от 900 до 365 дней).
- Логи API и Execution Controller — JSON-строки (`e3.shared.tracing`), `trace_id` = `pipeline_run_id` в них отдельное поле. Путь запуска: `docker compose logs api | grep <pipeline_run_id>`, ресурсы попытки — `kubectl get sparkapplications,pods -l e3.io/trace-id=<pipeline_run_id>`.
- После итога попытки контроллер своими ключами кладёт логи driver и executor в `artifacts/{node_run_id}/{attempt_id}/driver.log`, `executor-{id}.log` (Diagnostic Artifact), у отменённой — до удаления её pod'ов. Сохранённым попыткам ставит метку `e3.io/logs-preserved`, сбой повторяет на следующем проходе. Executor pod'ы остаются до удаления `SparkApplication` (`spark.kubernetes.executor.deleteOnTermination=false`). От Kubernetes контроллеру нужен `get` на `pods/log`. Своего RBAC в репозитории нет: `scripts/e2e.sh` запускает контроллер с admin-kubeconfig kind.
- Официального `minio/minio` на Docker Hub больше нет, используем `cgr.dev/chainguard/minio`.
- Staging попыток (`staging/executions/{attempt_id}/`, все попытки, включая неуспешные) чистит фоновая задача Control Plane через `E3_STAGING_RETENTION_DAYS` (по умолчанию 7) после `finished_at` PipelineRun; проверяет раз в `E3_STAGING_PURGE_PERIOD_SECONDS` (по умолчанию 3600). Очищенный запуск помечен `staging_purged_at`. `datasets/` и `staging/uploads/` не трогает, записи в БД остаются. У запусков, завершённых до миграции 0013, `finished_at` нет — их staging не чистится.
- Audit Event (`PRD.md` §9) читается только SQL-запросом к `audit.events`, API для журнала нет. Приложение туда только добавляет: разрешённое действие — в своей транзакции, отказ (403) — отдельной, повторы отсекает уникальный `(action, dedup_key)`. `user_id = NULL` — Platform Team (создание Organization и Project); изменения Membership пишутся без `dedup_key`, каждое отдельной записью.
