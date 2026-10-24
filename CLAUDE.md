## Agent skills

### Issue tracker

Задачи живут в GitHub Issues (`trudogolik45/e3-data-platform`), через `gh`. См. `docs/agents/issue-tracker.md`.

### Triage labels

Стандартные метки: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`. См. `docs/agents/triage-labels.md`.

### Domain docs

Single-context: один `GLOSSARY.md` и `docs/adr/` в корне. См. `docs/agents/domain.md`.

## Команды

- Поднять всё локально: `docker compose up -d --build` (API на `:8000`, MinIO на `:9000`).
- Lint: `uv run ruff check . && uv run ruff format --check . && uv run lint-imports`
- Типы: `uv run pyright`
- Тесты: `uv run pytest` (integration требует Docker: testcontainers поднимает PostgreSQL и MinIO).
- JSON Schema контрактов Backend ↔ DS (PRD §4, включая Manifest v1): `uv run e3-contracts-schema`.
- Зарегистрировать блок: `E3_API_URL=http://localhost:8000 E3_PLATFORM_TOKEN=... uv run e3-register-block spec.yaml`.
- Официального `minio/minio` на Docker Hub больше нет, используем `cgr.dev/chainguard/minio`.
