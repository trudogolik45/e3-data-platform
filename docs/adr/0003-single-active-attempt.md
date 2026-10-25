# Одна активная ExecutionAttempt на NodeRun и строгий критерий её смерти

У NodeRun в каждый момент не больше одной активной ExecutionAttempt — Current Attempt, которую назначил Run Orchestrator. Только её результат может быть принят, события чужих попыток игнорируются. Ретраи решает только Run Orchestrator: 3 ретрая, то есть максимум 4 попытки. Execution Controller сам ничего не перезапускает. Новая попытка стартует только после того, как старая доказуемо мертва. Для этого нужны три условия:
1. `SparkApplication` попытки в терминальном состоянии или удалён.
2. Нет pod'ов driver или executor с её `attempt_id`.
3. Kubernetes не может автоматически восстановить этот workload.

## Considered Options

- Запускать новую попытку, не дожидаясь остановки старой (данные изолированы по `attempt_id`): ретраи быстрее, но временно работают два Spark-задания и сложнее рассуждать о состоянии. Отклонено.
- Опираться на порядок команд в Kafka, чтобы опоздавшая `SubmitExecution` не пришла после проверки: outbox этот порядок не гарантирует. Отклонено.
- Хранить Attempt Fence отдельным объектом рядом с `SparkApplication`: между проверкой запрета и созданием задания остаётся щель. Отклонено в пользу одного объекта с тем же именем.

## Consequences

- Таймаут не объявляет попытку `LOST`: он только запускает Reconciliation. `LOST` ставит Run Orchestrator по событию `ExecutionLost`, которое Execution Controller шлёт после Attempt Fence и проверки всех трёх условий.
- Живая, но зависшая попытка в MVP может висеть бесконечно.
- Attempt Fence реализован через `SparkApplication` с `spec.suspend: true`: на Spark Operator 2.5.2 такой ресурс уходит в `SUSPENDED` и не запускает ни driver, ни executor (e2e `test_suspended_attempt_fence_starts_no_pods`).
- Ретраятся `SPARK_FAILED`, `SUBMISSION_FAILED` и `LOST` — максимум три повторные попытки. `NO_RESULT`, `OUTPUT_REJECTED` и `UNKNOWN` не ретраятся.
- Итог попытки фиксируется один раз: `ExecutionSucceeded` только сохраняет результат, а `SUCCEEDED` или `FAILED` ставит Run Orchestrator после проверки manifest.
- Один NodeRun имеет не более одного принятого результата, сколько бы ни было попыток и повторных доставок команд.
