# Attempt Credentials выдаёт Execution Controller

Attempt Credentials (краткоживущие S3-ключи попытки) выдаёт Execution Controller перед запуском `SparkApplication`: MinIO STS `AssumeRole` с inline-политикой. Чтение разрешено только в папках manifest'ов из `inputs`, чтение и запись — только в `output_prefix`. Всё это уже есть в `SubmitExecution`. Ключи кладутся в Kubernetes Secret попытки, его владелец — её `SparkApplication`. Так секреты не покидают Execution Plane и не попадают ни в Kafka, ни в outbox PostgreSQL.

## Considered Options

- Выдавать ключи в Control Plane и передавать в `SubmitExecution`: секрет лёг бы в outbox и лог Kafka. Отклонено.
- Один общий Secret с ключами на все попытки (как было): любая попытка читает и пишет весь bucket. Отклонено.
- Выдавать ключи от root MinIO: права попытки ограничивала бы только inline-политика. Отклонено: у Execution Controller свой пользователь MinIO с минимальными правами, и права сессии — пересечение с ними.

## Consequences

- Срок жизни задаёт настройка Execution Controller, по умолчанию 12 ч, без продления. Попытка дольше срока получает 403 и уходит в обычный ретрай.
- STS-ключи нельзя отозвать: у отменённой попытки они доживают свой срок.
- Права попытки выводятся из команды, которую Control Plane уже авторизовал. Execution Controller сам прав пользователей не проверяет.
