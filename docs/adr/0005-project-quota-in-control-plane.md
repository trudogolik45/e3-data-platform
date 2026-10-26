# Project Quota считает Run Orchestrator, а не Kubernetes

Project Quota (потолок CPU и RAM на Project) проверяет Run Orchestrator в Control Plane перед созданием ExecutionAttempt. Проверка и создание попытки идут в одной транзакции под замком Project. Занятое место каждый раз пересчитывается по работающим попыткам Project × размер попытки, отдельного счётчика нет. Поэтому место освобождается само при любом итоге попытки, а потерянное событие догоняет периодический `dispatch_pending`. Размер попытки задаёт Control Plane и передаёт в `SubmitExecution`, Execution Controller только применяет его.

## Considered Options

- Kubernetes `ResourceQuota`: нужен namespace на каждый Project, а при превышении Kubernetes отказывает в создании pod'а, а не ставит в очередь. Отказ стал бы провалом попытки, тратил бы ретраи и запускал Fail-fast. Отклонено.
- Новое состояние попытки «ждёт квоту»: у попытки появился бы номер до запуска, и ожидание путалось бы с ретраями. Отклонено: ждёт NodeRun в PENDING.
- Хранить размер попытки в настройках и Control Plane, и Execution Controller: копии разъедутся, и квота будет считать не то, что запущено. Отклонено.

## Consequences

- Квота ограничивает заявленные CPU и RAM driver и executor, без Spark memory overhead. Это не верхняя граница реального потребления в Kubernetes.
- Квота не знает ёмкости кластера: даже в пределах квот pod может ждать в Kubernetes. Общий потолок на Execution Plane — отдельное решение.
- Если квота меньше одной попытки хотя бы по одному ресурсу, Control Plane не стартует.
