# Лабораторная: распределённые системы, вариант 2 «Кинотеатр»

Два сервиса Tickets/Loyalty, RabbitMQ и постоянные SQLite-базы. Требуется Docker Compose. Порты: Tickets 8001, Loyalty 8002, RabbitMQ Management 15672 (cinema / cinema-local). Порты привязаны к localhost. Административные маршруты без аутентификации предназначены только для локального учебного стенда.

```powershell
docker compose up --build -d
docker compose logs -f
```

Обычный `docker compose up` тоже запускает стенд. По умолчанию синхронный режим с идемпотентностью и предохранителем. Настройки шагов меняются через `/admin/config`, хранятся в памяти и сбрасываются при перезапуске сервиса. SQLite и очередь переживают перезапуск благодаря именованным томам.

## Шаг 1: потеря ответа

В PowerShell используйте `curl.exe`, а не псевдоним curl. Создайте файл тела и включите намеренно небезопасные режимы:

```powershell
'{"session_id":"s1","seat":"A1","customer_id":"c1"}' | Set-Content -Encoding ascii ticket.json
Invoke-RestMethod http://localhost:8001/admin/config -Method Post -ContentType application/json -Body '{"mode":"sync","idempotent":false,"protection":"none"}'
Invoke-RestMethod http://localhost:8002/admin/config -Method Post -ContentType application/json -Body '{"delay":1.5,"dedup":false}'
curl.exe --max-time 1 -H 'Content-Type: application/json' --data-binary '@ticket.json' http://localhost:8001/tickets
curl.exe -H 'Content-Type: application/json' --data-binary '@ticket.json' http://localhost:8001/tickets
Start-Sleep 2
Invoke-RestMethod http://localhost:8001/state | ConvertTo-Json -Depth 8
Invoke-RestMethod http://localhost:8002/state | ConvertTo-Json -Depth 8
```

Ожидается два билета и два начисления. Затем включите `idempotent:true` в Tickets и повторите обе команды curl с одинаковым `-H 'Idempotency-Key: demo-fixed'`. Если первый запрос ещё выполняется, повтор получает 409; после его завершения повторите ещё раз — получите прежний 201 и ticket_id. Тот же ключ с другим телом даёт 409. Не проверяем занятость места намеренно, как требует условие.

## Шаг 2: одинаковая нагрузка в трёх режимах

```powershell
Invoke-RestMethod http://localhost:8002/admin/config -Method Post -ContentType application/json -Body '{"delay":20,"dedup":true}'
Invoke-RestMethod http://localhost:8001/admin/config -Method Post -ContentType application/json -Body '{"mode":"sync","idempotent":true,"protection":"none"}'
python scripts/load.py --label none --rate 5 --seconds 30 > evidence/none.json
```

Tickets имеет один процесс gunicorn с четырьмя потоками, в том числе для `/health`. Нагрузочный скрипт выдаёт уникальные ключи, опрашивает `/health`, измеряет долю ответов 201 не позже пяти секунд, среднее время и p95. Нулевой статус обозначает отсутствие ответа в пределах клиентского тайм-аута.

Перед следующим экспериментом дождитесь завершения накопленных вызовов: при отсутствии защиты это может занять несколько минут. Проверяйте `/health` и логи. Для строгой изоляции можно перезапустить оба сервиса (`docker compose restart tickets loyalty`), дождаться их готовности и повторно задать настройки, поскольку они сбрасываются.

Повторите с `protection:"timeout"`, сохранив `evidence/timeout.json`, и с `protection:"breaker"`, сохранив `evidence/breaker.json`. У Loyalty каждый раз delay=20. Тайм-аут 1,5 с; предохранитель открывается после 5 ошибок подряд на 10 с, допускает один пробный запрос. Ошибки — тайм-ауты, ошибки соединения и 5xx. Другие HTTP-ошибки завершают вызов, но не увеличивают счётчик предохранителя. Нельзя ожидать 100% успеха даже с защитой: при перегрузке запросы стоят в очереди.

Проверка восстановления: `docker compose stop loyalty`; продолжайте `python scripts/load.py --label stopped`, смотрите `docker compose logs tickets` до OPEN. Затем `docker compose start loyalty`; её задержка сбросится к нулю. Продолжайте нагрузку до HALF_OPEN и CLOSED. Предохранитель не обнаруживает отказ без запросов.

## Шаг 3: события и повторная доставка

```powershell
Invoke-RestMethod http://localhost:8001/admin/config -Method Post -ContentType application/json -Body '{"mode":"async","idempotent":true}'
docker compose stop loyalty
1..3 | ForEach-Object { curl.exe -H 'Content-Type: application/json' -H "Idempotency-Key: async-$([guid]::NewGuid())" --data-binary '@ticket.json' http://localhost:8001/tickets }
docker compose start loyalty
Start-Sleep 3
Invoke-RestMethod http://localhost:8002/state | ConvertTo-Json -Depth 8
```

Все три покупки должны вернуть 201 при остановленном Loyalty. Для дубликатов включите в Loyalty `dedup:false, delay:0` и выполните:

```powershell
docker compose exec -e START_WORKER=0 tickets python -m scripts.duplicate
```

Скрипт публикует одинаковые ticket_id и message_id дважды. Для `duplicate-demo` баланс увеличится на 20. Включите `dedup:true`, снова запустите скрипт: он создаст новую пару идентификаторов, но доставит её дважды; прирост составит только 10. Сравнивайте приращение, поскольку предыдущие демонстрации не очищают БД. Повторите событие с прежним ticket_id после `docker compose restart loyalty`: новой строки начисления не будет. `/state` показывает ticket_id для проверки.

Tickets сохраняет билет и outbox в одной транзакции, возвращает 201 без ожидания RabbitMQ. Фоновый издатель объявляет durable exchange, очередь и binding до публикации; использует persistent-сообщения, mandatory и publisher confirms. После сбоя между подтверждением и отметкой sent возможна повторная публикация. Loyalty подтверждает получение только после коммита SQLite. Уникальный ticket_id и SUM начислений устраняют повторный эффект без processed_messages.

## Проверки и отчёт

```powershell
docker compose exec -e START_WORKER=0 tickets python -m unittest discover -s tests -v
docker compose logs --no-color tickets loyalty > evidence/services.log
```

Восемь тестов проверяют идемпотентность, конфликт ключа, незавершённый запрос, дубли в небезопасном режиме, продажу при ошибке Loyalty, outbox, естественную идемпотентность и предохранитель. Тесты создают временную БД и не используют брокер.

Дополнительно есть три автономных теста фактического класса Breaker без Flask: `python -m unittest discover -s tests -p test_breaker_standalone.py -v`. На машине подготовки эти три теста прошли; проверка синтаксиса Python также прошла. Полный комплект приложения и Docker требуют отдельного запуска: установка зависимостей в среде подготовки завершилась сетевой ошибкой.

Отчёт: [REPORT.md](REPORT.md). Перед сдачей выполните эксперименты и дополните отмеченные места реальными данными. На машине подготовки Docker отсутствует: контейнерные эксперименты не выполнены, результаты не придуманы.

Ограничения: нет проверки занятости места; при сбое синхронного процесса между сохранением билета и фиксацией ответа ключ останется pending (409, нужна ручная сверка); тайм-аут не отменяет уже начатое начисление; ядовитые сообщения переотправляются без DLQ. Это учебный стенд, не платёжная система.

Механика подтверждений: [RabbitMQ](https://www.rabbitmq.com/docs/confirms). Ожидание готовности контейнеров: [Docker Compose](https://docs.docker.com/compose/how-tos/startup-order/).
