# TaskApi

Тестовое задание: .NET 9 Minimal API + PostgreSQL (EF Core) + RabbitMQ.

## Запуск

```bash
docker compose up -d                 # Postgres :5433, RabbitMQ :5672 (UI :15672, guest/guest)
dotnet run --project TaskApi         # миграции применяются при старте
```

Или целиком в контейнерах: `docker compose --profile app up -d --build`, API на `http://localhost:5080`.

Тесты (нужен запущенный Docker, Postgres и RabbitMQ поднимаются через Testcontainers):

```bash
dotnet test
```

## API

| Метод | Эндпоинт | Ответы |
|-------|----------|--------|
| POST | `/tasks` | 201, 400 (пустой/длинный Title, неизвестный Priority) |
| GET | `/tasks?skip=&take=` | 200; без параметров отдает все задачи |
| PUT | `/tasks/{id}/complete` | 200, 404, 409 (уже завершена) |
| DELETE | `/tasks/{id}` | 204, 404 |

## Что где

- `TaskItem.cs`: сущность, DTO и событие. `CompletedAt` ставится только в `Complete()`.
- `TaskDbContext.cs`: маппинг. RowVersion сделан через системную колонку Postgres `xmin`, своя колонка не нужна.
- `RabbitMqService.cs`: публикация событий и graceful shutdown.
- `Program.cs`: регистрация сервисов и эндпоинты (`#region`).
- `TaskApi.Tests/CompleteTaskIntegrationTest.cs`: сценарий «создать, завершить, получить сообщение в Rabbit» + гонка на complete + валидация.

## Решения

**Конкурентность.** `xmin` помечен как concurrency token, поэтому EF делает `UPDATE ... WHERE "Id" = @id AND xmin = @v`.
Если параллельный запрос завершил задачу раньше, обновится 0 строк, EF кинет `DbUpdateConcurrencyException`, в ответ 409.
В тесте 10 параллельных `complete` дают ровно один 200 и девять 409.

**RabbitMQ без блокировки.** Эндпоинт кладет событие в `Channel<T>` в памяти и сразу отвечает.
Отправку делает `BackgroundService`. Соединение открывается лениво и пересоздается после ошибки.
Если брокер недоступен, задача все равно завершена, а событие целиком пишется в лог с уровнем Error.
Publisher confirms не жду: в ТЗ это разрешено.

**Graceful shutdown.** В `StopAsync` очередь закрывается на запись, фоновый цикл дочитывает то, что уже успело в нее попасть,
но не дольше `RabbitMq:ShutdownTimeoutSeconds` (5 с). Потом закрываются канал и соединение.
Проверял вручную: завершил 30 задач, сразу после этого `docker stop` (SIGTERM), в очереди оказалось 30 сообщений, exit code 0.

## Где срезал углы

- `HACK`: очередь событий в памяти. При падении процесса (kill -9) неотправленные события теряются.
  В проде сделал бы transactional outbox: событие пишется в таблицу в той же транзакции, что и завершение задачи, а отдельный воркер публикует его с ретраями.
- `HACK`: миграции применяются на старте приложения. В проде вынес бы их в отдельный шаг деплоя (`dotnet ef migrations bundle`).
- Ретраев публикации нет: ошибка уходит в лог, событие теряется. Это осознанно, по ТЗ «fail silently, но с логом».
- Невалидный Guid в маршруте дает 404, а не 400: так работает route constraint `{id:guid}`, запрос просто не находит эндпоинт.
