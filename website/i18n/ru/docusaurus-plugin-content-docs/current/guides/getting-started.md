---
sidebar_position: 1
description: "Установка ruststream-sqlx, описание таблицы очереди, первый подписчик на SQLite и его тест без сервера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-sqlx`, создаёт таблицу очереди и запускает на ней первый сервис, на SQLite и без сервера.

## Установка {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sqlx = { version = "0.7", features = ["inbox", "sqlite", "chrono"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "sqlite", "derive", "chrono"] }
chrono = "0.4"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sqlx = { version = "0.7", features = ["inbox", "sqlite", "chrono", "testing"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-sqlx` обслуживает очередь, а `sqlx` - клиент, через который сервис уже работает со своей базой данных.
Фича `testing` добавляет [режим в процессе](../components/testing/index.md), в котором работают тесты.

Фич по умолчанию у крейта нет. Сервис включает то, чем пользуется:

| Фича | Что она даёт |
|---|---|
| `inbox` | [очередь задач](../components/tables/index.md) в таблицах сервиса, которую обслуживает [`SqlxBroker`](../components/broker/index.md) |
| `outbox` | [транзакционный outbox](../components/outbox/index.md) поверх любого брокера RustStream |
| `postgres`, `mysql`, `sqlite` | встроенный [диалект](../components/databases/index.md) и его драйвер sqlx; `mysql` обслуживает MySQL и MariaDB |
| `any` | [`AnyPool`](../components/databases/any-pool.md), который обслуживает диалект той базы данных, к которой пул обращается |
| `chrono`, `time` | [столбцы времени](../components/time/queue-time.md) в типах этого крейта |
| `json` | [столбец заголовков](../components/headers/index.md) в JSON |
| `testing` | режим в процессе, на котором работает `TestApp`, и [тестовый переключатель](../components/outbox/switch.md) outbox |
| `asyncapi` | таблицы в генерируемом [документе AsyncAPI](../components/asyncapi/index.md) |

## Таблица {/* #the-table */}

Очередь - это собственная таблица сервиса, и создаёт её сервис:

{/* single-form: the schema is SQL, the same for both forms */}
```rust title="src/main.rs" reference="examples/guide/getting_started.rs#schema"
```

Каждая строка - одна задача.
`id` идентифицирует строку, `attempt` считает её доставки, а `payload` хранит закодированное сообщение.
`locked_until` хранит [аренду](../components/forms/lease.md) строки в работе: в SQLite нет блокировок строк, поэтому очередь на SQLite захватывает строки под арендой.
Формы, которые может принять таблица, сравнивает руководство [Долгие задачи, сбои и форма захвата](claim-forms.md).

## Первый сервис {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

[`#[derive(Inbox)]`](../components/tables/index.md) описывает таблицу: `#[inbox(table = ..)]` называет её, а каждый `#[field(..)]` задаёт столбцу его [роль](../components/tables/roles.md).
`generated` отмечает столбец, который заполняет база данных.
В ручной форме то же описывает [`InboxSpec`](../components/tables/inbox-spec.md).
[`InboxQueue::<MakeThumbnail>::new("thumbnails")`](../components/subscriptions/index.md) - это подписка: она захватывает строки `thumbnail_jobs` и передаёт полезную нагрузку каждой из них кодеку.
Строку завершает то, что возвращает обработчик: `ack()` удаляет её, а `drop()` завершает её так же, но без миниатюры.
[`Ctx<keys::Attempt>`](../components/context/attempt.md) читает, какая это по счёту доставка строки.

Публикация в `thumbnails` записывает строку.
[`route`](../components/publishing/route.md) связывает это имя с `MakeThumbnail`, а вставляет строку собственный оператор [`Publish`](../components/publishing/index.md) сервиса.
Ответ обработчика и тест публикуют так же.
Код вне обработчика, например конечная точка HTTP, записывает строку через [`insert`](../components/tables/insert.md) из derive в собственной транзакции, как показывает руководство [Постановка задач в очередь](enqueueing.md).

## Запуск сервиса {/* #running-the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#main"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#main"
```

</TabItem>
</Tabs>

`main` сам запускает рантайм Tokio, потому что sqlx строит пул только внутри него.
[`SqlxBroker::new`](../components/broker/index.md) принимает этот пул, а пул остаётся за сервисом.

```bash
cargo run
```

Сервис захватывает строки по мере их появления в `thumbnails.db`.
Строку, которую вставила другая программа, он видит в пределах [интервала опроса](../components/subscriptions/poll-interval.md), по умолчанию одной секунды.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

## Первый тест {/* #the-first-test */}

В файле примера есть и два теста.
Каждый строит базу данных SQLite в памяти, создаёт таблицу и запускает тот же `service` через `TestApp::start`.
Тест публикует изображение через маршрут, как в промышленной среде, и проверяет, как `thumbnail` завершил строку и что осталось в таблице:

```bash
cargo test
```

Эти тесты разбирает руководство [Тестирование сервиса на SQL](testing.md).

## Переход на Postgres или MySQL {/* #moving-to-postgres-or-mysql */}

Та же структура обслуживает `PgPool` или `MySqlPool`: брокер берёт диалект своего пула.
Включите `postgres` или `mysql` в обоих крейтах, постройте такой пул и напишите `Publish<Postgres>` или `Publish<MySql>` с параметрами-заполнителями этой базы данных.
На сервере таблица без `locked_until` захватывает строки под [блокировкой строк](../components/forms/index.md), которую захват держит, пока работает обработчик.
Постройте индекс по порядку захвата: `(group, priority, retry_after, id)` из тех столбцов, что есть в таблице. Зачем он нужен, объясняет руководство [Работа в промышленной среде](production.md).

## Переход с самописной очереди {/* #coming-from-a-hand-written-queue */}

Очередь на sqlx обычно - это цикл `SELECT .. FOR UPDATE SKIP LOCKED` и несколько служебных столбцов.
Вот куда переходит каждая часть:

| Самописная очередь | ruststream-sqlx |
|---|---|
| цикл захвата и его оператор | подписка [`InboxQueue`](../components/subscriptions/index.md); оператор строит [диалект](../components/databases/index.md) |
| `DELETE` или `UPDATE .. SET done` после работы | [исход](../components/settlement/index.md) обработчика |
| столбец `attempts`, который увеличивают вручную | `#[field(attempt)]` и [`Ctx<keys::Attempt>`](../components/context/attempt.md) |
| столбец `run_at` для отложенного запуска | `#[field(retry_after)]` и [`retry_after(d)`](../components/settlement/retry-after.md) ([Переобработка](retries.md)) |
| столбец `locked_until` и задача, которая освобождает зависшие строки | [форма аренды](../components/forms/lease.md) ([Долгие задачи](claim-forms.md)) |
| таблица dead letter и перемещение в неё | `max_attempts(n)` и [`dead_letter`](../components/settlement/dead-letter.md) |
| задача, вставленная вместе с данными, которые она обслуживает | [`insert`](../components/tables/insert.md) в той же транзакции ([Постановка задач в очередь](enqueueing.md)) |
