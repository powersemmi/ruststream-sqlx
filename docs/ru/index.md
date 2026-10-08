# Базы данных SQL {#sql-databases}

`ruststream-sqlx` подключает базы данных SQL к сервису
[RustStream](https://powersemmi.github.io/ruststream/) через [`sqlx`](https://docs.rs/sqlx).
Крейт состоит из двух компонентов:

- inbox - очереди задач в собственных таблицах сервиса в Postgres, MySQL/MariaDB и SQLite. Их
  обслуживает брокер RustStream;
- транзакционный outbox. Сервис публикует сообщения через любой брокер RustStream, а каждое
  сообщение хранится в отдельной таблице сервиса, пока подписчик его не обработает.

```toml
ruststream = { version = "0.7", features = ["macros"] }
ruststream-sqlx = { version = "0.7", features = ["inbox", "outbox", "postgres"] }
sqlx = { version = "0.9", features = ["runtime-tokio", "postgres", "derive", "chrono"] }
chrono = "0.4"
serde = { version = "1", features = ["derive"] }
```

Функции `inbox` и `outbox` включают компоненты, и каждую можно включить отдельно от другой.
Функция драйвера выбирает базу данных: `postgres`, `mysql` (MySQL и MariaDB), `sqlite` или `any`
для `AnyPool`.

## Очереди задач в таблицах сервиса {#task-queues-in-the-services-tables}

```rust
--8<-- "crates/ruststream-sqlx/examples/inbox.rs:service"
```

`#[derive(Inbox)]` описывает таблицу очереди: её имя и роль каждого столбца. `SqlxBroker`
обслуживает очереди из пула соединений сервиса. Подписка берёт строки в работу, а от результата
обработчика зависит, что станет со строкой: подтверждение удаляет строку, переобработка возвращает её в
очередь. Публикация записывает строку собственным запросом сервиса.

Подписка берёт строки в работу в одном из трёх режимов. В режиме блокировки строк транзакция
остаётся открытой, пока работает обработчик. В режиме аренды подписка сразу фиксирует транзакцию.
В режиме рекомендательных блокировок подписка блокирует ключ строки. Обработчик, при регистрации
которого указан `.transactional()`, записывает данные через транзакцию своей доставки, и
подтверждение фиксирует его изменения вместе со строкой. Каждая роль столбца включает одно
поведение: группы, порядок, в котором строки берутся в работу, отложенную переобработку,
ограничение числа попыток с переносом строки в другую группу или таблицу и отметку, при которой
обработанная строка остаётся в таблице.

Ручной API - второй способ описать ту же таблицу, наравне с `#[derive]`: структура реализует
трейт, а все настройки таблицы задаёт типизированный билдер. Компилятор проверяет такое описание
так же строго, как `#[derive]`. Оба способа дают одни и те же запросы и одинаковые затраты.

Команда, которая проверяет свой SQL во время компиляции, добавляет в `#[derive]` параметр
`checked`. Тогда `cargo sqlx prepare` проверяет запросы `#[derive]` вместе с собственными
запросами сервиса. Тест запускает собственное приложение сервиса в `TestApp` на настоящей базе
данных, потому что SQL сервиса тоже входит в то, что проверяет тест. Для SQLite сервер не нужен.

## Транзакционный outbox {#the-transactional-outbox}

```rust
--8<-- "crates/ruststream-sqlx/examples/outbox.rs:service"
```

`#[derive(Outbox)]` описывает таблицу outbox сервиса, а `outbox!` регистрирует её под теми
именами, которые она отслеживает. Middleware публикации записывает каждое отслеживаемое
сообщение перед отправкой и передаёт идентификатор записи в заголовке. Middleware подписки
по этому идентификатору берёт запись в работу и помечает её обработанной, когда обработчик
подтверждает сообщение. При запуске сервис заново публикует все необработанные записи.

Сообщение доставляется хотя бы один раз. Два экземпляра сервиса, которые запускаются
одновременно, могут оба заново отправить одну и ту же запись. Если подписчик не должен
обрабатывать сообщение дважды, он проверяет это сам.

Внутри `#[ruststream::app]` приложение строится до запуска среды выполнения Tokio, а sqlx создаёт
пул соединений только внутри неё. Поэтому реестр начинает работу без пула: `on_startup` создаёт
пул и передаёт его через `set_pool`. В тестовой сборке outbox выключен, пока в окружении не задана
переменная `RUSTSTREAM_SQLX_OUTBOX=on`.

Middleware outbox оборачивают обработчики inbox так же, как любые другие. Запись
выполняется через отдельное соединение, поэтому она фиксируется, даже когда транзакция доставки
откатывается. Сообщение, которое отслеживается и попадает в таблицу inbox, сохраняет
идентификатор записи, если в таблице есть столбец `headers` и его заполняет запрос публикации
сервиса. Подробности - в разделе
[о транзакционном outbox](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-transactional-outbox).

## Что выбрать {#which-one-to-use}

Inbox подходит для работы, которая относится к данным сервиса: задача записывается в той же
транзакции, что и данные, для которых она создана, а очередь хранится в базе данных, с которой
сервис уже работает. Outbox подходит для сообщений, которые сервис публикует через другой брокер и
не должен потерять. Сервис может использовать оба компонента.

## Где всё остальное {#where-the-rest-is}

Справочник на docs.rs начинается с учебника по крейту, по разделу на тему:

- [Брокер inbox](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-inbox-broker):
  таблица очереди, её подписки и то, что результат обработчика делает со строкой.
- [Макрос или ручное описание](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#macro-or-manual):
  таблица очереди, описанная вручную через `InboxTable` и типизированный билдер.
- [Роли](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#roles) и
  [время](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#time): что включает
  каждый столбец и откуда берётся текущее время.
- [Блокировки строк, аренда и рекомендательные блокировки](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#row-locks-leases-or-advisory-locks)
  и
  [транзакционный режим](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#transactional-mode):
  как подписка берёт строки в работу и как обработчик записывает данные через транзакцию своей
  доставки.
- [Режим строки](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#row-mode),
  [структура заголовков](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-headers-layout)
  и [пакеты](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#batches): что
  обработчик получает из строки.
- [Базы данных](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#databases) и
  [собственный диалект сервиса](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#a-dialect-of-the-services-own):
  какие запросы выполняет каждая база данных и как написать диалект для другой.
- [Проверка запросов во время компиляции](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#statements-checked-at-compile-time):
  режим `checked`.
- [Пробуждение подписки](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#waking-a-subscription):
  интервал опроса и `LISTEN/NOTIFY` в Postgres.
- [Тестирование сервиса на inbox](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#testing-a-service-on-the-inbox):
  собственное приложение сервиса в `TestApp`.
- [Транзакционный outbox](https://docs.rs/ruststream-sqlx/latest/ruststream_sqlx/index.html#the-transactional-outbox):
  гарантия доставки, запись и её реестр, middleware, повторная публикация, пул соединений,
  тестирование и затраты.

Во что крейт обходится на каждом сообщении и сколько сообщений он обрабатывает в секунду рядом с
циклом на чистом sqlx, который делает ту же работу, показано на [странице бенчмарков](benchmarks.md).

Обработчики, роутеры, кодеки и middleware даёт сам фреймворк, а его входные страницы начинаются
с [сайта RustStream](https://powersemmi.github.io/ruststream/).
