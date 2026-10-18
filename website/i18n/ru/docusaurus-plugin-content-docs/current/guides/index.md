---
slug: /
sidebar_position: 0
description: "Руководства по базам данных SQL в сервисах RustStream с ruststream-sqlx: очереди задач в таблицах сервиса и транзакционный outbox."
---

# Руководства

Крейт `ruststream-sqlx` подключает базы данных SQL к сервису
[RustStream](https://powersemmi.github.io/ruststream/latest/ru/) через [sqlx](https://docs.rs/sqlx).
Он состоит из двух частей. Inbox служит очередью задач в собственной таблице сервиса, и её
обслуживает брокер RustStream. Outbox хранит запись о каждом сообщении, которое сервис публикует
через любой брокер RustStream, пока получатель его не обработает. Обе части работают с Postgres,
MySQL и MariaDB, а также с SQLite.
