---
slug: /
sidebar_position: 0
description: "Guides to SQL databases in RustStream services with ruststream-sqlx: task queues in the service's tables and a transactional outbox."
---

# Guides

`ruststream-sqlx` brings SQL databases into a [RustStream](https://powersemmi.github.io/ruststream/latest/)
service through [sqlx](https://docs.rs/sqlx). It has two parts. The inbox is a task queue in the
service's own table, served as a RustStream broker. The outbox records what a service publishes on
any RustStream broker until a consumer has processed it. Both run on Postgres, MySQL and MariaDB,
and SQLite.
