# Базы данных SQL {#sql-databases}

`ruststream-sqlx` подключает базы данных SQL к сервису
[RustStream](https://powersemmi.github.io/ruststream/) через [`sqlx`](https://docs.rs/sqlx).
Крейт состоит из двух компонентов:

- Транзакционный outbox поверх любого брокера RustStream. Сообщения, которые публикует
  обработчик, сохраняются в той же транзакции, что и его собственные изменения, и брокер получает
  их после фиксации транзакции.
- Очереди задач в таблицах Postgres, MySQL/MariaDB и SQLite, которыми владеет сервис.

## Где всё остальное {#where-the-rest-is}

Справочник по крейту находится на docs.rs: [`ruststream-sqlx`](https://docs.rs/ruststream-sqlx).

Обработчики, роутеры, кодеки и middleware даёт сам фреймворк, а его входные страницы начинаются
с [сайта RustStream](https://powersemmi.github.io/ruststream/).
