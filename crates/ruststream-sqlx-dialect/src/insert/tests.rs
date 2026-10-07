//! The `const fn` inserts: the same text as each built-in dialect's `insert`, over every shape a
//! description takes, and the refusals a `const` turns into build errors.

use std::any::Any;
use std::panic;

use crate::{Column, Dialect, Form, KeyPart, Param, StatementError, TableSpec};

use super::Sql;

const KEY: &[KeyPart<'static>] = &[KeyPart::Literal("jobs-"), KeyPart::Column("id")];
const DATA: &[Column<'static>] = &[
    Column::new("subject"),
    Column::new("created_at").generated(),
];
const FETCHED: &[Column<'static>] = &[Column::new("recipient")];
const ODD: &[Column<'static>] = &[Column::new("we\"ird"), Column::new("ba`ck")];

/// The roles a description in the matrix sets besides its id.
#[derive(Debug, Clone, Copy)]
enum Roles {
    None,
    Every { attempt_generated: bool },
    Fifo,
}

fn forms() -> [Form<'static>; 4] {
    [
        Form::RowLock,
        Form::Lease(Column::new("locked_until")),
        Form::Lease(Column::new("locked_until").generated()),
        Form::Advisory(KEY),
    ]
}

fn with_roles(spec: &TableSpec<'static>, roles: Roles) -> TableSpec<'static> {
    let spec = *spec;
    match roles {
        Roles::None => spec,
        Roles::Every { attempt_generated } => {
            let attempt = Column::new("attempt");
            spec.group(Column::new("name"))
                .partition_key(Column::new("customer"))
                .priority(Column::new("priority"))
                .retry_after(Column::new("retry_after"))
                .attempt(if attempt_generated {
                    attempt.generated()
                } else {
                    attempt
                })
                .processed_at(Column::new("processed_at"))
                .headers(Column::new("headers"))
                .payload(Column::new("payload"))
        }
        Roles::Fifo => spec
            .fifo_group(Column::new("account"))
            .payload(Column::new("payload")),
    }
}

/// Every shape a description takes: schema or none, a generated id or not, each form, every role
/// or none, a generated attempt, data and fetched columns, and names that need escaping.
fn descriptions() -> Vec<TableSpec<'static>> {
    let mut all = Vec::new();
    for (table, schema) in [
        ("email_jobs", None),
        ("email_jobs", Some("app")),
        ("we\"ird`jobs", Some("o\"dd`schema")),
    ] {
        for id in [Column::new("job_id"), Column::new("job_id").generated()] {
            for form in forms() {
                for roles in [
                    Roles::None,
                    Roles::Every {
                        attempt_generated: false,
                    },
                    Roles::Every {
                        attempt_generated: true,
                    },
                    Roles::Fifo,
                ] {
                    for data in [&[][..], DATA, ODD] {
                        for fetched in [&[][..], FETCHED] {
                            let mut spec = TableSpec::new(table, id, form);
                            if let Some(schema) = schema {
                                spec = spec.within(schema);
                            }
                            all.push(with_roles(&spec, roles).data(data).fetching(fetched));
                        }
                    }
                }
            }
        }
    }
    all
}

/// The parameters an insert of `spec` binds: the position of every column the database does not
/// fill, in `TableSpec::columns` order.
fn written(spec: &TableSpec<'_>) -> Vec<Param> {
    spec.columns()
        .enumerate()
        .filter(|(_, column)| !column.is_generated())
        .map(|(position, _)| Param::Column(position))
        .collect()
}

/// The renderer's text equals `dialect`'s insert for every description, and the insert binds
/// every written column by its position.
fn renders_as_the_dialect(
    dialect: &dyn Dialect,
    render: fn(&TableSpec<'_>) -> Sql<1024>,
) -> Result<(), StatementError> {
    for spec in descriptions() {
        let insert = dialect.insert(&spec)?;
        assert_eq!(render(&spec).as_str(), insert.sql(), "{spec:?}");
        assert_eq!(insert.params(), written(&spec), "{spec:?}");
    }
    Ok(())
}

/// What a panic says, whichever way its message was formatted.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}

/// The message `render` panics with.
fn refusal(render: impl FnOnce() + panic::UnwindSafe) -> String {
    let payload = panic::catch_unwind(render).expect_err("the renderer refuses");
    panic_message(&*payload)
}

const EMAILS: TableSpec<'static> = TableSpec::new(
    "email_jobs",
    Column::new("job_id").generated(),
    Form::Lease(Column::new("locked_until")),
)
.within("app")
.group(Column::new("name"))
.attempt(Column::new("attempt").generated())
.payload(Column::new("payload"))
.data(&[Column::new("recipient")]);

const BARE: TableSpec<'static> = TableSpec::new(
    "jobs",
    Column::new("id").generated(),
    Form::Lease(Column::new("locked_until").generated()),
);

#[test]
fn a_capacity_that_fits_the_text_exactly_holds_it() {
    #[cfg(feature = "sqlite")]
    {
        const EXACT: Sql<33> = super::sqlite(&BARE);
        assert_eq!(EXACT.as_str(), "INSERT INTO `jobs` DEFAULT VALUES");
        assert_eq!(EXACT.to_string(), EXACT.as_str());
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use crate::Postgres;
    use crate::insert;

    const INSERT: Sql<256> = insert::postgres(&EMAILS);

    #[test]
    fn the_insert_in_a_const_is_the_dialects() -> Result<(), StatementError> {
        assert_eq!(
            INSERT.as_str(),
            "INSERT INTO \"app\".\"email_jobs\" (\"name\", \"locked_until\", \"payload\", \
             \"recipient\") VALUES ($1, $2, $3, $4)"
        );
        assert_eq!(INSERT.as_str(), Postgres.insert(&EMAILS)?.sql());
        assert_eq!(
            insert::postgres::<64>(&BARE).as_str(),
            "INSERT INTO \"jobs\" DEFAULT VALUES"
        );
        Ok(())
    }

    #[test]
    fn every_description_renders_as_the_dialect() -> Result<(), StatementError> {
        renders_as_the_dialect(&Postgres, insert::postgres)
    }

    #[test]
    fn a_name_longer_than_postgres_keeps_is_refused() {
        let long = "n".repeat(64);
        let spec = TableSpec::new("jobs", Column::new(&long), Form::RowLock);
        let message = refusal(|| {
            let _ = insert::postgres::<256>(&spec);
        });
        assert_eq!(
            message,
            format!("`{long}` is longer than the 63 bytes the postgres dialect allows in a name")
        );
        assert_eq!(
            Postgres.insert(&spec).map_err(|error| error.to_string()),
            Err(message)
        );
    }
}

#[cfg(feature = "mysql")]
mod mysql {
    use super::*;
    use crate::MySql;
    use crate::insert;

    const INSERT: Sql<256> = insert::mysql(&EMAILS);

    #[test]
    fn the_insert_in_a_const_is_the_dialects() -> Result<(), StatementError> {
        assert_eq!(
            INSERT.as_str(),
            "INSERT INTO `app`.`email_jobs` (`name`, `locked_until`, `payload`, `recipient`) \
             VALUES (?, ?, ?, ?)"
        );
        assert_eq!(INSERT.as_str(), MySql.insert(&EMAILS)?.sql());
        assert_eq!(
            insert::mysql::<64>(&BARE).as_str(),
            "INSERT INTO `jobs` () VALUES ()"
        );
        Ok(())
    }

    #[test]
    fn every_description_renders_as_the_dialect() -> Result<(), StatementError> {
        renders_as_the_dialect(&MySql, insert::mysql)
    }

    #[test]
    fn a_name_longer_than_mysql_keeps_is_refused() {
        // 65 characters in more than 65 bytes: MySQL counts characters.
        let long = "\u{e9}".repeat(65);
        let fits = "\u{e9}".repeat(64);
        let spec = TableSpec::new("jobs", Column::new(&long), Form::RowLock);
        let message = refusal(|| {
            let _ = insert::mysql::<512>(&spec);
        });
        assert_eq!(
            message,
            format!("`{long}` is longer than the 64 characters the mysql dialect allows in a name")
        );
        assert_eq!(
            MySql.insert(&spec).map_err(|error| error.to_string()),
            Err(message)
        );
        let kept = TableSpec::new("jobs", Column::new(&fits), Form::RowLock);
        assert!(insert::mysql::<512>(&kept).as_str().contains(&fits));
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::Sqlite;
    use crate::insert;

    const INSERT: Sql<256> = insert::sqlite(&EMAILS);

    #[test]
    fn the_insert_in_a_const_is_the_dialects() -> Result<(), StatementError> {
        assert_eq!(
            INSERT.as_str(),
            "INSERT INTO `app`.`email_jobs` (`name`, `locked_until`, `payload`, `recipient`) \
             VALUES (?, ?, ?, ?)"
        );
        assert_eq!(INSERT.as_str(), Sqlite.insert(&EMAILS)?.sql());
        Ok(())
    }

    #[test]
    fn every_description_renders_as_the_dialect() -> Result<(), StatementError> {
        renders_as_the_dialect(&Sqlite, insert::sqlite)
    }

    #[test]
    fn a_name_of_any_length_is_kept() {
        let long = "n".repeat(1000);
        let spec = TableSpec::new("jobs", Column::new(&long), Form::RowLock);
        assert!(insert::sqlite::<1100>(&spec).as_str().contains(&long));
    }

    #[test]
    fn a_capacity_too_small_names_the_capacity_the_table_and_the_length() {
        let message = refusal(|| {
            let _ = insert::sqlite::<16>(&EMAILS);
        });
        assert_eq!(
            message,
            "the insert into `email_jobs` takes 99 bytes, more than `Sql<16>` holds: raise `N` to \
             99"
        );
    }

    #[test]
    fn a_table_read_with_a_star_has_no_insert() {
        let message = refusal(|| {
            let _ = insert::sqlite::<256>(&EMAILS.selecting_all());
        });
        assert_eq!(
            message,
            "the insert into `email_jobs` needs every column, and the description reads the table \
             with `*` (`selecting_all`): write the insert in the service"
        );
        assert_eq!(
            Sqlite.insert(&EMAILS.selecting_all()),
            Err(StatementError::Flattened {
                statement: "insert"
            })
        );
    }
}
