//! How a by-name row holds its columns to the struct's types and binds its statements.

use std::marker::PhantomData;

use ruststream::HeaderMap;
use ruststream::codec::CodecError;
use sqlx::Error;

use super::{Fits, NamedBytes, NamedId, NamedRow, hold_to};
use crate::inbox::engine::{Claimed, undecodable};
use crate::inbox::named::kinds::{BytesKind, ClockKind, IdKind, IntKind, Kinds};

/// A struct of an `i64` id, a byte payload, a text key and an `i16` attempt.
const KINDS: Kinds = Kinds {
    clock: ClockKind::System,
    id: IdKind::I64,
    payload: BytesKind::Bytes,
    key: Some(BytesKind::Text),
    attempt: Some(IntKind::I16),
    retry_after: None,
    processed_at: None,
    locked_until: None,
};

/// A row whose columns hold the types `KINDS` reads, and only those, for the dialect `D`.
fn row<D>(fits: Fits) -> NamedRow<D> {
    NamedRow {
        id: NamedId::I64(7),
        payload: NamedBytes::Bytes(b"{}".to_vec()),
        headers: HeaderMap::new(),
        key: Some(NamedBytes::Text("acme".to_owned())),
        attempt: Some(2),
        fits,
        dialect: PhantomData,
    }
}

const FITTING: Fits = Fits {
    id: IdKind::I64.bit(),
    payload: BytesKind::Bytes.bit(),
    key: BytesKind::Text.bit(),
    attempt: IntKind::I16.bit(),
};

/// The driver's error a delivery of an undecodable row reports.
fn driver_error(error: &CodecError) -> Option<&Error> {
    match error {
        CodecError::Decode(source) => source.downcast_ref::<Error>(),
        _ => None,
    }
}

/// The column a held row names as not holding its struct's type, and the attempt it keeps.
fn unfit_column(claimed: &Claimed<NamedRow<()>>) -> Option<(String, Option<u64>)> {
    match claimed {
        Claimed::Undecodable { id, attempt, error } => {
            assert_eq!(id, &NamedId::I64(7), "the row keeps its id for the policy");
            match driver_error(error) {
                Some(Error::ColumnDecode { index, .. }) => Some((index.clone(), *attempt)),
                other => panic!("not a column's error: {other:?}"),
            }
        }
        Claimed::Row(_) | Claimed::Missing(_) => None,
    }
}

#[test]
fn logs_name_a_row_by_its_id_as_the_table_holds_it() {
    let ids = [
        NamedId::I16(1),
        NamedId::I32(2),
        NamedId::I64(3),
        NamedId::Text("job-4".to_owned()),
        NamedId::Bytes(vec![5]),
    ];
    let logged: Vec<String> = ids.iter().map(|id| format!("{id:?}")).collect();
    assert_eq!(logged, ["1", "2", "3", "\"job-4\"", "[5]"]);
}

#[test]
fn an_id_is_copied_into_the_storage_of_the_one_it_replaces() {
    let mut kept = NamedId::Text("job-0001".to_owned());
    let storage = match &kept {
        NamedId::Text(text) => text.as_ptr(),
        other => panic!("not a text id: {other:?}"),
    };
    kept.clone_from(&NamedId::Text("job-0002".to_owned()));
    assert!(
        matches!(&kept, NamedId::Text(text) if text == "job-0002" && text.as_ptr() == storage),
        "{kept:?}"
    );
    let mut bytes = NamedId::Bytes(vec![1, 2]);
    bytes.clone_from(&NamedId::Bytes(vec![3, 4]));
    assert_eq!(bytes, NamedId::Bytes(vec![3, 4]));
    // An id of another type takes the new one's type.
    kept.clone_from(&NamedId::I64(7));
    assert_eq!(kept, NamedId::I64(7));
    assert_eq!(kept.clone(), NamedId::I64(7));
    let ids = [
        NamedId::I16(1),
        NamedId::I32(2),
        NamedId::Text("job-3".to_owned()),
        NamedId::Bytes(vec![4]),
    ];
    for id in ids {
        assert_eq!(id.clone(), id);
    }
}

#[test]
fn a_row_whose_columns_hold_its_structs_types_stays_a_row() -> Result<(), Error> {
    let mut claimed = Claimed::Row(row::<()>(FITTING));
    hold_to(KINDS, &mut claimed)?;
    assert!(matches!(claimed, Claimed::Row(_)));
    // A null key fits whatever its column's type.
    let mut keyless = Claimed::Row(row::<()>(Fits {
        key: u8::MAX,
        ..FITTING
    }));
    hold_to(KINDS, &mut keyless)?;
    assert!(matches!(keyless, Claimed::Row(_)));
    Ok(())
}

#[test]
fn a_column_that_does_not_hold_its_structs_type_sends_the_row_to_the_policy() -> Result<(), Error> {
    let text_payload = Fits {
        payload: BytesKind::Text.bit(),
        ..FITTING
    };
    let byte_key = Fits {
        key: BytesKind::Bytes.bit(),
        ..FITTING
    };
    let wide_attempt = Fits {
        attempt: IntKind::I64.bit(),
        ..FITTING
    };
    let text_payload_wide_attempt = Fits {
        attempt: IntKind::I64.bit(),
        ..text_payload
    };
    // The attempt goes along, so the cap spends the row, unless its own column is one the
    // struct does not read: the struct's `FromRow` reads no attempt from it either.
    for (fits, column, attempt) in [
        (text_payload, "\"payload\"", Some(2)),
        (byte_key, "\"partition_key\"", Some(2)),
        (wide_attempt, "\"attempt\"", None),
        (text_payload_wide_attempt, "\"payload\"", None),
    ] {
        let mut claimed = Claimed::Row(row::<()>(fits));
        hold_to(KINDS, &mut claimed)?;
        assert_eq!(
            unfit_column(&claimed),
            Some((column.to_owned(), attempt)),
            "{fits:?}"
        );
    }
    Ok(())
}

#[test]
fn an_id_that_does_not_hold_its_structs_type_fails_the_claim() {
    let mut claimed = Claimed::Row(row::<()>(Fits {
        id: IdKind::I32.bit(),
        ..FITTING
    }));
    let failed = hold_to(KINDS, &mut claimed);
    assert!(
        matches!(&failed, Err(Error::ColumnDecode { index, .. }) if index == "\"id\""),
        "{failed:?}"
    );
}

#[test]
fn a_row_already_undecodable_is_left_to_the_policy() -> Result<(), Error> {
    let mut claimed = Claimed::<NamedRow<()>>::Undecodable {
        id: NamedId::I64(7),
        attempt: Some(3),
        error: Box::new(undecodable(Error::ColumnNotFound("payload".to_owned()))),
    };
    hold_to(KINDS, &mut claimed)?;
    assert!(matches!(
        &claimed,
        Claimed::Undecodable { attempt: Some(3), error, .. }
            if matches!(driver_error(error), Some(Error::ColumnNotFound(_)))
    ));
    Ok(())
}

#[cfg(all(feature = "postgres", feature = "chrono"))]
mod on_postgres {
    use std::time::Duration;

    use chrono::{DateTime, TimeDelta, Utc};
    use ruststream::HeaderMap;
    use ruststream_sqlx_dialect::{Column, Form, Param, TableSpec};
    use sqlx::postgres::PgArguments;
    use sqlx::{Arguments, Postgres};

    use super::super::{NamedId, NamedRow, NamedTime};
    use super::{FITTING, KINDS, row};
    use crate::inbox::engine::{Event, Events, IdAt, Leasing, Now, Prepared, Shape, Values};
    use crate::inbox::named::kinds::{ClockKind, Kinds, TimeKind};
    use crate::inbox::queue::Queue;
    use crate::inbox::time::QueueTime;
    use crate::inbox::{BuiltIn, PayloadRow};

    /// The row of a by-name subscription on Postgres, read and bound by its built-in dialect.
    type Named = NamedRow<BuiltIn<Postgres>>;

    const SPEC: TableSpec<'static> =
        TableSpec::new("email_jobs", Column::new("job_id"), Form::RowLock)
            .payload(Column::new("payload"));

    /// The subscription `emails` of a table read with `kinds`.
    fn queue(kinds: Option<Kinds>) -> &'static Queue {
        Box::leak(Box::new(Queue {
            name: "emails",
            table: "email_jobs",
            row: "SendEmail",
            spec: SPEC,
            id_at: IdAt::First,
            native_retry_after: true,
            kinds,
            prepared: Prepared::default(),
            begin_claim: None,
            counted_attempt: false,
            poll_interval: Duration::from_secs(1),
            lease: None,
            cap: None,
        }))
    }

    /// The subscription `emails` of a lease table read with `kinds`, with a lease of thirty
    /// seconds.
    fn leased(kinds: Option<Kinds>) -> &'static Queue {
        Box::leak(Box::new(Queue {
            spec: TableSpec::new(
                "email_jobs",
                Column::new("job_id"),
                Form::Lease(Column::new("locked_until")),
            )
            .payload(Column::new("payload")),
            lease: Some(Duration::from_secs(30)),
            ..*queue(kinds)
        }))
    }

    /// Binds `param` for `event`, and says whether it bound and how many values are bound.
    fn bind(
        param: Param,
        event: Event,
        queue: &'static Queue,
        id: Option<&NamedId>,
    ) -> Result<(bool, usize), sqlx::Error> {
        bind_leased(param, event, queue, id, None)
    }

    /// Binds `param` for `event` where the statement takes `leasing`, and writes and matches
    /// its expiry.
    fn bind_leased(
        param: Param,
        event: Event,
        queue: &'static Queue,
        id: Option<&NamedId>,
        leasing: Option<&Leasing<NamedTime>>,
    ) -> Result<(bool, usize), sqlx::Error> {
        let lease = leasing.map(|leasing| leasing.expiry);
        let values = Values {
            event,
            queue,
            limit: 10,
            id,
            ids: &[],
            delay: Duration::from_secs(30),
            destination: "emails.dead",
            now: Now::default(),
            lease,
            leasing,
            held: lease,
            key: None,
        };
        let mut arguments = PgArguments::default();
        let bound = <Named as Events<Postgres>>::bind(param, &mut arguments, &values)?;
        Ok((bound, arguments.len()))
    }

    #[test]
    fn a_lock_key_binds_as_text_where_the_statement_names_one() -> Result<(), sqlx::Error> {
        let queue = queue(Some(KINDS));
        let bind_key = |event, key| {
            let values = Values {
                event,
                queue,
                limit: 10,
                id: None,
                ids: &[],
                delay: Duration::ZERO,
                destination: "",
                now: Now::default(),
                lease: None,
                leasing: None,
                held: None,
                key,
            };
            let mut arguments = PgArguments::default();
            let bound = <Named as Events<Postgres>>::bind(Param::Key, &mut arguments, &values)?;
            Ok::<_, sqlx::Error>((bound, arguments.len()))
        };
        assert_eq!(bind_key(Event::Lock, Some("email_jobs-7"))?, (true, 1));
        assert_eq!(bind_key(Event::Unlock, Some("email_jobs-7"))?, (true, 1));
        // A statement outside the lock and the unlock has no key to bind.
        assert_eq!(bind_key(Event::Ack, None)?, (false, 0));
        Ok(())
    }

    #[test]
    fn a_lease_is_taken_and_bound_in_the_type_of_its_column() -> Result<(), sqlx::Error> {
        let queue = leased(Some(Kinds {
            locked_until: Some(TimeKind::Chrono),
            ..KINDS
        }));
        let before = Utc::now();
        let lease = <Named as Events<Postgres>>::lease(queue, Now::default())?;
        assert!(
            matches!(
                lease,
                Leasing {
                    at,
                    now: NamedTime::Chrono(now),
                    expiry: NamedTime::Chrono(expiry),
                } if now == DateTime::<Utc>::from(at)
                    && expiry.timestamp_subsec_nanos() == 0
                    && expiry >= before + TimeDelta::seconds(30)
                    && expiry == now.after(Duration::from_secs(30)).rounded_up()
            ),
            "a whole second, a lease from the claim's now: {lease:?}"
        );
        for param in [Param::Lease, Param::Held, Param::LeaseNow] {
            assert_eq!(
                bind_leased(param, Event::Extend, queue, None, Some(&lease))?,
                (true, 1),
                "{param:?}"
            );
        }
        // A claim holds no lease yet, and a statement outside the lease form writes none.
        assert_eq!(
            bind_leased(Param::Held, Event::Claim, queue, None, None)?,
            (false, 0)
        );
        // A queue whose kinds name no lease cannot tell one.
        let refused = <Named as Events<Postgres>>::lease(leased(Some(KINDS)), Now::default());
        assert!(
            matches!(refused, Err(sqlx::Error::Configuration(_))),
            "{refused:?}"
        );
        assert_eq!(
            bind(Param::LeaseNow, Event::Claim, leased(Some(KINDS)), None)?,
            (false, 0)
        );
        Ok(())
    }

    fn timed(clock: ClockKind) -> Kinds {
        Kinds {
            clock,
            retry_after: Some(TimeKind::Chrono),
            processed_at: Some(TimeKind::Chrono),
            ..KINDS
        }
    }

    #[test]
    fn every_id_binds_as_its_column_holds_it() -> Result<(), sqlx::Error> {
        let queue = queue(Some(KINDS));
        for id in [
            NamedId::I16(1),
            NamedId::I32(2),
            NamedId::I64(3),
            NamedId::Text("job-4".to_owned()),
            NamedId::Bytes(vec![5]),
        ] {
            assert_eq!(bind(Param::Id, Event::Ack, queue, Some(&id))?, (true, 1));
        }
        assert_eq!(bind(Param::Id, Event::Ack, queue, None)?, (false, 0));
        Ok(())
    }

    #[test]
    fn the_queue_binds_its_name_limit_destination_and_delay() -> Result<(), sqlx::Error> {
        let queue = queue(Some(KINDS));
        assert_eq!(bind(Param::Group, Event::Claim, queue, None)?, (true, 1));
        assert_eq!(bind(Param::Limit, Event::Claim, queue, None)?, (true, 1));
        assert_eq!(
            bind(Param::Destination, Event::DeadLetter, queue, None)?,
            (true, 1)
        );
        assert_eq!(
            bind(Param::Destination, Event::Ack, queue, None)?,
            (false, 0)
        );
        assert_eq!(
            bind(Param::Delay, Event::RetryAfter, queue, None)?,
            (true, 1)
        );
        assert_eq!(bind(Param::Ids, Event::Fetch, queue, None)?, (false, 0));
        Ok(())
    }

    #[test]
    fn times_bind_on_the_hosts_clock_in_their_columns_type() -> Result<(), sqlx::Error> {
        let host = queue(Some(timed(ClockKind::System)));
        assert_eq!(bind(Param::Now, Event::Claim, host, None)?, (true, 1));
        // The take of an advisory claim checks the row is still due, as the claim does.
        assert_eq!(bind(Param::Now, Event::Take, host, None)?, (true, 1));
        assert_eq!(bind(Param::Now, Event::Ack, host, None)?, (true, 1));
        assert_eq!(bind(Param::Now, Event::Discard, host, None)?, (true, 1));
        assert_eq!(
            bind(Param::RetryAfter, Event::RetryAfter, host, None)?,
            (true, 1)
        );
        // The database's clock is read by the statement itself; a table without the column,
        // or a queue that never described its kinds, has no time to bind.
        let database = queue(Some(timed(ClockKind::Database)));
        assert_eq!(bind(Param::Now, Event::Claim, database, None)?, (false, 0));
        let untimed = queue(Some(KINDS));
        assert_eq!(bind(Param::Now, Event::Ack, untimed, None)?, (false, 0));
        assert_eq!(
            bind(Param::Now, Event::Claim, queue(None), None)?,
            (false, 0)
        );
        Ok(())
    }

    #[cfg(feature = "time")]
    #[test]
    fn time_crate_columns_bind_too() -> Result<(), sqlx::Error> {
        let kinds = Some(Kinds {
            retry_after: Some(TimeKind::Time),
            ..KINDS
        });
        assert_eq!(
            bind(Param::RetryAfter, Event::RetryAfter, queue(kinds), None)?,
            (true, 1)
        );
        Ok(())
    }

    #[test]
    fn a_named_row_lends_its_parts_and_leaves_every_event_to_the_crate() {
        let mut row = row(FITTING);
        assert_eq!(row.payload(), b"{}");
        assert_eq!(<Named as Events<Postgres>>::id(&row), &NamedId::I64(7));
        assert_eq!(
            <Named as Events<Postgres>>::partition_key(&row),
            Some(b"acme".as_slice())
        );
        assert_eq!(<Named as Events<Postgres>>::attempt(&row), Some(2));
        let mut headers = HeaderMap::new();
        headers.insert("x-tenant", "acme");
        row.headers = headers.clone();
        // The delivery takes the headers once: they move out of the row.
        assert_eq!(<Named as Events<Postgres>>::take_headers(&mut row), headers);
        assert!(<Named as Events<Postgres>>::take_headers(&mut row).is_empty());
        assert_eq!(<Named as Events<Postgres>>::SHAPE, Shape::default());
        assert_eq!(<Named as Events<Postgres>>::kinds(), None);
        assert_eq!(
            <Named as Events<Postgres>>::unfit_header(&headers),
            Some("x-tenant")
        );
    }
}
