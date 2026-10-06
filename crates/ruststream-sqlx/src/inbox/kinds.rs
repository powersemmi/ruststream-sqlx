//! Which column types a by-name subscription reads and binds from the table description alone.

use std::any::TypeId;
#[cfg(feature = "json")]
use std::collections::BTreeMap;

#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
#[cfg(feature = "json")]
use sqlx::types::Json;
#[cfg(feature = "time")]
use time::OffsetDateTime;

use super::time::{DatabaseClock, SystemClock};

/// The column types of a row a by-name subscription reads and binds without the row's own code.
/// Machinery; the derive answers it once per subscription.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Kinds {
    /// Where "now" comes from.
    pub(crate) clock: ClockKind,
    /// The type the struct reads the id as.
    pub(crate) id: IdKind,
    /// The type the struct reads the payload as.
    pub(crate) payload: BytesKind,
    /// The type the struct reads the key as, where the table has one.
    pub(crate) key: Option<BytesKind>,
    /// The type the struct reads the attempt as, where the table has one.
    pub(crate) attempt: Option<IntKind>,
    /// The type of the `retry_after` column, where the table has one.
    pub(crate) retry_after: Option<TimeKind>,
    /// The type of the `processed_at` column, where the table has one.
    pub(crate) processed_at: Option<TimeKind>,
    /// The type a lease is written in, in the lease form.
    pub(crate) locked_until: Option<TimeKind>,
}

/// Where a described row's "now" comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ClockKind {
    /// The host's clock: the statements bind it.
    System,
    /// The database's own clock: the statements read it and bind no time.
    Database,
}

/// The type a struct reads its id as, in the order a by-name subscription tries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IdKind {
    I64,
    I32,
    I16,
    Text,
    Bytes,
}

/// The type a struct reads a payload or a key as, in the order a by-name subscription tries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BytesKind {
    Bytes,
    Text,
}

/// The type a struct reads its attempt as, in the order a by-name subscription tries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IntKind {
    I64,
    I32,
    I16,
}

/// The type a time column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TimeKind {
    /// `chrono::DateTime<Utc>`.
    #[cfg(feature = "chrono")]
    Chrono,
    /// `time::OffsetDateTime`.
    #[cfg(feature = "time")]
    Time,
}

impl IdKind {
    /// The kind's bit among the types a column holds, as `RoleColumns::id` reports them.
    pub(crate) const fn bit(self) -> u8 {
        1 << self as u8
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::I64 => "i64",
            Self::I32 => "i32",
            Self::I16 => "i16",
            Self::Text => "String",
            Self::Bytes => "Vec<u8>",
        }
    }
}

impl BytesKind {
    /// The kind's bit among the types a column holds, as `RoleColumns::bytes` reports them.
    pub(crate) const fn bit(self) -> u8 {
        1 << self as u8
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Bytes => "bytes",
            Self::Text => "text",
        }
    }
}

impl IntKind {
    /// The kind's bit among the types a column holds, as `RoleColumns::attempt` reports them.
    pub(crate) const fn bit(self) -> u8 {
        1 << self as u8
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::I64 => "i64",
            Self::I32 => "i32",
            Self::I16 => "i16",
        }
    }
}

/// Builds [`Kinds`] from a row's field types, refusing a type the described path cannot read or
/// bind. Machinery; each step compares type ids once, when a subscription opens.
#[doc(hidden)]
#[derive(Debug)]
#[must_use]
pub struct KindsOf(Option<Partial>);

/// The kinds read so far; the payload's comes last of the required ones.
#[derive(Debug, Clone, Copy)]
struct Partial {
    clock: ClockKind,
    id: IdKind,
    payload: Option<BytesKind>,
    key: Option<BytesKind>,
    attempt: Option<IntKind>,
    retry_after: Option<TimeKind>,
    processed_at: Option<TimeKind>,
    locked_until: Option<TimeKind>,
}

impl KindsOf {
    /// A row whose id is an `i16`, `i32`, `i64`, `String` or `Vec<u8>`, and whose "now" comes from
    /// [`SystemClock`] or [`DatabaseClock`].
    pub fn new<Id: 'static, Clock: 'static>() -> Self {
        let id = kind_of::<Id, _>(&[
            (TypeId::of::<i64>(), IdKind::I64),
            (TypeId::of::<i32>(), IdKind::I32),
            (TypeId::of::<i16>(), IdKind::I16),
            (TypeId::of::<String>(), IdKind::Text),
            (TypeId::of::<Vec<u8>>(), IdKind::Bytes),
        ]);
        let clock = if is::<Clock, SystemClock>() {
            Some(ClockKind::System)
        } else if is::<Clock, DatabaseClock>() {
            Some(ClockKind::Database)
        } else {
            None
        };
        Self(id.zip(clock).map(|(id, clock)| Partial {
            clock,
            id,
            payload: None,
            key: None,
            attempt: None,
            retry_after: None,
            processed_at: None,
            locked_until: None,
        }))
    }

    /// A payload of bytes or text.
    pub fn payload<T: 'static>(self) -> Self {
        let kind = kind_of::<T, _>(&[
            (TypeId::of::<Vec<u8>>(), BytesKind::Bytes),
            (TypeId::of::<String>(), BytesKind::Text),
        ]);
        self.with(kind, |partial, kind| Partial {
            payload: Some(kind),
            ..partial
        })
    }

    /// Headers kept as a JSON object of strings, which may be null.
    pub fn headers<T: 'static>(self) -> Self {
        #[cfg(feature = "json")]
        let kind = kind_of::<T, _>(&[
            (TypeId::of::<Json<BTreeMap<String, String>>>(), ()),
            (TypeId::of::<Option<Json<BTreeMap<String, String>>>>(), ()),
        ]);
        #[cfg(not(feature = "json"))]
        let kind = None;
        self.with(kind, |partial, ()| partial)
    }

    /// A key of text or bytes, which may be null.
    pub fn partition_key<T: 'static>(self) -> Self {
        let kind = kind_of::<T, _>(&[
            (TypeId::of::<String>(), BytesKind::Text),
            (TypeId::of::<Box<str>>(), BytesKind::Text),
            (TypeId::of::<Vec<u8>>(), BytesKind::Bytes),
            (TypeId::of::<Box<[u8]>>(), BytesKind::Bytes),
            (TypeId::of::<Option<String>>(), BytesKind::Text),
            (TypeId::of::<Option<Box<str>>>(), BytesKind::Text),
            (TypeId::of::<Option<Vec<u8>>>(), BytesKind::Bytes),
            (TypeId::of::<Option<Box<[u8]>>>(), BytesKind::Bytes),
        ]);
        self.with(kind, |partial, kind| Partial {
            key: Some(kind),
            ..partial
        })
    }

    /// An integer attempt.
    pub fn attempt<T: 'static>(self) -> Self {
        let kind = kind_of::<T, _>(&[
            (TypeId::of::<i64>(), IntKind::I64),
            (TypeId::of::<i32>(), IntKind::I32),
            (TypeId::of::<i16>(), IntKind::I16),
        ]);
        self.with(kind, |partial, kind| Partial {
            attempt: Some(kind),
            ..partial
        })
    }

    /// The time a `retry_after` column holds.
    pub fn retry_after<T: 'static>(self) -> Self {
        self.with(time::<T>(), |partial, kind| Partial {
            retry_after: Some(kind),
            ..partial
        })
    }

    /// The time a `processed_at` column holds.
    pub fn processed_at<T: 'static>(self) -> Self {
        self.with(time::<T>(), |partial, kind| Partial {
            processed_at: Some(kind),
            ..partial
        })
    }

    /// The time a lease is written in, in the `locked_until` column.
    pub fn locked_until<T: 'static>(self) -> Self {
        self.with(time::<T>(), |partial, kind| Partial {
            locked_until: Some(kind),
            ..partial
        })
    }

    /// The kinds, or `None` when a type was refused or the row has no payload.
    #[must_use]
    pub fn finish(self) -> Option<Kinds> {
        let partial = self.0?;
        Some(Kinds {
            clock: partial.clock,
            id: partial.id,
            payload: partial.payload?,
            key: partial.key,
            attempt: partial.attempt,
            retry_after: partial.retry_after,
            processed_at: partial.processed_at,
            locked_until: partial.locked_until,
        })
    }

    /// The builder with `kind` recorded by `record`, or refused where the type had no kind.
    fn with<Kind>(self, kind: Option<Kind>, record: impl FnOnce(Partial, Kind) -> Partial) -> Self {
        Self(
            self.0
                .zip(kind)
                .map(|(partial, kind)| record(partial, kind)),
        )
    }
}

/// The kind `T` is among `kinds`, by type id.
fn kind_of<T: 'static, Kind: Copy>(kinds: &[(TypeId, Kind)]) -> Option<Kind> {
    let id = TypeId::of::<T>();
    kinds
        .iter()
        .find_map(|&(of, kind)| (of == id).then_some(kind))
}

/// Whether `T` is `Other`.
fn is<T: 'static, Other: 'static>() -> bool {
    TypeId::of::<T>() == TypeId::of::<Other>()
}

/// The kind of the time `T`, if the described path binds it.
fn time<T: 'static>() -> Option<TimeKind> {
    kind_of::<T, _>(&[
        #[cfg(feature = "chrono")]
        (TypeId::of::<DateTime<Utc>>(), TimeKind::Chrono),
        #[cfg(feature = "time")]
        (TypeId::of::<OffsetDateTime>(), TimeKind::Time),
    ])
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{BytesKind, ClockKind, IdKind, IntKind, Kinds, KindsOf};
    use crate::inbox::time::{Clock, DatabaseClock, SystemClock};

    /// A clock of the service's own: the described path cannot read it.
    struct Office;

    impl Clock for Office {
        fn now() -> SystemTime {
            UNIX_EPOCH
        }
    }

    /// The kinds of a row of an `i64` id and a byte payload on the host's clock.
    const PLAIN: Kinds = Kinds {
        clock: ClockKind::System,
        id: IdKind::I64,
        payload: BytesKind::Bytes,
        key: None,
        attempt: None,
        retry_after: None,
        processed_at: None,
        locked_until: None,
    };

    fn plain() -> KindsOf {
        KindsOf::new::<i64, SystemClock>().payload::<Vec<u8>>()
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn a_row_of_known_types_yields_its_kinds() {
        use chrono::{DateTime, Utc};

        use super::TimeKind;

        let kinds = plain().retry_after::<DateTime<Utc>>().finish();
        assert_eq!(
            kinds,
            Some(Kinds {
                retry_after: Some(TimeKind::Chrono),
                ..PLAIN
            })
        );
        let leased = plain().locked_until::<DateTime<Utc>>().finish();
        assert_eq!(
            leased,
            Some(Kinds {
                locked_until: Some(TimeKind::Chrono),
                ..PLAIN
            })
        );
        let marked = KindsOf::new::<String, DatabaseClock>()
            .payload::<String>()
            .processed_at::<DateTime<Utc>>()
            .finish();
        assert_eq!(
            marked,
            Some(Kinds {
                clock: ClockKind::Database,
                id: IdKind::Text,
                payload: BytesKind::Text,
                processed_at: Some(TimeKind::Chrono),
                ..PLAIN
            })
        );
    }

    #[test]
    fn a_type_outside_the_kinds_turns_the_row_away() {
        assert_eq!(
            KindsOf::new::<u64, SystemClock>()
                .payload::<Vec<u8>>()
                .finish(),
            None
        );
        assert_eq!(
            KindsOf::new::<i64, Office>().payload::<Vec<u8>>().finish(),
            None
        );
        assert_eq!(plain().retry_after::<SystemTime>().finish(), None);
        assert_eq!(plain().processed_at::<SystemTime>().finish(), None);
        assert_eq!(plain().locked_until::<SystemTime>().finish(), None);
        assert_eq!(
            KindsOf::new::<i64, SystemClock>()
                .payload::<Box<[u8]>>()
                .finish(),
            None
        );
        assert_eq!(plain().attempt::<u32>().finish(), None);
        assert_eq!(plain().partition_key::<i64>().finish(), None);
        assert_eq!(plain().headers::<String>().finish(), None);
        // The payload is what a delivery hands the handler: a row without one has no kinds.
        assert_eq!(KindsOf::new::<i64, SystemClock>().finish(), None);
    }

    #[test]
    fn every_id_payload_key_and_attempt_kind_is_read() {
        assert_eq!(
            KindsOf::new::<i64, DatabaseClock>()
                .payload::<Vec<u8>>()
                .finish(),
            Some(Kinds {
                clock: ClockKind::Database,
                ..PLAIN
            })
        );
        let ids = [
            (KindsOf::new::<i16, SystemClock>(), IdKind::I16),
            (KindsOf::new::<i32, SystemClock>(), IdKind::I32),
            (KindsOf::new::<String, SystemClock>(), IdKind::Text),
            (KindsOf::new::<Vec<u8>, SystemClock>(), IdKind::Bytes),
        ];
        for (row, id) in ids {
            assert_eq!(
                row.payload::<Vec<u8>>().finish(),
                Some(Kinds { id, ..PLAIN })
            );
        }
        let keys = [
            (plain().partition_key::<String>(), BytesKind::Text),
            (plain().partition_key::<Option<Box<str>>>(), BytesKind::Text),
            (plain().partition_key::<Vec<u8>>(), BytesKind::Bytes),
            (
                plain().partition_key::<Option<Box<[u8]>>>(),
                BytesKind::Bytes,
            ),
        ];
        for (row, key) in keys {
            assert_eq!(
                row.finish(),
                Some(Kinds {
                    key: Some(key),
                    ..PLAIN
                })
            );
        }
        let attempts = [
            (plain().attempt::<i16>(), IntKind::I16),
            (plain().attempt::<i32>(), IntKind::I32),
            (plain().attempt::<i64>(), IntKind::I64),
        ];
        for (row, attempt) in attempts {
            assert_eq!(
                row.finish(),
                Some(Kinds {
                    attempt: Some(attempt),
                    ..PLAIN
                })
            );
        }
    }

    #[test]
    fn each_kind_names_its_type_and_owns_one_bit() {
        let ids = [
            IdKind::I64,
            IdKind::I32,
            IdKind::I16,
            IdKind::Text,
            IdKind::Bytes,
        ];
        let names: Vec<_> = ids.iter().map(|kind| kind.name()).collect();
        assert_eq!(names, ["i64", "i32", "i16", "String", "Vec<u8>"]);
        let bits: Vec<_> = ids.iter().map(|kind| kind.bit()).collect();
        assert_eq!(bits, [1, 2, 4, 8, 16]);
        assert_eq!(
            [BytesKind::Bytes.bit(), BytesKind::Text.bit()],
            [1, 2],
            "bytes are tried first"
        );
        assert_eq!(
            [BytesKind::Bytes.name(), BytesKind::Text.name()],
            ["bytes", "text"]
        );
        assert_eq!(
            [IntKind::I64, IntKind::I32, IntKind::I16].map(IntKind::bit),
            [1, 2, 4]
        );
        assert_eq!(
            [IntKind::I64, IntKind::I32, IntKind::I16].map(IntKind::name),
            ["i64", "i32", "i16"]
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_headers_are_read_with_or_without_a_null() {
        use std::collections::BTreeMap;

        use sqlx::types::Json;

        assert_eq!(
            plain().headers::<Json<BTreeMap<String, String>>>().finish(),
            Some(PLAIN)
        );
        assert_eq!(
            plain()
                .headers::<Option<Json<BTreeMap<String, String>>>>()
                .finish(),
            Some(PLAIN)
        );
    }

    #[cfg(feature = "time")]
    #[test]
    fn time_columns_of_the_time_crate_are_read() {
        use time::OffsetDateTime;

        use super::TimeKind;

        let kinds = plain()
            .retry_after::<OffsetDateTime>()
            .processed_at::<OffsetDateTime>()
            .locked_until::<OffsetDateTime>()
            .finish();
        assert_eq!(
            kinds,
            Some(Kinds {
                retry_after: Some(TimeKind::Time),
                processed_at: Some(TimeKind::Time),
                locked_until: Some(TimeKind::Time),
                ..PLAIN
            })
        );
    }
}
