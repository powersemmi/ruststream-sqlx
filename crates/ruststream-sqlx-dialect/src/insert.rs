//! The insert of a row into a queue table, for each built-in dialect, written by a `const fn`.
//!
//! [`postgres`], [`mysql`] and [`sqlite`] turn a [`TableSpec`] into the text of its insert while
//! the service compiles: every column the database does not fill, in [`TableSpec::columns`]
//! order, each bound by the next placeholder. The built-in dialects' [`Dialect::insert`], and
//! with it the insert `#[derive(Inbox)]` generates, take their text from the same writer, so a
//! table described by hand inserts with the same statement as a derived one.
//!
//! The text lands in a [`Sql<N>`]: `N` bytes the caller chooses, a constant like the description
//! it renders. A capacity too small for the text, a description read with `*` and a name longer
//! than the database keeps are refused with a panic, which in a `const` is a build error naming
//! the table.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "postgres")]
//! # fn main() {
//! use ruststream_sqlx_dialect::insert::{self, Sql};
//! use ruststream_sqlx_dialect::{Column, Form, TableSpec};
//!
//! const EMAILS: TableSpec<'static> = TableSpec::new(
//!     "email_jobs",
//!     Column::new("job_id").generated(),
//!     Form::Lease(Column::new("locked_until")),
//! )
//! .group(Column::new("name"))
//! .payload(Column::new("payload"))
//! .data(&[Column::new("recipient")]);
//!
//! // The binds follow `TableSpec::columns`: the roles, then the data columns.
//! const INSERT: Sql<128> = insert::postgres(&EMAILS);
//!
//! assert_eq!(
//!     INSERT.as_str(),
//!     r#"INSERT INTO "email_jobs" ("name", "locked_until", "payload", "recipient") VALUES ($1, $2, $3, $4)"#,
//! );
//! # }
//! # #[cfg(not(feature = "postgres"))]
//! # fn main() {}
//! ```
//!
//! [`Dialect::insert`]: crate::Dialect::insert

use core::fmt::{self, Debug, Display, Formatter};

use crate::column::Column;
use crate::spec::{Columns, TableSpec};
use crate::statement::{NameLimit, Param, Statement, StatementError};

#[cfg(test)]
mod tests;

/// How a built-in database spells an insert.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Spelling {
    /// The dialect's name, as [`Dialect::name`](crate::Dialect::name) gives it.
    pub(crate) dialect: &'static str,
    /// The character that quotes a name; one inside the name is doubled.
    pub(crate) quote: u8,
    /// How a placeholder reads.
    pub(crate) placeholder: Placeholder,
    /// What follows the table in an insert that writes no column, so every column takes its
    /// default.
    pub(crate) default_row: &'static str,
    /// The longest name the database keeps; `None` where it keeps a name of any length.
    pub(crate) name_limit: Option<NameLimit>,
}

/// How a placeholder reads.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Placeholder {
    /// `?`, bound by position.
    #[cfg(any(feature = "mysql", feature = "sqlite"))]
    Question,
    /// `$1`, `$2`, ..: numbered from one.
    #[cfg(feature = "postgres")]
    Dollar,
}

/// Appends `ident` to `out`, quoted with `quote`, which doubles inside the name.
pub(crate) fn quote_into(quote: u8, ident: &str, out: &mut String) {
    let quote = char::from(quote);
    out.push(quote);
    for character in ident.chars() {
        if character == quote {
            out.push(quote);
        }
        out.push(character);
    }
    out.push(quote);
}

/// Whether `name` fits in `limit`, measured the way the database measures it.
pub(crate) const fn fits(name: &str, limit: NameLimit) -> bool {
    match limit {
        NameLimit::Bytes(most) => name.len() <= most as usize,
        NameLimit::Characters(most) => characters(name) <= most as usize,
    }
}

/// The characters of `text`: its bytes that start one.
const fn characters(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut count = 0;
    let mut index = 0;
    while index < bytes.len() {
        // A UTF-8 continuation byte reads `0b10xx_xxxx`.
        if bytes[index] & 0b1100_0000 != 0b1000_0000 {
            count += 1;
        }
        index += 1;
    }
    count
}

/// The first name of `spec` longer than `limit` allows: its schema, its table, then its columns
/// in [`TableSpec::columns`] order.
pub(crate) const fn too_long<'a>(spec: &TableSpec<'a>, limit: NameLimit) -> Option<&'a str> {
    if let Some(schema) = spec.schema()
        && !fits(schema, limit)
    {
        return Some(schema);
    }
    if !fits(spec.table(), limit) {
        return Some(spec.table());
    }
    let mut columns = Columns::new(spec);
    while let Some(column) = columns.next_column() {
        if !fits(column.name(), limit) {
            return Some(column.name());
        }
    }
    None
}

/// Why a description has no insert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal<'a> {
    /// The description reads the table with `*`, so its columns are not all known.
    Flattened,
    /// A name is longer than the database keeps.
    TooLong(&'a str, NameLimit),
}

impl Refusal<'_> {
    /// The error [`Dialect::insert`](crate::Dialect::insert) returns for it.
    fn into_error(self, dialect: &'static str) -> StatementError {
        match self {
            Self::Flattened => StatementError::Flattened {
                statement: "insert",
            },
            Self::TooLong(identifier, limit) => StatementError::IdentifierTooLong {
                dialect,
                identifier: identifier.to_owned(),
                limit,
            },
        }
    }
}

/// Why `spec` has no insert in `spelling`, or `None` when it has one.
const fn refusal<'a>(spec: &TableSpec<'a>, spelling: Spelling) -> Option<Refusal<'a>> {
    if let Some(limit) = spelling.name_limit
        && let Some(name) = too_long(spec, limit)
    {
        return Some(Refusal::TooLong(name, limit));
    }
    if spec.selects_all() {
        return Some(Refusal::Flattened);
    }
    None
}

/// Text written into a buffer by a `const fn`. The length counts every byte written, those past
/// the buffer's end included, so a pass over an empty buffer measures the text.
struct Text<'b> {
    out: &'b mut [u8],
    len: usize,
}

impl<'b> Text<'b> {
    const fn new(out: &'b mut [u8]) -> Self {
        Self { out, len: 0 }
    }

    const fn byte(&mut self, byte: u8) {
        if self.len < self.out.len() {
            self.out[self.len] = byte;
        }
        self.len += 1;
    }

    const fn push(&mut self, text: &str) {
        let bytes = text.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            self.byte(bytes[index]);
            index += 1;
        }
    }

    /// `name`, quoted with `quote`, which doubles inside the name.
    const fn ident(&mut self, quote: u8, name: &str) {
        self.byte(quote);
        let bytes = name.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] == quote {
                self.byte(quote);
            }
            self.byte(bytes[index]);
            index += 1;
        }
        self.byte(quote);
    }

    /// `number` in decimal.
    const fn number(&mut self, number: usize) {
        let mut unit = 1;
        while number / unit >= 10 {
            unit *= 10;
        }
        while unit > 0 {
            // A decimal digit is below ten, so it fits a byte.
            #[allow(clippy::cast_possible_truncation)]
            self.byte(b'0' + (number / unit % 10) as u8);
            unit /= 10;
        }
    }

    /// The text written so far, cut at the last whole character that fits the buffer.
    const fn as_str(&self) -> &str {
        let written = if self.len < self.out.len() {
            self.len
        } else {
            self.out.len()
        };
        let (text, _) = self.out.split_at(written);
        prefix(text)
    }
}

/// The longest prefix of `bytes` that is UTF-8.
const fn prefix(bytes: &[u8]) -> &str {
    match core::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(error) => {
            let (valid, _) = bytes.split_at(error.valid_up_to());
            match core::str::from_utf8(valid) {
                Ok(text) => text,
                Err(_) => panic!("a prefix up to `valid_up_to` is UTF-8"),
            }
        }
    }
}

/// Whether an insert writes `column`: the database fills a generated one.
const fn written(column: Column<'_>) -> bool {
    !column.is_generated()
}

/// Writes the insert of `spec` in `spelling`: every column the database does not fill.
const fn write(text: &mut Text<'_>, spec: &TableSpec<'_>, spelling: Spelling) {
    text.push("INSERT INTO ");
    if let Some(schema) = spec.schema() {
        text.ident(spelling.quote, schema);
        text.push(".");
    }
    text.ident(spelling.quote, spec.table());
    let mut columns = Columns::new(spec);
    let mut count = 0;
    while let Some(column) = columns.next_column() {
        if written(column) {
            text.push(if count == 0 { " (" } else { ", " });
            text.ident(spelling.quote, column.name());
            count += 1;
        }
    }
    if count == 0 {
        text.push(spelling.default_row);
        return;
    }
    text.push(") VALUES (");
    let mut index = 0;
    while index < count {
        if index > 0 {
            text.push(", ");
        }
        match spelling.placeholder {
            #[cfg(any(feature = "mysql", feature = "sqlite"))]
            Placeholder::Question => text.push("?"),
            #[cfg(feature = "postgres")]
            Placeholder::Dollar => {
                text.push("$");
                text.number(index + 1);
            }
        }
        index += 1;
    }
    text.push(")");
}

/// The insert of `spec` in `spelling` as a statement: its text, and the position in
/// [`TableSpec::columns`] of each column its placeholders bind, in order.
pub(crate) fn statement(
    spec: &TableSpec<'_>,
    spelling: Spelling,
) -> Result<Statement, StatementError> {
    if let Some(refusal) = refusal(spec, spelling) {
        return Err(refusal.into_error(spelling.dialect));
    }
    let mut measure = Text::new(&mut []);
    write(&mut measure, spec, spelling);
    let mut bytes = vec![0; measure.len];
    let mut text = Text::new(&mut bytes);
    write(&mut text, spec, spelling);
    let sql = text.as_str().to_owned();
    let params = spec
        .columns()
        .enumerate()
        .filter(|(_, column)| written(*column))
        .map(|(position, _)| Param::Column(position));
    Ok(Statement::new(sql, params))
}

/// The text of a statement rendered in a `const`: up to `N` bytes.
///
/// [`postgres`], [`mysql`] and [`sqlite`] fill one from a [`TableSpec`]. `N` is the caller's
/// choice; a text longer than `N` is refused with a panic that names `N`, the table and the length
/// the text needs, so in a `const` it is a build error that says what to raise `N` to.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "sqlite")]
/// # fn main() {
/// use ruststream_sqlx_dialect::insert::{self, Sql};
/// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
///
/// const MAIL: TableSpec<'static> = TableSpec::new(
///     "mail",
///     Column::new("id").generated(),
///     Form::Lease(Column::new("locked_until")),
/// )
/// .data(&[Column::new("recipient")]);
///
/// const INSERT: Sql<96> = insert::sqlite(&MAIL);
///
/// // A constant's text lives as long as the program, as a driver's prepared statement needs.
/// let sql: &'static str = INSERT.as_str();
/// assert_eq!(sql, "INSERT INTO `mail` (`locked_until`, `recipient`) VALUES (?, ?)");
/// # }
/// # #[cfg(not(feature = "sqlite"))]
/// # fn main() {}
/// ```
///
/// A capacity the text outgrows does not build:
///
/// ```compile_fail,E0080
/// # #[cfg(feature = "sqlite")]
/// # mod demo {
/// use ruststream_sqlx_dialect::insert::{self, Sql};
/// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
///
/// const MAIL: TableSpec<'static> = TableSpec::new("mail", Column::new("id"), Form::RowLock)
///     .data(&[Column::new("recipient")]);
///
/// // The insert into `mail` takes 52 bytes, more than `Sql<16>` holds.
/// pub const INSERT: Sql<16> = insert::sqlite(&MAIL);
/// # }
/// # fn main() {}
/// # #[cfg(not(feature = "sqlite"))]
/// # compile_error!("the example needs the `sqlite` feature");
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sql<const N: usize> {
    bytes: [u8; N],
    len: usize,
}

impl<const N: usize> Sql<N> {
    /// The insert of `spec` in `spelling`, refused with a panic where it has none or outgrows `N`.
    const fn insert(spec: &TableSpec<'_>, spelling: Spelling) -> Self {
        if let Some(refusal) = refusal(spec, spelling) {
            refuse(spec, spelling, refusal);
        }
        let mut bytes = [0; N];
        let mut text = Text::new(&mut bytes);
        write(&mut text, spec, spelling);
        let len = text.len;
        if len > N {
            outgrown(spec, N, len);
        }
        Self { bytes, len }
    }

    /// The text.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "mysql")]
    /// # fn main() {
    /// use ruststream_sqlx_dialect::insert::{self, Sql};
    /// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
    ///
    /// const JOBS: TableSpec<'static> =
    ///     TableSpec::new("jobs", Column::new("id"), Form::RowLock).payload(Column::new("payload"));
    /// const INSERT: Sql<64> = insert::mysql(&JOBS);
    ///
    /// assert_eq!(INSERT.as_str(), "INSERT INTO `jobs` (`id`, `payload`) VALUES (?, ?)");
    /// # }
    /// # #[cfg(not(feature = "mysql"))]
    /// # fn main() {}
    /// ```
    #[must_use]
    pub const fn as_str(&self) -> &str {
        // The text is whole names and ASCII, so the prefix is all of it.
        let (text, _) = self.bytes.split_at(self.len);
        prefix(text)
    }
}

impl<const N: usize> Debug for Sql<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Sql").field(&self.as_str()).finish()
    }
}

impl<const N: usize> Display for Sql<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<const N: usize> AsRef<str> for Sql<N> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// The capacity of a refusal's message; a longer one is cut at a whole character.
const MESSAGE: usize = 512;

/// Panics with `refusal`'s message.
const fn refuse(spec: &TableSpec<'_>, spelling: Spelling, refusal: Refusal<'_>) -> ! {
    let mut bytes = [0; MESSAGE];
    let mut text = Text::new(&mut bytes);
    match refusal {
        Refusal::Flattened => {
            text.push("the insert into `");
            text.push(spec.table());
            text.push(
                "` needs every column, and the description reads the table with `*` \
                 (`selecting_all`): write the insert in the service",
            );
        }
        Refusal::TooLong(name, limit) => {
            text.push("`");
            text.push(name);
            text.push("` is longer than the ");
            match limit {
                NameLimit::Bytes(most) => {
                    text.number(most as usize);
                    text.push(" bytes");
                }
                NameLimit::Characters(most) => {
                    text.number(most as usize);
                    text.push(" characters");
                }
            }
            text.push(" the ");
            text.push(spelling.dialect);
            text.push(" dialect allows in a name");
        }
    }
    panic!("{}", text.as_str())
}

/// Panics with the message of a text of `len` bytes that outgrows `Sql<capacity>`.
const fn outgrown(spec: &TableSpec<'_>, capacity: usize, len: usize) -> ! {
    let mut bytes = [0; MESSAGE];
    let mut text = Text::new(&mut bytes);
    text.push("the insert into `");
    text.push(spec.table());
    text.push("` takes ");
    text.number(len);
    text.push(" bytes, more than `Sql<");
    text.number(capacity);
    text.push(">` holds: raise `N` to ");
    text.number(len);
    panic!("{}", text.as_str())
}

/// The Postgres insert of `spec`: every column the database does not fill, in
/// [`TableSpec::columns`] order, bound as `$1`, `$2`, ..; `DEFAULT VALUES` when there is none.
///
/// # Panics
///
/// When the text outgrows `N`, when the description reads the table with `*`
/// ([`TableSpec::selecting_all`]), or when a name is longer than the 63 bytes Postgres keeps. In
/// a `const` the panic is a build error.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::insert::{self, Sql};
/// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
///
/// const ORDERS: TableSpec<'static> =
///     TableSpec::new("order_jobs", Column::new("job_id").generated(), Form::RowLock)
///         .within("app")
///         .payload(Column::new("payload"));
/// const INSERT: Sql<64> = insert::postgres(&ORDERS);
///
/// assert_eq!(INSERT.as_str(), r#"INSERT INTO "app"."order_jobs" ("payload") VALUES ($1)"#);
/// ```
#[cfg(feature = "postgres")]
#[must_use]
pub const fn postgres<const N: usize>(spec: &TableSpec<'_>) -> Sql<N> {
    Sql::insert(spec, crate::postgres::SPELLING)
}

/// The MySQL and MariaDB insert of `spec`: every column the database does not fill, in
/// [`TableSpec::columns`] order, bound as `?`; `() VALUES ()` when there is none.
///
/// # Panics
///
/// When the text outgrows `N`, when the description reads the table with `*`
/// ([`TableSpec::selecting_all`]), or when a name is longer than the 64 characters MySQL keeps.
/// In a `const` the panic is a build error.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::insert::{self, Sql};
/// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
///
/// const ORDERS: TableSpec<'static> =
///     TableSpec::new("order_jobs", Column::new("job_id").generated(), Form::RowLock)
///         .payload(Column::new("payload"));
/// const INSERT: Sql<64> = insert::mysql(&ORDERS);
///
/// assert_eq!(INSERT.as_str(), "INSERT INTO `order_jobs` (`payload`) VALUES (?)");
/// ```
#[cfg(feature = "mysql")]
#[must_use]
pub const fn mysql<const N: usize>(spec: &TableSpec<'_>) -> Sql<N> {
    Sql::insert(spec, crate::mysql::SPELLING)
}

/// The SQLite insert of `spec`: every column the database does not fill, in
/// [`TableSpec::columns`] order, bound as `?`; `DEFAULT VALUES` when there is none.
///
/// # Panics
///
/// When the text outgrows `N`, or when the description reads the table with `*`
/// ([`TableSpec::selecting_all`]). In a `const` the panic is a build error.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx_dialect::insert::{self, Sql};
/// use ruststream_sqlx_dialect::{Column, Form, TableSpec};
///
/// const ORDERS: TableSpec<'static> =
///     TableSpec::new("order_jobs", Column::new("job_id").generated(), Form::RowLock)
///         .payload(Column::new("payload"));
/// const INSERT: Sql<64> = insert::sqlite(&ORDERS);
///
/// assert_eq!(INSERT.as_str(), "INSERT INTO `order_jobs` (`payload`) VALUES (?)");
/// ```
#[cfg(feature = "sqlite")]
#[must_use]
pub const fn sqlite<const N: usize>(spec: &TableSpec<'_>) -> Sql<N> {
    Sql::insert(spec, crate::sqlite::SPELLING)
}
