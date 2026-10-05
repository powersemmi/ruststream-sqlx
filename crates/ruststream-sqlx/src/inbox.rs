//! The inbox: queue tables a service describes with its own structs.

use ruststream_sqlx_dialect::TableSpec;

/// A struct that describes a queue table; `#[derive(Inbox)]` implements it.
///
/// # Examples
///
/// ```
/// use ruststream_sqlx::{Inbox, InboxRow};
///
/// #[derive(Inbox)]
/// #[inbox(table = "email_jobs", schema = "app")]
/// struct SendEmail {
///     #[field(id)]
///     job_id: i64,
///     #[field(payload)]
///     payload: Vec<u8>,
/// }
///
/// /// Where a queue's rows live, for the line a service logs when it starts.
/// fn location<Row: InboxRow>() -> String {
///     let spec = Row::SPEC;
///     match spec.schema() {
///         Some(schema) => format!("{schema}.{}", spec.table()),
///         None => spec.table().to_owned(),
///     }
/// }
///
/// assert_eq!(location::<SendEmail>(), "app.email_jobs");
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not describe a queue table",
    label = "not an inbox row",
    note = "derive it: `#[derive(Inbox)]` with `#[inbox(table = \"..\")]` and a `#[field(id)]` field"
)]
pub trait InboxRow: Sized + Send + Sync + 'static {
    /// The table the struct describes: its name, its columns and their roles, and the form its
    /// rows are claimed in.
    const SPEC: TableSpec<'static>;

    /// The type of the field that plays `id`.
    type Id: Send + Sync + 'static;
}
