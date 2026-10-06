//! The advisory lock form's text: how a dialect probes a key in use, the key a row's parts render,
//! the candidates of a claim and the row a take names.

use super::{BuiltIn, CLAIMABLE, SqlWriter};
use crate::form::KeyPart;
use crate::spec::TableSpec;
use crate::statement::Param;

/// The name the advisory claim gives each candidate's lock key.
const LOCK_KEY: &str = "__lock";

/// How the advisory claim leaves out the candidates whose key another session holds; each built-in
/// dialect has its own, so each variant exists with its dialect's feature.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Probe {
    /// The database cannot tell: the claim selects every claimable row.
    #[cfg(feature = "sqlite")]
    Blind,
    /// A condition on the key that takes nothing, beside the claim's other conditions: the text
    /// before the key and the text after it.
    #[cfg(feature = "mysql")]
    Check(&'static str, &'static str),
    /// A condition that takes a lock on the key until the select's transaction ends: the text
    /// before the key and the text after it.
    #[cfg(feature = "postgres")]
    Lock(&'static str, &'static str),
}

impl<D> SqlWriter<'_, D>
where
    D: BuiltIn + ?Sized,
{
    /// `text` as a string literal: quoted, each quote doubled, and each backslash doubled where a
    /// backslash escapes.
    pub(crate) fn literal(&mut self, text: &str) -> &mut Self {
        self.sql.push('\'');
        for character in text.chars() {
            if character == '\'' || (character == '\\' && D::BACKSLASH_ESCAPES) {
                self.sql.push(character);
            }
            self.sql.push(character);
        }
        self.sql.push('\'');
        self
    }

    /// The parts of a lock key with `separator` between them: each literal quoted, each column as
    /// `column` writes it. A key of no parts is one empty literal.
    pub(crate) fn key_parts(
        &mut self,
        key: &[KeyPart<'_>],
        separator: &str,
        column: impl Fn(&mut Self, &str),
    ) -> &mut Self {
        if key.is_empty() {
            return self.literal("");
        }
        for (index, part) in key.iter().enumerate() {
            if index > 0 {
                self.push(separator);
            }
            match part {
                KeyPart::Literal(text) => {
                    self.literal(text);
                }
                KeyPart::Column(name) => column(self, name),
            }
        }
        self
    }

    /// The lock key of a row of `spec`'s table, as the dialect renders it from `key`'s parts.
    fn lock_key(&mut self, spec: &TableSpec<'_>, key: &[KeyPart<'_>]) -> &mut Self {
        let dialect = self.dialect;
        dialect.render_lock_key(self, spec.schema(), key);
        self
    }

    /// The row a take names: its id, while the row is still claimable.
    pub(super) fn taken_row(&mut self, spec: &TableSpec<'_>) -> &mut Self {
        self.push(" WHERE ")
            .ident(spec.id().name())
            .push(" = ")
            .param(Param::Id)
            .conditions(spec, &CLAIMABLE, " AND ")
    }

    /// The advisory claim: the id and the lock key, named `__lock`, of up to [`Param::Limit`]
    /// claimable rows in claim order, without the rows whose key another session holds where the
    /// dialect's [`Probe`] tells them.
    pub(super) fn candidates(&mut self, spec: &TableSpec<'_>, key: &[KeyPart<'_>]) -> &mut Self {
        match D::PROBE {
            #[cfg(feature = "sqlite")]
            Probe::Blind => self.candidate_rows(spec, key, None),
            #[cfg(feature = "mysql")]
            Probe::Check(before, after) => self.candidate_rows(spec, key, Some((before, after))),
            // A probe that takes a lock runs above the ordered select, on one row after another,
            // until the limit is reached. In the select's own `WHERE` it would run on every
            // claimable row before the sort, and a long queue would fill the database's lock
            // table. `OFFSET 0` keeps the database from moving the probe into the select.
            #[cfg(feature = "postgres")]
            Probe::Lock(before, after) => {
                let id = spec.id().name();
                self.push("SELECT ")
                    .ident(id)
                    .push(", ")
                    .ident(LOCK_KEY)
                    .push(" FROM (")
                    .candidate_rows(spec, key, None)
                    .push(" OFFSET 0) AS __candidates WHERE ")
                    .push(before)
                    .ident(LOCK_KEY)
                    .push(after)
            }
        }
        .push(" LIMIT ")
        .param(Param::Limit)
    }

    /// The id and the lock key of the claimable rows in claim order, without the rows whose key
    /// fails `check`: the text before the key and the text after it.
    fn candidate_rows(
        &mut self,
        spec: &TableSpec<'_>,
        key: &[KeyPart<'_>],
        check: Option<(&str, &str)>,
    ) -> &mut Self {
        let id = spec.id().name();
        self.push("SELECT ")
            .ident(id)
            .push(", ")
            .lock_key(spec, key)
            .push(" AS ")
            .ident(LOCK_KEY)
            .push(" FROM ")
            .table(spec);
        let keyword = self.conditions_then(spec, &CLAIMABLE, " WHERE ");
        if let Some((before, after)) = check {
            self.push(keyword)
                .push(before)
                .lock_key(spec, key)
                .push(after);
        }
        self.claim_order(spec, id)
    }
}
