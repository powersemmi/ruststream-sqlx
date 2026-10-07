//! `custom(..)` in `#[inbox(..)]`: the events a service implements itself.

use proc_macro2::Span;
use syn::meta::ParseNestedMeta;
use syn::spanned::Spanned;

/// The events a service implements itself: `#[inbox(custom(..))]`.
// One switch per event the crate can hand over.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Custom {
    /// Where `claim` is listed: the advisory lock form selects its candidates itself, which the
    /// struct is checked for.
    pub(crate) claim: Option<Span>,
    pub(crate) fetch: bool,
    pub(crate) ack: bool,
    pub(crate) retry: bool,
    pub(crate) retry_after: bool,
    pub(crate) discard: bool,
    pub(crate) dead_letter: bool,
    /// Where `extend` is listed: an event of the lease form only, which the struct is checked
    /// for.
    pub(crate) extend: Option<Span>,
    /// Where `lock` is listed: an event of the advisory lock form, listed with `unlock`, which
    /// the struct is checked for.
    pub(crate) lock: Option<Span>,
    /// Where `unlock` is listed, as `lock` is.
    pub(crate) unlock: Option<Span>,
}

impl Custom {
    /// Reads one event of `custom(..)`.
    pub(super) fn list(&mut self, event: &ParseNestedMeta<'_>) -> syn::Result<()> {
        let word = event
            .path
            .get_ident()
            .map(ToString::to_string)
            .unwrap_or_default();
        if word == "publish" {
            return Err(event.error(
                "`publish` has no default to hand over: implement `Publish` for the struct \
                 without listing it",
            ));
        }
        if let Some(listed) = self.spanned(&word) {
            if listed.replace(event.path.span()).is_some() {
                return Err(event.error(format!("`{word}` is listed twice")));
            }
            return Ok(());
        }
        let Some(slot) = self.slot(&word) else {
            return Err(event.error(
                "unknown event in `custom(..)`: expected `claim`, `fetch`, `ack`, `retry`, \
                 `retry_after`, `discard`, `dead_letter`, `extend`, `lock` or `unlock`",
            ));
        };
        if std::mem::replace(slot, true) {
            return Err(event.error(format!("`{word}` is listed twice")));
        }
        Ok(())
    }

    /// Where the event `word` names is listed, for an event the struct is checked for.
    fn spanned(&mut self, word: &str) -> Option<&mut Option<Span>> {
        Some(match word {
            "claim" => &mut self.claim,
            "extend" => &mut self.extend,
            "lock" => &mut self.lock,
            "unlock" => &mut self.unlock,
            _ => return None,
        })
    }

    /// The switch of the event `word` names.
    fn slot(&mut self, word: &str) -> Option<&mut bool> {
        Some(match word {
            "fetch" => &mut self.fetch,
            "ack" => &mut self.ack,
            "retry" => &mut self.retry,
            "retry_after" => &mut self.retry_after,
            "discard" => &mut self.discard,
            "dead_letter" => &mut self.dead_letter,
            _ => return None,
        })
    }
}
