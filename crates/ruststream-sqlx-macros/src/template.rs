//! The advisory lock key: literal text with fields named between braces, `"jobs-{job_id}"`.

use syn::LitStr;

/// One piece of a key template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Piece {
    /// Text copied into the key.
    Literal(String),
    /// The value of the named field.
    Field(String),
}

/// Splits the template into literal text and field names.
pub(crate) fn parse(template: &LitStr) -> syn::Result<Vec<Piece>> {
    let text = template.value();
    let mut pieces = Vec::new();
    let mut rest = text.as_str();
    while let Some(open) = rest.find(['{', '}']) {
        if open > 0 {
            pieces.push(Piece::Literal(rest[..open].to_owned()));
        }
        if rest[open..].starts_with('}') {
            return Err(syn::Error::new(
                template.span(),
                "the lock key has a `}` without its `{`: a field is named between braces",
            ));
        }
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Err(syn::Error::new(
                template.span(),
                "the lock key has a `{` without its `}`: a field is named between braces",
            ));
        };
        let field = &after[..close];
        if field.is_empty() {
            return Err(syn::Error::new(
                template.span(),
                "the lock key has empty braces: name a field between them",
            ));
        }
        pieces.push(Piece::Field(field.to_owned()));
        rest = &after[close + 1..];
    }
    if !rest.is_empty() {
        pieces.push(Piece::Literal(rest.to_owned()));
    }
    Ok(pieces)
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::{Piece, parse};

    #[test]
    fn a_template_splits_into_text_and_fields() -> syn::Result<()> {
        assert_eq!(
            parse(&parse_quote!("jobs-{job_id}-{name}"))?,
            [
                Piece::Literal("jobs-".to_owned()),
                Piece::Field("job_id".to_owned()),
                Piece::Literal("-".to_owned()),
                Piece::Field("name".to_owned()),
            ]
        );
        assert_eq!(
            parse(&parse_quote!("{job_id}"))?,
            [Piece::Field("job_id".to_owned())]
        );
        assert_eq!(
            parse(&parse_quote!("one-at-a-time"))?,
            [Piece::Literal("one-at-a-time".to_owned())]
        );
        Ok(())
    }

    #[test]
    fn unbalanced_or_empty_braces_are_refused() {
        let message = |template: syn::LitStr| parse(&template).map_err(|error| error.to_string());
        assert_eq!(
            message(parse_quote!("jobs-{job_id")),
            Err(
                "the lock key has a `{` without its `}`: a field is named between braces"
                    .to_owned()
            )
        );
        assert_eq!(
            message(parse_quote!("jobs-}")),
            Err(
                "the lock key has a `}` without its `{`: a field is named between braces"
                    .to_owned()
            )
        );
        assert_eq!(
            message(parse_quote!("jobs-{}")),
            Err("the lock key has empty braces: name a field between them".to_owned())
        );
    }
}
