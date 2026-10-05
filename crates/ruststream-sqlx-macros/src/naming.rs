//! sqlx's `rename_all` casings, applied the way sqlx's own derives apply them.

use heck::{ToKebabCase, ToLowerCamelCase, ToShoutySnakeCase, ToSnakeCase, ToUpperCamelCase};
use syn::LitStr;

/// A `#[sqlx(rename_all = "..")]` casing: the spellings and their effect are sqlx 0.9's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenameAll {
    Lower,
    Snake,
    Upper,
    ScreamingSnake,
    Kebab,
    Camel,
    Pascal,
}

impl RenameAll {
    /// Reads the casing from the attribute's string.
    pub(crate) fn parse(value: &LitStr) -> syn::Result<Self> {
        match value.value().as_str() {
            "lowercase" => Ok(Self::Lower),
            "snake_case" => Ok(Self::Snake),
            "UPPERCASE" => Ok(Self::Upper),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnake),
            "kebab-case" => Ok(Self::Kebab),
            "camelCase" => Ok(Self::Camel),
            "PascalCase" => Ok(Self::Pascal),
            other => Err(syn::Error::new(
                value.span(),
                format!(
                    "unknown `rename_all` casing `{other}`: sqlx takes lowercase, snake_case, \
                     UPPERCASE, SCREAMING_SNAKE_CASE, kebab-case, camelCase or PascalCase"
                ),
            )),
        }
    }

    /// The column name sqlx reads for a field named `name`.
    pub(crate) fn apply(self, name: &str) -> String {
        match self {
            Self::Lower => name.to_lowercase(),
            Self::Snake => name.to_snake_case(),
            Self::Upper => name.to_uppercase(),
            Self::ScreamingSnake => name.to_shouty_snake_case(),
            Self::Kebab => name.to_kebab_case(),
            Self::Camel => name.to_lower_camel_case(),
            Self::Pascal => name.to_upper_camel_case(),
        }
    }
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::RenameAll;

    #[test]
    fn every_sqlx_casing_renames_like_sqlx() -> syn::Result<()> {
        let cases = [
            ("lowercase", "retry_after"),
            ("snake_case", "retry_after"),
            ("UPPERCASE", "RETRY_AFTER"),
            ("SCREAMING_SNAKE_CASE", "RETRY_AFTER"),
            ("kebab-case", "retry-after"),
            ("camelCase", "retryAfter"),
            ("PascalCase", "RetryAfter"),
        ];
        for (casing, column) in cases {
            let casing = RenameAll::parse(&parse_quote!(#casing))?;
            assert_eq!(casing.apply("retry_after"), column);
        }
        Ok(())
    }

    #[test]
    fn an_unknown_casing_lists_the_known_ones() {
        let error = RenameAll::parse(&parse_quote!("camel")).map_err(|error| error.to_string());
        assert_eq!(
            error,
            Err(
                "unknown `rename_all` casing `camel`: sqlx takes lowercase, snake_case, \
                 UPPERCASE, SCREAMING_SNAKE_CASE, kebab-case, camelCase or PascalCase"
                    .to_owned()
            )
        );
    }
}
