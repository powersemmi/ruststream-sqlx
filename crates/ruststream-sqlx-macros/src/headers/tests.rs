use syn::{DeriveInput, parse_quote};

use super::expand;

fn errors(input: &DeriveInput) -> Vec<String> {
    expand(input).map_or_else(
        |error| error.into_iter().map(|error| error.to_string()).collect(),
        |_| Vec::new(),
    )
}

#[test]
fn what_belongs_to_the_message_is_refused_on_the_headers_struct() {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs", custom(fetch))]
        struct Head {
            #[field(id)] id: i64,
            #[field(payload)] body: Vec<u8>,
            #[field(headers)] meta: Json<Map>,
            #[sqlx(flatten)] extra: Extra,
        }
    };
    assert_eq!(
        errors(&input),
        [
            "`custom(..)` lists the events of the message, which a handler takes: put it on the \
             message struct whose `#[field(headers)]` field flattens `Head`",
            "`body` plays `payload`, and a headers struct holds no message: the handler takes \
             the message struct that flattens `Head`, as in row mode",
            "`meta` plays `headers` in a headers struct: every field of `Head` without a role is \
             a header already, so drop the role",
            "`extra` flattens a struct whose columns `Head` cannot see: a headers struct names \
             every column of the queue table it describes",
        ]
    );
    let untabled: DeriveInput = parse_quote! { struct Head { #[field(id)] id: i64 } };
    assert_eq!(
        errors(&untabled),
        ["#[derive(InboxHeaders)] needs the table: add `#[inbox(table = \"..\")]`"]
    );
}

#[test]
fn the_fields_without_a_role_are_the_headers_under_their_columns_names() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[inbox(table = "jobs")]
        #[sqlx(rename_all = "camelCase")]
        struct Head {
            #[field(id, generated)] job_id: i64,
            #[field(group)] name: String,
            tenant_name: String,
            #[sqlx(rename = "trace")] trace_id: Option<String>,
            #[sqlx(skip)] cache: u8,
        }
    };
    let impls = expand(&input)?.to_string().replace(' ', "");
    for expected in [
        "impl::ruststream_sqlx::InboxHeadersforHead{typeId=i64;\
         typeSettings=(::ruststream_sqlx::spec::HeaderFields,);\
         constTABLE:::ruststream_sqlx::InboxSpec<Self::Settings>=\
         ::ruststream_sqlx::InboxSpec::new(\"jobs\",\
         ::ruststream_sqlx::dialect::Column::new(\"jobId\").generated())",
        ".group(::ruststream_sqlx::dialect::Column::new(\"name\"))\
         .data(&[::ruststream_sqlx::dialect::Column::new(\"tenantName\"),\
         ::ruststream_sqlx::dialect::Column::new(\"trace\")]).header_fields();",
        "fnid(&self)->&i64{&self.job_id}",
        "constNAMES:&'static[&'staticstr]=&[\"tenantName\",\"trace\"];",
        "::ruststream_sqlx::put_header(&mutheaders,\"tenantName\",&self.tenant_name);",
        "::ruststream_sqlx::put_header(&mutheaders,\"trace\",&self.trace_id);",
    ] {
        assert!(impls.contains(expected), "{expected}\n{impls}");
    }
    assert!(!impls.contains("\"cache\""), "a skipped field is no header");
    for machinery in ["HeadersRow", "HeadersLease", "Events"] {
        assert!(!impls.contains(machinery), "{machinery}: {impls}");
    }
    // The insert is built with each dialect the macros are built with.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    assert!(
        impls.contains("impl<__C>"),
        "the headers struct gets the generated insert"
    );
    Ok(())
}
