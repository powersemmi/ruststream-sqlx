use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, parse_quote};

use super::parse::{OutboxRole, record};
use super::registry;

fn errors(input: &DeriveInput) -> Vec<String> {
    record(input).map_or_else(
        |error| error.into_iter().map(|error| error.to_string()).collect(),
        |_| Vec::new(),
    )
}

fn expanded(input: TokenStream2) -> String {
    registry::expand(input).map_or_else(|error| error.to_string(), |tokens| tokens.to_string())
}

#[test]
fn the_roles_land_on_their_fields_and_name_their_columns() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox", schema = "app")]
        #[sqlx(rename_all = "camelCase")]
        struct Order {
            #[field(id)]
            record_id: i64,
            #[field(name)]
            #[sqlx(rename = "channel")]
            name: String,
            #[field(payload)]
            payload: Vec<u8>,
            #[field(headers)]
            headers: Option<String>,
            #[field(processed_at)]
            processed_at: Option<String>,
            created_at: String,
            #[sqlx(skip)]
            note: String,
        }
    };
    let record = record(&input)?;
    assert_eq!(record.table.name.value(), "outbox");
    assert_eq!(
        record.table.schema.as_ref().map(syn::LitStr::value),
        Some("app".to_owned())
    );
    let columns: Vec<_> = record
        .columns()
        .map(|(field, column)| (field.ident.to_string(), column.name.clone(), column.role))
        .collect();
    assert_eq!(
        columns,
        [
            (
                "record_id".to_owned(),
                "recordId".to_owned(),
                Some(OutboxRole::Id)
            ),
            (
                "name".to_owned(),
                "channel".to_owned(),
                Some(OutboxRole::Name)
            ),
            (
                "payload".to_owned(),
                "payload".to_owned(),
                Some(OutboxRole::Payload)
            ),
            (
                "headers".to_owned(),
                "headers".to_owned(),
                Some(OutboxRole::Headers)
            ),
            (
                "processed_at".to_owned(),
                "processedAt".to_owned(),
                Some(OutboxRole::ProcessedAt)
            ),
            ("created_at".to_owned(), "createdAt".to_owned(), None),
        ]
    );
    assert!(!record.flattens());
    Ok(())
}

#[test]
fn custom_lists_the_events_the_service_writes() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox", custom(fetch, ack, retry, discard, recover))]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(name)]
            name: String,
            #[field(payload)]
            payload: Vec<u8>,
        }
    };
    let custom = record(&input)?.table.custom;
    assert!(custom.fetch && custom.ack && custom.retry && custom.discard && custom.recover);
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox", custom(retry))]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(name)]
            name: String,
            #[field(payload)]
            payload: Vec<u8>,
        }
    };
    let custom = record(&input)?.table.custom;
    assert!(custom.retry && !custom.fetch && !custom.ack && !custom.discard && !custom.recover);
    Ok(())
}

#[test]
fn a_record_without_its_required_roles_names_each() {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order {
            headers: Option<String>,
        }
    };
    assert_eq!(
        errors(&input),
        [
            "table `outbox` has no `id` field: mark the field that identifies a record with \
             `#[field(id)]`",
            "table `outbox` has no `name` field: mark the field that holds the name a record \
             was published under with `#[field(name)]`",
            "table `outbox` has no `payload` field: mark the field that holds the published \
             bytes with `#[field(payload)]`",
        ]
    );
}

#[test]
fn a_role_is_played_once() {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(payload, id)]
            payload: Vec<u8>,
        }
    };
    assert_eq!(
        errors(&input),
        ["this field already plays `payload`: a field plays one role"]
    );
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(name)]
            name: String,
            #[field(name)]
            channel: String,
            #[field(payload)]
            payload: Vec<u8>,
        }
    };
    assert_eq!(
        errors(&input),
        ["`channel` plays `name`, which `name` plays already: a role is played by one field"]
    );
}

#[test]
fn an_unknown_role_lists_the_outboxs_roles() {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(retry_after)]
            due: String,
        }
    };
    assert_eq!(
        errors(&input),
        [
            "unknown role of an outbox record: expected `id`, `name`, `payload`, `headers` or \
             `processed_at`"
        ]
    );
}

#[test]
fn a_column_is_named_once_and_a_role_needs_a_column() {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(name)]
            name: String,
            #[field(payload)]
            payload: Vec<u8>,
            #[sqlx(rename = "name")]
            channel: String,
            #[field(headers)]
            #[sqlx(skip)]
            headers: Option<String>,
        }
    };
    assert_eq!(
        errors(&input),
        ["`#[sqlx(skip)]` leaves `headers` without a column, so it cannot play `headers`",]
    );
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(name)]
            name: String,
            #[field(payload)]
            payload: Vec<u8>,
            #[sqlx(rename = "name")]
            channel: String,
        }
    };
    assert_eq!(
        errors(&input),
        ["`channel` reads column `name`, which `name` reads already"]
    );
}

#[test]
fn the_table_attribute_refuses_what_it_does_not_take() {
    let cases: [(DeriveInput, &str); 7] = [
        (
            parse_quote!(
                struct Order {}
            ),
            "#[derive(Outbox)] needs the table: add `#[outbox(table = \"..\")]`",
        ),
        (
            parse_quote!(
                #[outbox(table = "app.outbox")]
                struct Order {}
            ),
            "`table` holds a dot: name the table alone, and its schema with `schema = \"..\"`",
        ),
        (
            parse_quote!(
                #[outbox(table = "outbox", table = "other")]
                struct Order {}
            ),
            "`table` is given twice",
        ),
        (
            parse_quote!(
                #[outbox(table = "outbox", advisory_lock = "x")]
                struct Order {}
            ),
            "unknown `#[outbox(..)]` option: expected `table`, `schema` or `custom`",
        ),
        (
            parse_quote!(
                #[outbox(table = "outbox", custom(publish))]
                struct Order {}
            ),
            "`publish` has no default to hand over: implement `outbox::Publish` for the record \
             without listing it",
        ),
        (
            parse_quote!(
                #[outbox(table = "outbox", custom(claim))]
                struct Order {}
            ),
            "unknown event in `custom(..)`: expected `fetch`, `ack`, `retry`, `discard` or \
             `recover`",
        ),
        (
            parse_quote!(
                #[outbox(table = "outbox", custom(ack, ack))]
                struct Order {}
            ),
            "`ack` is listed twice",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(errors(&input), [expected]);
    }
}

#[test]
fn a_tuple_struct_is_refused() {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order(i64);
    };
    assert_eq!(
        errors(&input),
        ["#[derive(Outbox)] describes a table: it takes a struct with named fields"]
    );
}

#[test]
fn outbox_registers_each_name_on_a_registry_with_its_pool() {
    assert_eq!(
        expanded(quote!(pool: pool.clone(), "orders" => Order, "refunds" => app::Refund,)),
        quote!(
            ::ruststream_sqlx::Outbox::new(pool.clone())
                .register::<Order>("orders")
                .register::<app::Refund>("refunds")
        )
        .to_string()
    );
}

#[test]
fn outbox_without_a_pool_defers_it() {
    assert_eq!(
        expanded(quote!("orders" => Order)),
        quote!(::ruststream_sqlx::Outbox::deferred().register::<Order>("orders")).to_string()
    );
}

#[test]
fn outbox_refuses_a_name_registered_twice() {
    assert_eq!(
        expanded(quote!("orders" => Order, "refunds" => Refund, "orders" => Refund)),
        "`orders` is registered twice: a name has one record type"
    );
}

#[test]
fn outbox_refuses_a_key_other_than_pool() {
    assert_eq!(
        expanded(quote!(connection: pool, "orders" => Order)),
        "unknown key `connection`: `outbox!` takes `pool: <expr>` before the names"
    );
}

#[test]
fn the_derive_writes_the_manual_description_and_no_statement() -> syn::Result<()> {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox", schema = "app", custom(fetch, retry))]
        struct Order {
            #[field(id)]
            id: i64,
            #[field(name)]
            #[sqlx(rename = "channel")]
            name: String,
            #[field(payload)]
            payload: Vec<u8>,
            #[field(headers)]
            headers: Option<String>,
            #[field(processed_at)]
            processed_at: Option<String>,
            created_at: String,
        }
    };
    let expanded = super::expand(&input)?;
    let column = |name: &str| quote!(::ruststream_sqlx::dialect::Column::new(#name));
    let (id, channel, payload, created_at, headers, processed_at) = (
        column("id"),
        column("channel"),
        column("payload"),
        column("created_at"),
        column("headers"),
        column("processed_at"),
    );
    let spec = quote!(::ruststream_sqlx::outbox::spec);
    let expected = quote! {
        #[automatically_derived]
        impl ::ruststream_sqlx::OutboxTable for Order {
            type Id = i64;
            type Table = ::ruststream_sqlx::OutboxSpec<(
                #spec::Headers,
                #spec::ProcessedAt,
                #spec::own::Fetch,
                #spec::own::Retry,
            )>;

            const TABLE: Self::Table =
                ::ruststream_sqlx::OutboxSpec::new("outbox", #id, #channel, #payload)
                    .within("app")
                    .data(&[#created_at])
                    .headers(#headers)
                    .processed_at(#processed_at)
                    .own::<#spec::own::Fetch>()
                    .own::<#spec::own::Retry>();

            fn id(&self) -> &i64 {
                let _ = &self.processed_at;
                &self.id
            }

            fn name(&self) -> &str {
                ::core::convert::AsRef::<str>::as_ref(&self.name)
            }

            fn payload(&self) -> &[u8] {
                ::core::convert::AsRef::<[u8]>::as_ref(&self.payload)
            }
        }

        #[automatically_derived]
        impl ::ruststream_sqlx::HeaderRow for Order {
            type Column = Option<String>;

            fn headers_mut(&mut self) -> &mut Option<String> {
                &mut self.headers
            }
        }
    };
    assert_eq!(expanded.to_string(), expected.to_string());
    Ok(())
}

#[test]
fn an_id_read_through_json_is_refused_on_its_field() {
    let input: DeriveInput = parse_quote! {
        #[outbox(table = "outbox")]
        struct Order {
            #[field(id)]
            #[sqlx(json)]
            id: Key,
            #[field(name)]
            name: String,
            #[field(payload)]
            payload: Vec<u8>,
        }
    };
    let error = super::expand(&input).map_or_else(|error| error.to_string(), |_| String::new());
    assert_eq!(
        error,
        "the outbox binds a record's id as itself: the `id` field reads its column without \
         `#[sqlx(json)]`"
    );
}
