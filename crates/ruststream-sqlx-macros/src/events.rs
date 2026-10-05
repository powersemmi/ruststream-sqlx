//! The row's contract with the broker beyond its description: `PayloadRow` and the hidden
//! `Events<DB>`, one implementation for every database the field types allow.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use ruststream_sqlx_dialect::Role;
use syn::spanned::Spanned;
use syn::{DeriveInput, Generics, Path, Type, WherePredicate, parse_quote};

use crate::parse::{Field, Inbox};

/// The field that plays `role`, if one does.
fn playing<'i, 'a>(inbox: &'i Inbox<'a>, role: Role) -> Option<&'i Field<'a>> {
    inbox
        .columns()
        .find(|(_, column)| column.role == Some(role))
        .map(|(field, _)| field)
}

/// `PayloadRow`, for a struct with a payload field.
pub(crate) fn payload_row(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
) -> Option<TokenStream2> {
    let field = playing(inbox, Role::Payload)?;
    let ident = field.ident;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let bytes =
        quote_spanned!(field.ty.span()=> ::core::convert::AsRef::<[u8]>::as_ref(&self.#ident));
    Some(quote! {
        impl #impl_generics ::ruststream_sqlx::PayloadRow for #name #ty_generics #where_clause {
            fn payload(&self) -> &[u8] {
                #bytes
            }
        }
    })
}

/// The bounds and the binder arms one time role adds.
struct TimeRole {
    time: TokenStream2,
}

impl TimeRole {
    fn new(ty: &Type, predicates: &mut Vec<WherePredicate>) -> Self {
        let p = quote!(::ruststream_sqlx::__private);
        predicates.push(parse_quote!(#ty: #p::TimeFor<__DB>));
        let time = quote!(<#ty as #p::TimeFor<__DB>>::Time);
        Self { time }
    }
}

/// The hidden `Events<DB>` implementation.
// One generator for the whole contract: the parts share the bounds they build.
#[allow(clippy::too_many_lines)]
pub(crate) fn events(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    id: &Field<'_>,
) -> TokenStream2 {
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let name = &input.ident;
    let custom = inbox.table.custom;
    let clock: Path = inbox
        .table
        .clock
        .clone()
        .unwrap_or_else(|| parse_quote!(::ruststream_sqlx::SystemClock));
    let id_ident = id.ident;
    let id_ty = id.ty;

    let mut predicates: Vec<WherePredicate> = vec![
        parse_quote!(__DB: #p::QueueDatabase),
        parse_quote!(
            Self: for<'__r> #p::sqlx::FromRow<'__r, <__DB as #p::sqlx::Database>::Row>
                + ::core::marker::Unpin
        ),
        parse_quote!(#id_ty: for<'__q> #p::sqlx::Encode<'__q, __DB> + #p::sqlx::Type<__DB>),
    ];

    let via = |ty: &Type| quote!(<#ty as #p::Via<__DB>>::Is);

    let (headers, unfit_header) = playing(inbox, Role::Headers).map_or_else(
        || {
            (
                quote!(#p::HeaderMap::new()),
                quote!(#p::first_header(headers)),
            )
        },
        |field| {
            let column = via(field.ty);
            predicates.push(parse_quote!(#column: #r::HeaderColumn));
            let ident = field.ident;
            (
                quote!(#r::HeaderColumn::to_headers(&self.#ident)),
                quote!(<#column as #r::HeaderColumn>::unfit(headers)),
            )
        },
    );
    let key = playing(inbox, Role::PartitionKey).map_or_else(
        || quote!(::core::option::Option::None),
        |field| {
            let column = via(field.ty);
            predicates.push(parse_quote!(#column: #r::KeyColumn));
            let ident = field.ident;
            quote!(#r::KeyColumn::key(&self.#ident))
        },
    );
    let attempt = playing(inbox, Role::Attempt).map_or_else(
        || quote!(::core::option::Option::None),
        |field| {
            let column = via(field.ty);
            predicates.push(parse_quote!(#column: #r::AttemptColumn));
            let ident = field.ident;
            quote!(::core::option::Option::Some(#r::AttemptColumn::attempt(&self.#ident)))
        },
    );

    let retry_after =
        playing(inbox, Role::RetryAfter).map(|field| TimeRole::new(field.ty, &mut predicates));
    let processed_at =
        playing(inbox, Role::ProcessedAt).map(|field| TimeRole::new(field.ty, &mut predicates));
    let retry_after_arms = retry_after.as_ref().map(|role| {
        let time = &role.time;
        quote! {
            (#p::Param::Now, #p::Event::Claim) => {
                #p::put::<__DB, _>(arguments, #p::now::<#clock, #time>(values.now, values.event)?)?;
                true
            }
            (#p::Param::RetryAfter, #p::Event::RetryAfter) => {
                #p::put::<__DB, _>(
                    arguments,
                    #p::later::<#clock, #time>(values.now, values.delay, values.event)?,
                )?;
                true
            }
        }
    });
    let processed_at_arms = processed_at.as_ref().map(|role| {
        let time = &role.time;
        quote! {
            (#p::Param::Now, #p::Event::Ack | #p::Event::Discard) => {
                #p::put::<__DB, _>(arguments, #p::now::<#clock, #time>(values.now, values.event)?)?;
                true
            }
        }
    });

    // Matching fetched rows to claimed ids compares ids.
    if custom.claim || custom.fetch {
        let column = via(id_ty);
        predicates.push(parse_quote!(#column: ::core::cmp::PartialEq));
    }
    // The crate's claim of ids decodes them.
    if custom.fetch && !custom.claim {
        let column = via(id_ty);
        predicates.push(parse_quote!(#id_ty: for<'__r> #p::sqlx::Decode<'__r, __DB>));
        predicates.push(parse_quote!(#column: ::core::marker::Unpin));
    }
    // The crate's fetch binds the ids a claim of the service's own returned.
    let ids_arm = (custom.claim && !custom.fetch).then(|| {
        predicates.push(parse_quote!(
            for<'__q, '__x> &'__x [#id_ty]: #p::sqlx::Encode<'__q, __DB> + #p::sqlx::Type<__DB>
        ));
        quote! {
            (#p::Param::Ids, #p::Event::Fetch) => {
                #p::put::<__DB, _>(arguments, values.ids)?;
                true
            }
        }
    });

    let event_bound = |listed: bool, event: TokenStream2, predicates: &mut Vec<WherePredicate>| {
        if listed {
            predicates.push(parse_quote!(Self: #r::#event<__DB>));
        }
    };
    event_bound(custom.claim, quote!(Claim), &mut predicates);
    event_bound(custom.fetch, quote!(Fetch), &mut predicates);
    event_bound(custom.ack, quote!(Ack), &mut predicates);
    event_bound(custom.retry, quote!(Retry), &mut predicates);
    event_bound(custom.retry_after, quote!(RetryAfter), &mut predicates);
    event_bound(custom.discard, quote!(Discard), &mut predicates);
    event_bound(custom.dead_letter, quote!(DeadLetter), &mut predicates);

    let claim = match (custom.claim, custom.fetch) {
        (false, false) => quote!(#p::claim_rows::<__DB, Self>(conn, cx, out)),
        (claim, fetch) => {
            let ids = if claim {
                quote!(<Self as #r::Claim<__DB>>::claim(&mut *conn, cx.queue, cx.limit).await?)
            } else {
                quote!(#p::claim_ids::<__DB, Self>(&mut *conn, cx).await?)
            };
            let rows = if fetch {
                quote!(<Self as #r::Fetch<__DB>>::fetch(&mut *conn, &ids).await?)
            } else {
                quote!(#p::fetch_by_ids::<__DB, Self>(&mut *conn, cx, &ids).await?)
            };
            quote!(async move {
                let ids = #ids;
                let rows = #rows;
                #p::match_claimed::<__DB, Self>(ids, rows, out);
                ::core::result::Result::Ok(())
            })
        }
    };
    let ack = if custom.ack {
        quote!({ let _ = cx; <Self as #r::Ack<__DB>>::ack(conn, id) })
    } else {
        quote!(#p::ack::<__DB, Self>(conn, cx, id))
    };
    let retry = if custom.retry {
        quote!(async move {
            let _ = cx;
            <Self as #r::Retry<__DB>>::retry(conn, id).await?;
            ::core::result::Result::Ok(#p::Released::Written)
        })
    } else {
        quote!(#p::retry::<__DB, Self>(conn, cx, id))
    };
    let retry_after_call = if custom.retry_after {
        quote!({ let _ = cx; <Self as #r::RetryAfter<__DB>>::retry_after(conn, id, delay) })
    } else {
        quote!(#p::retry_after::<__DB, Self>(conn, cx, id, delay))
    };
    let discard = if custom.discard {
        quote!({ let _ = cx; <Self as #r::Discard<__DB>>::discard(conn, id) })
    } else {
        quote!(#p::discard::<__DB, Self>(conn, cx, id))
    };
    let dead_letter = if custom.dead_letter {
        quote!({ let _ = cx; <Self as #r::DeadLetter<__DB>>::dead_letter(conn, id, destination) })
    } else {
        quote!(#p::dead_letter::<__DB, Self>(conn, cx, id, destination))
    };

    let flags = [
        custom.claim,
        custom.fetch,
        custom.ack,
        custom.retry,
        custom.retry_after,
        custom.discard,
        custom.dead_letter,
        retry_after.is_some(),
    ];
    let [
        c_claim,
        c_fetch,
        c_ack,
        c_retry,
        c_retry_after,
        c_discard,
        c_dead,
        retry_after_column,
    ] = flags;

    let mut generics = generics.clone();
    generics.params.push(parse_quote!(__DB));
    generics.make_where_clause().predicates.extend(predicates);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics #p::Events<__DB> for #name #ty_generics #where_clause {
            const SHAPE: #p::Shape = #p::Shape {
                custom_claim: #c_claim,
                custom_fetch: #c_fetch,
                custom_ack: #c_ack,
                custom_retry: #c_retry,
                custom_retry_after: #c_retry_after,
                custom_discard: #c_discard,
                custom_dead_letter: #c_dead,
                retry_after_column: #retry_after_column,
            };

            fn id(&self) -> &<Self as #r::InboxRow>::Id {
                &self.#id_ident
            }

            fn headers(&self) -> #p::HeaderMap {
                #headers
            }

            fn unfit_header(headers: &#p::HeaderMap) -> ::core::option::Option<&str> {
                #unfit_header
            }

            fn partition_key(&self) -> ::core::option::Option<&[u8]> {
                #key
            }

            fn attempt(&self) -> ::core::option::Option<u64> {
                #attempt
            }

            fn bind(
                param: #p::Param,
                arguments: &mut <__DB as #p::sqlx::Database>::Arguments,
                values: &#p::Values<'_, Self>,
            ) -> ::core::result::Result<bool, #p::sqlx::Error> {
                ::core::result::Result::Ok(match (param, values.event) {
                    (#p::Param::Id, _) => match values.id {
                        ::core::option::Option::Some(id) => {
                            #p::put::<__DB, _>(arguments, id)?;
                            true
                        }
                        ::core::option::Option::None => false,
                    },
                    (#p::Param::Group, _) => {
                        <__DB as #p::QueueDatabase>::bind_str(arguments, values.queue)?;
                        true
                    }
                    (#p::Param::Limit, _) => {
                        <__DB as #p::QueueDatabase>::bind_i64(arguments, values.limit)?;
                        true
                    }
                    (#p::Param::Destination, #p::Event::DeadLetter) => {
                        <__DB as #p::QueueDatabase>::bind_str(arguments, values.destination)?;
                        true
                    }
                    (#p::Param::Delay, #p::Event::RetryAfter) => {
                        <__DB as #p::QueueDatabase>::bind_i64(arguments, #p::micros(values.delay))?;
                        true
                    }
                    #retry_after_arms
                    #processed_at_arms
                    #ids_arm
                    _ => false,
                })
            }

            fn claim<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Claiming<'__a>,
                out: &'__a mut ::std::vec::Vec<#p::Claimed<Self>>,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__a {
                #claim
            }

            fn ack<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling<'__a>,
                id: &'__a <Self as #r::InboxRow>::Id,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__a {
                #ack
            }

            fn retry<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling<'__a>,
                id: &'__a <Self as #r::InboxRow>::Id,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#p::Released, #p::sqlx::Error>,
            > + ::core::marker::Send + '__a {
                #retry
            }

            fn retry_after<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling<'__a>,
                id: &'__a <Self as #r::InboxRow>::Id,
                delay: ::core::time::Duration,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__a {
                #retry_after_call
            }

            fn discard<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling<'__a>,
                id: &'__a <Self as #r::InboxRow>::Id,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__a {
                #discard
            }

            fn dead_letter<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling<'__a>,
                id: &'__a <Self as #r::InboxRow>::Id,
                destination: &'__a str,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__a {
                #dead_letter
            }
        }
    }
}
