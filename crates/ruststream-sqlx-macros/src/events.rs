//! The row's contract with the broker beyond its description: `PayloadRow` and the hidden
//! `Events<DB>`, one implementation for every database the field types allow.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
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

/// The answer to "can a by-name subscription read and bind this row from its description alone":
/// the kinds of its columns, or `None` when an event is the service's own, for the described path
/// runs the crate's defaults only. `lease` is the time a lease is written in, in the lease form.
fn kinds(
    inbox: &Inbox<'_>,
    clock: &Path,
    id_ty: &Type,
    own_code: bool,
    [retry_after, processed_at]: [Option<&TimeRole>; 2],
    lease: Option<&TokenStream2>,
) -> TokenStream2 {
    if own_code {
        return quote!(::core::option::Option::None);
    }
    let p = quote!(::ruststream_sqlx::__private);
    let step = |role: Role, step: &str| {
        playing(inbox, role).map(|field| {
            let step = format_ident!("{step}");
            let ty = field.ty;
            quote!(.#step::<#ty>())
        })
    };
    let payload = step(Role::Payload, "payload");
    let headers = step(Role::Headers, "headers");
    let key = step(Role::PartitionKey, "partition_key");
    let attempt = step(Role::Attempt, "attempt");
    let retry_after = retry_after.map(|role| {
        let time = &role.time;
        quote!(.retry_after::<#time>())
    });
    let processed_at = processed_at.map(|role| {
        let time = &role.time;
        quote!(.processed_at::<#time>())
    });
    let locked_until = lease.map(|lease| quote!(.locked_until::<#lease>()));
    quote! {
        #p::KindsOf::new::<#id_ty, #clock>()
            #payload #headers #key #attempt #retry_after #processed_at #locked_until
            .finish()
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
        // The id binds into every settlement, and the claim reads it alone from a row whose
        // other columns do not decode.
        parse_quote!(
            #id_ty: for<'__q> #p::sqlx::Encode<'__q, __DB>
                + for<'__r> #p::sqlx::Decode<'__r, __DB>
                + #p::sqlx::Type<__DB>
        ),
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
                quote!(#r::HeaderColumn::take_headers(&mut self.#ident)),
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
    // A row that does not decode still reports its attempt: the column read alone, by its name,
    // as the field's own type.
    let (attempt, read_attempt) = inbox
        .columns()
        .find(|(_, column)| column.role == Some(Role::Attempt))
        .map_or_else(
            || {
                (
                    quote!(::core::option::Option::None),
                    quote!({
                        let _ = (row, queue);
                        ::core::option::Option::None
                    }),
                )
            },
            |(field, column)| {
                let name = &column.name;
                let ty = field.ty;
                // sqlx decodes the column as the `try_from` type where the field names one, and
                // converts it into the field's.
                let decoded = column.try_from.as_deref().unwrap_or(ty);
                let attempt_ty = via(ty);
                predicates.push(parse_quote!(#attempt_ty: #r::AttemptColumn));
                // Written on the type itself, as the id's bound is: the two coincide where the id
                // and the attempt share a type, and a bound through `Via` would then be a second,
                // ambiguous way to prove the same one.
                predicates.push(parse_quote!(
                    #decoded: for<'__r> #p::sqlx::Decode<'__r, __DB> + #p::sqlx::Type<__DB>
                ));
                if column.try_from.is_some() {
                    predicates.push(parse_quote!(
                        #attempt_ty: ::core::convert::TryFrom<#decoded>
                    ));
                }
                let ident = field.ident;
                (
                    quote!(::core::option::Option::Some(#r::AttemptColumn::attempt(&self.#ident))),
                    quote!({
                        let _ = queue;
                        #p::attempt_in::<__DB, #decoded, #ty>(row, #name)
                    }),
                )
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
                #p::put::<__DB, _>(arguments, #p::now::<#clock, #time, __DB, Self>(values)?)?;
                true
            }
            (#p::Param::RetryAfter, #p::Event::RetryAfter) => {
                #p::put::<__DB, _>(arguments, #p::later::<#clock, #time, __DB, Self>(values)?)?;
                true
            }
        }
    });
    let processed_at_arms = processed_at.as_ref().map(|role| {
        let time = &role.time;
        quote! {
            (#p::Param::Now, #p::Event::Ack | #p::Event::Discard) => {
                #p::put::<__DB, _>(arguments, #p::now::<#clock, #time, __DB, Self>(values)?)?;
                true
            }
        }
    });

    // The lease form: the expiry a claim writes and a settlement matches is the token, in the
    // type the `locked_until` field holds. A claim reads "now" once, takes its lease from that
    // instant and binds it wherever its statements compare a time.
    let leased = playing(inbox, Role::LockedUntil).is_some();
    let lease = quote!(<Self as #r::LeaseRow>::Lease);
    let (token, leasing, lease_arms) = if leased {
        predicates.push(parse_quote!(
            #lease: for<'__q> #p::sqlx::Encode<'__q, __DB> + #p::sqlx::Type<__DB>
        ));
        (
            lease.clone(),
            quote!(#p::lease::<#clock, #lease>(queue, now)),
            Some(quote! {
                (#p::Param::LeaseNow, _) => match values.leasing {
                    ::core::option::Option::Some(leasing) => {
                        #p::put::<__DB, _>(arguments, leasing.now)?;
                        true
                    }
                    ::core::option::Option::None => false,
                },
                (#p::Param::Lease, _) => match values.lease {
                    ::core::option::Option::Some(lease) => {
                        #p::put::<__DB, _>(arguments, lease)?;
                        true
                    }
                    ::core::option::Option::None => false,
                },
                (#p::Param::Held, _) => match values.held {
                    ::core::option::Option::Some(held) => {
                        #p::put::<__DB, _>(arguments, held)?;
                        true
                    }
                    ::core::option::Option::None => false,
                },
            }),
        )
    } else {
        (
            quote!(()),
            quote!({
                let _ = (queue, now);
                #p::no_lease()
            }),
            None,
        )
    };

    // Matching fetched rows to claimed ids compares ids.
    if custom.claim || custom.fetch {
        let column = via(id_ty);
        predicates.push(parse_quote!(#column: ::core::cmp::PartialEq));
    }
    // The crate's claim of ids collects them through sqlx, which needs them `Unpin`.
    if custom.fetch && !custom.claim {
        let column = via(id_ty);
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
    event_bound(custom.extend.is_some(), quote!(Extend), &mut predicates);

    let claim = match (custom.claim, custom.fetch) {
        (false, false) => quote!(#p::claim_rows::<__DB, Self>(conn, cx, lease, out)),
        (claim, fetch) => {
            let ids = if claim {
                quote!(<Self as #r::Claim<__DB>>::claim(&mut *conn, cx.queue.name, cx.limit).await?)
            } else {
                quote!(#p::claim_ids::<__DB, Self>(&mut *conn, cx, lease).await?)
            };
            // The service's fetch decodes its own rows; the crate's hands over those it could
            // not decode too.
            let (rows, matched) = if fetch {
                (
                    quote!(<Self as #r::Fetch<__DB>>::fetch(&mut *conn, &ids).await?),
                    quote!(match_rows),
                )
            } else {
                (
                    quote!(#p::fetch_by_ids::<__DB, Self>(&mut *conn, cx, lease, &ids).await?),
                    quote!(match_claimed),
                )
            };
            // A claim and a fetch of the service's own bind no lease: the crate stamps the rows
            // they take.
            let unbound = (claim && fetch).then(|| quote!(let _ = lease;));
            // A claim that took no id has no row to read: an empty poll spends no round trip on
            // the fetch, and a service's fetch written as `IN (..)` never meets the empty list
            // MySQL refuses.
            quote!(async move {
                #unbound
                let ids = #ids;
                if ids.is_empty() {
                    return ::core::result::Result::Ok(());
                }
                let fetched = #rows;
                #p::#matched::<__DB, Self>(ids, fetched, out);
                ::core::result::Result::Ok(())
            })
        }
    };
    // The service's own event settles without the lease: the crate confirms it first.
    let own = |call: TokenStream2| {
        quote!(async move {
            let _ = (cx, held);
            #call.await?;
            ::core::result::Result::Ok(#p::Settled::Written)
        })
    };
    let ack = if custom.ack {
        own(quote!(<Self as #r::Ack<__DB>>::ack(conn, id)))
    } else {
        quote!(#p::ack::<__DB, Self>(conn, cx, id, held))
    };
    let retry = if custom.retry {
        own(quote!(<Self as #r::Retry<__DB>>::retry(conn, id)))
    } else {
        quote!(#p::retry::<__DB, Self>(conn, cx, id, held))
    };
    let retry_after_call = if custom.retry_after {
        own(quote!(<Self as #r::RetryAfter<__DB>>::retry_after(conn, id, delay)))
    } else {
        quote!(#p::retry_after::<__DB, Self>(conn, cx, id, held, delay))
    };
    let discard = if custom.discard {
        own(quote!(<Self as #r::Discard<__DB>>::discard(conn, id)))
    } else {
        quote!(#p::discard::<__DB, Self>(conn, cx, id, held))
    };
    let dead_letter = if custom.dead_letter {
        own(quote!(<Self as #r::DeadLetter<__DB>>::dead_letter(conn, id, destination)))
    } else {
        quote!(#p::dead_letter::<__DB, Self>(conn, cx, id, held, destination))
    };
    let extend = if custom.extend.is_some() {
        quote!(async move {
            let _ = cx;
            let extended = <Self as #r::Extend<__DB>>::extend(conn, id, held, until).await?;
            ::core::result::Result::Ok(if extended {
                #p::Settled::Written
            } else {
                #p::Settled::Lost
            })
        })
    } else {
        quote!(#p::extend::<__DB, Self>(conn, cx, id, held, until))
    };

    let flags = [
        custom.claim,
        custom.fetch,
        custom.ack,
        custom.retry,
        custom.retry_after,
        custom.discard,
        custom.dead_letter,
        custom.extend.is_some(),
    ];
    let kinds = kinds(
        inbox,
        &clock,
        id_ty,
        flags.contains(&true),
        [retry_after.as_ref(), processed_at.as_ref()],
        leased.then_some(&lease),
    );
    let [
        c_claim,
        c_fetch,
        c_ack,
        c_retry,
        c_retry_after,
        c_discard,
        c_dead,
        c_extend,
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
                custom_extend: #c_extend,
            };

            type Token = #token;

            fn kinds() -> ::core::option::Option<#p::Kinds> {
                #kinds
            }

            fn id(&self) -> &<Self as #p::QueueRow>::Id {
                &self.#id_ident
            }

            fn take_headers(&mut self) -> #p::HeaderMap {
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

            fn read_attempt(
                row: &<__DB as #p::sqlx::Database>::Row,
                queue: &'static #p::Queue,
            ) -> ::core::option::Option<u64> {
                #read_attempt
            }

            fn bind(
                param: #p::Param,
                arguments: &mut <__DB as #p::sqlx::Database>::Arguments,
                values: &#p::Values<'_, __DB, Self>,
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
                        <__DB as #p::QueueDatabase>::bind_str(arguments, values.queue.name)?;
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
                    #lease_arms
                    #ids_arm
                    _ => false,
                })
            }

            fn lease(
                queue: &'static #p::Queue,
                now: #p::Now,
            ) -> ::core::result::Result<
                #p::Leasing<<Self as #p::Events<__DB>>::Token>,
                #p::sqlx::Error,
            > {
                #leasing
            }

            fn claim<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Claiming,
                lease: ::core::option::Option<&'__a #p::Leasing<<Self as #p::Events<__DB>>::Token>>,
                out: &'__a mut ::std::vec::Vec<#p::Claimed<Self>>,
            ) -> impl ::core::future::Future<Output = ::core::result::Result<(), #p::sqlx::Error>>
                   + ::core::marker::Send + '__a {
                #claim
            }

            fn ack<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling,
                id: &'__a <Self as #p::QueueRow>::Id,
                held: ::core::option::Option<&'__a <Self as #p::Events<__DB>>::Token>,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#p::Settled, #p::sqlx::Error>,
            > + ::core::marker::Send + '__a {
                #ack
            }

            fn retry<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling,
                id: &'__a <Self as #p::QueueRow>::Id,
                held: ::core::option::Option<&'__a <Self as #p::Events<__DB>>::Token>,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#p::Settled, #p::sqlx::Error>,
            > + ::core::marker::Send + '__a {
                #retry
            }

            fn retry_after<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling,
                id: &'__a <Self as #p::QueueRow>::Id,
                held: ::core::option::Option<&'__a <Self as #p::Events<__DB>>::Token>,
                delay: ::core::time::Duration,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#p::Settled, #p::sqlx::Error>,
            > + ::core::marker::Send + '__a {
                #retry_after_call
            }

            fn discard<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling,
                id: &'__a <Self as #p::QueueRow>::Id,
                held: ::core::option::Option<&'__a <Self as #p::Events<__DB>>::Token>,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#p::Settled, #p::sqlx::Error>,
            > + ::core::marker::Send + '__a {
                #discard
            }

            fn dead_letter<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling,
                id: &'__a <Self as #p::QueueRow>::Id,
                held: ::core::option::Option<&'__a <Self as #p::Events<__DB>>::Token>,
                destination: &'__a str,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#p::Settled, #p::sqlx::Error>,
            > + ::core::marker::Send + '__a {
                #dead_letter
            }

            fn extend<'__a>(
                conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
                cx: &'__a #p::Settling,
                id: &'__a <Self as #p::QueueRow>::Id,
                held: &'__a <Self as #p::Events<__DB>>::Token,
                until: &'__a <Self as #p::Events<__DB>>::Token,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#p::Settled, #p::sqlx::Error>,
            > + ::core::marker::Send + '__a {
                #extend
            }
        }
    }
}
