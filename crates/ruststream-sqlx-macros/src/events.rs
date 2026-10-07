//! The row's contract with the broker beyond its description: the hidden `Events<DB>`, one
//! implementation for every database the field types allow.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use ruststream_sqlx_dialect::Role;
use syn::{DeriveInput, Generics, Path, Type, WherePredicate, parse_quote};

use crate::parse::{Custom, Field, Inbox, playing};

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
fn kinds(inbox: &Inbox<'_>, row: &RowParts, id_ty: &Type, own_code: bool) -> TokenStream2 {
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
    let retry_after = row.retry_after.as_ref().map(|role| {
        let time = &role.time;
        quote!(.retry_after::<#time>())
    });
    let processed_at = row.processed_at.as_ref().map(|role| {
        let time = &role.time;
        quote!(.processed_at::<#time>())
    });
    let locked_until = row
        .lease
        .as_ref()
        .map(|lease| quote!(.locked_until::<#lease>()));
    let clock = &row.clock;
    quote! {
        #p::KindsOf::new::<#id_ty, #clock>()
            #payload #headers #key #attempt #retry_after #processed_at #locked_until
            .finish()
    }
}

/// The bounds every row's contract starts from: the database, the row's decoding, and its id,
/// which binds into every settlement and which the claim reads alone from a row whose other
/// columns do not decode.
pub(crate) fn base_predicates(id_ty: &Type) -> Vec<WherePredicate> {
    let p = quote!(::ruststream_sqlx::__private);
    vec![
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
    ]
}

/// What a struct's own columns give the contract: the bodies of the per-row methods and the
/// binder's arms, for the row type `row` (`Self` in a flat struct, the message in a headers
/// struct's), with the bounds their types put on the database pushed onto `predicates`.
pub(crate) struct RowParts {
    pub(crate) clock: Path,
    pub(crate) token: TokenStream2,
    pub(crate) headers: TokenStream2,
    pub(crate) unfit_header: TokenStream2,
    pub(crate) key: TokenStream2,
    pub(crate) attempt: TokenStream2,
    pub(crate) read_attempt: TokenStream2,
    pub(crate) leasing: TokenStream2,
    retry_after: Option<TimeRole>,
    processed_at: Option<TimeRole>,
    retry_after_arms: Option<TokenStream2>,
    processed_at_arms: Option<TokenStream2>,
    lease_arms: Option<TokenStream2>,
    /// The lease's time, in the lease form.
    lease: Option<TokenStream2>,
}

/// The time a lease is written in, where the struct has a `locked_until` field.
pub(crate) enum LeaseTime {
    /// The time a path names: the struct's `LeaseRow::Lease`.
    Named(TokenStream2),
    /// The time the field's type binds in the database, through `TimeFor`.
    OfField,
}

/// The per-row half of the contract, built for the row type `row`; `lease` names the time a
/// lease is written in, read where the struct has a `locked_until` field.
// One generator for the per-row half: the parts share the bounds they build.
#[allow(clippy::too_many_lines)]
pub(crate) fn row_parts(
    inbox: &Inbox<'_>,
    row: &TokenStream2,
    lease: LeaseTime,
    predicates: &mut Vec<WherePredicate>,
) -> RowParts {
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let clock: Path = inbox
        .table
        .clock
        .clone()
        .unwrap_or_else(|| parse_quote!(::ruststream_sqlx::SystemClock));

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
        playing(inbox, Role::RetryAfter).map(|field| TimeRole::new(field.ty, predicates));
    let processed_at =
        playing(inbox, Role::ProcessedAt).map(|field| TimeRole::new(field.ty, predicates));
    let retry_after_arms = retry_after.as_ref().map(|role| {
        let time = &role.time;
        // The take of an advisory claim names its row while it is still due, as the claim does.
        quote! {
            (#p::Param::Now, #p::Event::Claim | #p::Event::Take) => {
                #p::put::<__DB, _>(arguments, #p::now::<#clock, #time, __DB, #row>(values)?)?;
                true
            }
            (#p::Param::RetryAfter, #p::Event::RetryAfter) => {
                #p::put::<__DB, _>(arguments, #p::later::<#clock, #time, __DB, #row>(values)?)?;
                true
            }
        }
    });
    let processed_at_arms = processed_at.as_ref().map(|role| {
        let time = &role.time;
        quote! {
            (#p::Param::Now, #p::Event::Ack | #p::Event::Discard) => {
                #p::put::<__DB, _>(arguments, #p::now::<#clock, #time, __DB, #row>(values)?)?;
                true
            }
        }
    });

    // The lease form: the expiry a claim writes and a settlement matches is the token, in the
    // type the `locked_until` field holds. A claim reads "now" once, takes its lease from that
    // instant and binds it wherever its statements compare a time.
    let lease_field = playing(inbox, Role::LockedUntil);
    let leased = lease_field.is_some();
    // A headers struct names the lease's time through the field's type, as the time roles do: a
    // bound on a projection of the struct's own lease trait does not hold inside the impl that the
    // message's `Events` requires.
    let lease = match (lease, lease_field) {
        (LeaseTime::Named(lease), _) => lease,
        (LeaseTime::OfField, Some(field)) => TimeRole::new(field.ty, predicates).time,
        (LeaseTime::OfField, None) => quote!(()),
    };
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
    RowParts {
        clock,
        token,
        headers,
        unfit_header,
        key,
        attempt,
        read_attempt,
        leasing,
        retry_after,
        processed_at,
        retry_after_arms,
        processed_at_arms,
        lease_arms,
        lease: leased.then_some(lease),
    }
}

impl RowParts {
    /// The binder's body: the arms every row binds, the struct's own, and `extra`.
    pub(crate) fn bind(&self, extra: Option<&TokenStream2>) -> TokenStream2 {
        let p = quote!(::ruststream_sqlx::__private);
        let Self {
            retry_after_arms,
            processed_at_arms,
            lease_arms,
            ..
        } = self;
        quote! {
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
                (#p::Param::Key, _) => match values.key {
                    ::core::option::Option::Some(key) => {
                        <__DB as #p::QueueDatabase>::bind_str(arguments, key)?;
                        true
                    }
                    ::core::option::Option::None => false,
                },
                #retry_after_arms
                #processed_at_arms
                #lease_arms
                #extra
                _ => false,
            })
        }
    }
}

/// The events of a row's contract: which the service implements itself, and the bodies of the
/// event methods, which run the crate's default or the service's own.
pub(crate) struct EventParts {
    /// The switch of each event `custom(..)` lists, in `Shape` order.
    flags: [bool; 10],
    /// The binder's arm for the ids a claim of the service's own returned.
    pub(crate) ids_arm: Option<TokenStream2>,
    /// The event methods, `claim` to `candidates`.
    pub(crate) methods: TokenStream2,
}

impl EventParts {
    /// Whether an event is the service's own.
    pub(crate) fn own_code(&self) -> bool {
        self.flags.contains(&true)
    }

    /// The `SHAPE` constant.
    pub(crate) fn shape(&self) -> TokenStream2 {
        let p = quote!(::ruststream_sqlx::__private);
        let [
            c_claim,
            c_fetch,
            c_ack,
            c_retry,
            c_retry_after,
            c_discard,
            c_dead,
            c_extend,
            c_lock_event,
            c_unlock_event,
        ] = self.flags;
        quote! {
            const SHAPE: #p::Shape = #p::Shape {
                custom_claim: #c_claim,
                custom_fetch: #c_fetch,
                custom_ack: #c_ack,
                custom_retry: #c_retry,
                custom_retry_after: #c_retry_after,
                custom_discard: #c_discard,
                custom_dead_letter: #c_dead,
                custom_extend: #c_extend,
                custom_lock: #c_lock_event,
                custom_unlock: #c_unlock_event,
            };
        }
    }
}

/// The hidden `Events<DB>` implementation of a flat struct.
pub(crate) fn events(
    input: &DeriveInput,
    generics: &Generics,
    inbox: &Inbox<'_>,
    id: &Field<'_>,
) -> TokenStream2 {
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let name = &input.ident;
    let id_ident = id.ident;
    let id_ty = id.ty;
    let mut predicates = base_predicates(id_ty);
    let row = row_parts(
        inbox,
        &quote!(Self),
        LeaseTime::Named(quote!(<Self as #r::LeaseRow>::Lease)),
        &mut predicates,
    );
    let events = event_parts(inbox.table.custom, id_ty, &mut predicates);
    let kinds = kinds(inbox, &row, id_ty, events.own_code());
    let shape = events.shape();
    let EventParts {
        ids_arm, methods, ..
    } = &events;
    let bind = row.bind(ids_arm.as_ref());
    let RowParts {
        token,
        headers,
        unfit_header,
        key,
        attempt,
        read_attempt,
        leasing,
        ..
    } = &row;

    let mut generics = generics.clone();
    generics.params.push(parse_quote!(__DB));
    generics.make_where_clause().predicates.extend(predicates);
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics #p::Events<__DB> for #name #ty_generics #where_clause {
            #shape

            type Token = #token;

            type Headers = #p::HeaderMap;

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
                #bind
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

            #methods
        }
    }
}

/// The event half of the contract: the service's own events listed in `custom`, the bounds they
/// put on the row pushed onto `predicates`. `id_ty` is the type of the row's id.
// One generator for the event half: the parts share the bounds they build.
#[allow(clippy::too_many_lines)]
pub(crate) fn event_parts(
    custom: Custom,
    id_ty: &Type,
    predicates: &mut Vec<WherePredicate>,
) -> EventParts {
    let p = quote!(::ruststream_sqlx::__private);
    let r = quote!(::ruststream_sqlx);
    let claimed = custom.claim.is_some();
    let via = |ty: &Type| quote!(<#ty as #p::Via<__DB>>::Is);

    // Matching fetched rows to claimed ids compares ids.
    if claimed || custom.fetch {
        let column = via(id_ty);
        predicates.push(parse_quote!(#column: ::core::cmp::PartialEq));
    }
    // The crate's claim of ids collects them through sqlx, which needs them `Unpin`.
    if custom.fetch && !claimed {
        let column = via(id_ty);
        predicates.push(parse_quote!(#column: ::core::marker::Unpin));
    }
    // The crate's fetch binds the ids a claim of the service's own returned.
    let ids_arm = (claimed && !custom.fetch).then(|| {
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
    event_bound(claimed, quote!(Claim), predicates);
    event_bound(custom.fetch, quote!(Fetch), predicates);
    event_bound(custom.ack, quote!(Ack), predicates);
    event_bound(custom.retry, quote!(Retry), predicates);
    event_bound(custom.retry_after, quote!(RetryAfter), predicates);
    event_bound(custom.discard, quote!(Discard), predicates);
    event_bound(custom.dead_letter, quote!(DeadLetter), predicates);
    event_bound(custom.extend.is_some(), quote!(Extend), predicates);
    event_bound(custom.lock.is_some(), quote!(Lock), predicates);
    event_bound(custom.unlock.is_some(), quote!(Unlock), predicates);

    let claim = match (claimed, custom.fetch) {
        (false, false) => quote!({
            // A claim of whole rows reads no ids.
            let _ = ids;
            #p::claim_rows::<__DB, Self>(conn, cx, lease, out)
        }),
        (claim, fetch) => {
            // The crate's claim reads the ids into the subscription's buffer; the service's
            // claim returns a vector of its own, which takes the buffer's place for this claim.
            let ids = if claim {
                quote! {
                    let mut own = <Self as #r::Claim<__DB>>::claim(&mut *conn, cx.queue.name, cx.limit)
                        .await?;
                    let _ = ids;
                    let ids = &mut own;
                }
            } else {
                quote! {
                    ids.clear();
                    #p::claim_ids::<__DB, Self>(&mut *conn, cx, lease, ids).await?;
                }
            };
            // The service's fetch decodes its own rows; the crate's hands over those it could
            // not decode too.
            let (rows, matched) = if fetch {
                (
                    quote!(<Self as #r::Fetch<__DB>>::fetch(&mut *conn, ids).await?),
                    quote!(match_rows),
                )
            } else {
                (
                    quote!(#p::fetch_by_ids::<__DB, Self>(&mut *conn, cx, lease, ids).await?),
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
                #ids
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
    // The service's own fetch reads a taken row of the advisory lock form, as it reads the rows
    // of a claim; the take tells it the row is still claimable.
    let take = if custom.fetch {
        quote!(async move {
            if !#p::take_id::<__DB, Self>(&mut *conn, cx, id).await? {
                return ::core::result::Result::Ok(false);
            }
            let rows = <Self as #r::Fetch<__DB>>::fetch(&mut *conn, ::core::slice::from_ref(id))
                .await?;
            #p::match_taken::<__DB, Self>(id, rows, out);
            ::core::result::Result::Ok(true)
        })
    } else {
        quote!(#p::take::<__DB, Self>(conn, cx, id, out))
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
    // The service's own lock and unlock name the key alone; the crate's bind it into the
    // statements its dialect built.
    let lock_event = if custom.lock.is_some() {
        quote!(async move {
            let _ = cx;
            <Self as #r::Lock<__DB>>::lock(conn, key).await
        })
    } else {
        quote!(#p::lock::<__DB, Self>(conn, cx, key))
    };
    let unlock_event = if custom.unlock.is_some() {
        quote!(async move {
            let _ = cx;
            <Self as #r::Unlock<__DB>>::unlock(conn, key).await
        })
    } else {
        quote!(#p::unlock::<__DB, Self>(conn, cx, key))
    };

    let flags = [
        claimed,
        custom.fetch,
        custom.ack,
        custom.retry,
        custom.retry_after,
        custom.discard,
        custom.dead_letter,
        custom.extend.is_some(),
        custom.lock.is_some(),
        custom.unlock.is_some(),
    ];
    // Only the crate's claim of ids for a fetch of the service's own keeps ids between its claim
    // and its fetch: the subscription owns their buffer, and a table that reads rows whole, or
    // claims with the service's own `Claim`, keeps none.
    let ids_buffer = if custom.fetch && !claimed {
        quote!(::std::vec::Vec<<Self as #p::QueueRow>::Id>)
    } else {
        quote!(())
    };
    let methods = quote! {
        type Ids = #ids_buffer;

        fn claim<'__a>(
            conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
            cx: &'__a #p::Claiming,
            lease: ::core::option::Option<&'__a #p::Leasing<<Self as #p::Events<__DB>>::Token>>,
            ids: &'__a mut <Self as #p::Events<__DB>>::Ids,
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

        fn lock<'__a>(
            conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
            cx: &'__a #p::Claiming,
            key: &'__a str,
        ) -> impl ::core::future::Future<
            Output = ::core::result::Result<bool, #p::sqlx::Error>,
        > + ::core::marker::Send + '__a {
            #lock_event
        }

        fn unlock<'__a>(
            conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
            cx: &'__a #p::Settling,
            key: &'__a str,
        ) -> impl ::core::future::Future<
            Output = ::core::result::Result<bool, #p::sqlx::Error>,
        > + ::core::marker::Send + '__a {
            #unlock_event
        }

        fn take<'__a>(
            conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
            cx: &'__a #p::Claiming,
            id: &'__a <Self as #p::QueueRow>::Id,
            out: &'__a mut ::std::vec::Vec<#p::Claimed<Self>>,
        ) -> impl ::core::future::Future<
            Output = ::core::result::Result<bool, #p::sqlx::Error>,
        > + ::core::marker::Send + '__a {
            #take
        }

        fn candidates<'__a>(
            conn: &'__a mut <__DB as #p::sqlx::Database>::Connection,
            cx: &'__a #p::Claiming,
            out: &'__a mut #p::Candidates<<Self as #p::QueueRow>::Id>,
        ) -> impl ::core::future::Future<
            Output = ::core::result::Result<(), #p::sqlx::Error>,
        > + ::core::marker::Send + '__a {
            #p::candidates::<__DB, Self>(conn, cx, out)
        }
    };
    EventParts {
        flags,
        ids_arm,
        methods,
    }
}
