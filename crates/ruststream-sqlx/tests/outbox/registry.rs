//! The registry's own rules, which hold with the switch off: a name registers once, and the pool is
//! set once.

use ruststream::memory::MemoryPublisher;
use ruststream_sqlx::outbox::Outbox;
use sqlx::Sqlite;
use sqlx::sqlite::SqlitePoolOptions;

use crate::records::{OrderRecord, RefundRecord};

#[test]
#[should_panic(expected = "`orders` is registered with the outbox twice")]
fn a_name_registered_twice_panics_naming_it() {
    let _ = Outbox::<Sqlite>::deferred()
        .register::<OrderRecord>("orders")
        .register::<RefundRecord>("refunds")
        .register::<RefundRecord>("orders");
}

#[test]
#[should_panic(expected = "`audits` is not registered with the outbox")]
fn republishing_a_name_not_registered_panics_naming_it() {
    let tracking = Outbox::<Sqlite>::deferred().register::<OrderRecord>("orders");
    let _ = tracking.republish_names::<MemoryPublisher>(["orders", "audits"]);
}

#[tokio::test]
async fn a_second_pool_is_refused() {
    let pool = SqlitePoolOptions::new()
        .connect_lazy("sqlite::memory:")
        .expect("the URL parses");
    let tracking = Outbox::new(pool.clone()).register::<OrderRecord>("orders");
    assert!(
        tracking.set_pool(pool.clone()).is_err(),
        "a pool given to `new` is set"
    );

    let deferred = Outbox::<Sqlite>::deferred().register::<OrderRecord>("orders");
    deferred
        .set_pool(pool.clone())
        .expect("the first pool is taken");
    assert!(
        deferred.set_pool(pool).is_err(),
        "the second pool is refused"
    );
}
