use ruststream_sqlx::SqlxBroker;
use sqlx::MySqlPool;

fn broker(pool: MySqlPool) -> SqlxBroker<sqlx::MySql> {
    SqlxBroker::new(pool).listen_notify()
}

fn main() {
    let _ = broker;
}
