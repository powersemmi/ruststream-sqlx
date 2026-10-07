use ruststream_sqlx::outbox;

struct OrderEvent;
struct RefundEvent;

fn main() {
    let _ = outbox! {
        "orders" => OrderEvent,
        "refunds" => RefundEvent,
        "orders" => RefundEvent,
    };
}
