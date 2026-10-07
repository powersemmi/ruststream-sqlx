use ruststream_sqlx::Outbox;

#[derive(Outbox)]
#[outbox(table = "outbox")]
struct NoRoles {
    headers: Option<String>,
}

#[derive(Outbox)]
#[outbox(table = "outbox")]
struct TwoRolesOnAField {
    #[field(id)]
    id: i64,
    #[field(name, payload)]
    name: String,
}

#[derive(Outbox)]
#[outbox(table = "outbox")]
struct RoleOnTwoFields {
    #[field(id)]
    id: i64,
    #[field(name)]
    name: String,
    #[field(name)]
    channel: String,
    #[field(payload)]
    payload: Vec<u8>,
}

#[derive(Outbox)]
#[outbox(table = "outbox")]
struct InboxRole {
    #[field(id)]
    id: i64,
    #[field(retry_after)]
    due: i64,
}

fn main() {}
