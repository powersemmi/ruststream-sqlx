use ruststream_sqlx::Inbox;

#[derive(Inbox)]
#[inbox(table = "jobs")]
#[sqlx(rename_all = "camel")]
struct UnknownCasing {
    #[field(id)]
    job_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct EmptyRename {
    #[field(id)]
    #[sqlx(rename = "")]
    job_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct RoleOnSkipped {
    #[field(id)]
    #[sqlx(skip)]
    job_id: i64,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct RoleOnFlattened {
    #[field(id)]
    job_id: i64,
    #[field(payload)]
    #[sqlx(flatten)]
    body: Body,
}

#[derive(Inbox)]
#[inbox(table = "jobs")]
struct GeneratedOnSkipped {
    #[field(id)]
    job_id: i64,
    #[field(generated)]
    #[sqlx(skip)]
    created_at: i64,
}

struct Body {
    bytes: Vec<u8>,
}

fn main() {}
