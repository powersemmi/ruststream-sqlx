---
sidebar_position: 9
title: Switch
description: "A test build leaves the outbox off, so a test needs no outbox table; RUSTSTREAM_SQLX_OUTBOX=on turns it on for the whole test process."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Switch

A test build leaves the outbox off, so a test needs no outbox table; `RUSTSTREAM_SQLX_OUTBOX=on` turns it on for the whole test process.

A build with the `testing` feature leaves the outbox off: the two middlewares pass every message untouched, `wrap` sends straight to the publisher it wraps, and the republish sends nothing.
A test of the handlers then runs on the service's production app with no outbox table and no database for it.
A production build always tracks, and the switch is compiled out.

`RUSTSTREAM_SQLX_OUTBOX=on` in the environment turns the outbox on in a test build, and any other value leaves it off.
The variable is read once per process, the first time the outbox runs, and tests share one process, so it is set outside them: in a `just` recipe, a CI job or a nextest profile.
A test of tracking itself runs only with the switch on and returns early without it; a job that sets `RUSTSTREAM_REQUIRE_LIVE` then fails such a test instead of passing it.
The `testing` feature also reaches `cargo run --example` and `cargo build --all-targets` of a crate that enables it, so an example among the outbox's pages runs with the switch set.

## Example

The test drives the service of the [Transactional outbox](./index.md) page over a database that has no tables, and runs only without the switch: the order reaches its consumer and carries no record id.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_outbox_switch.rs#switch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_outbox_switch.rs#switch"
```

</TabItem>
</Tabs>
