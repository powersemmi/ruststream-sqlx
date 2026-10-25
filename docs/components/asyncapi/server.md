---
sidebar_position: 1
title: Server
description: "The server is the pool's host and port with the protocol postgres or mysql, SQLite in process with no host, an AnyPool by its scheme, never a credential."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server is the pool's host and port with the protocol `postgres` or `mysql`, SQLite in process with no host, an `AnyPool` by its scheme, never a credential.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
On Postgres, MySQL and MariaDB the server is read off the URL the broker was made with, or off its pool's connect options: the `host` is the host and port, and the `protocol` is `postgres` or `mysql`.
The user, the password and the database name stay out of it.
SQLite runs inside the service, so its server names the protocol `sqlite` and no host: neither the file's path nor any other part of the URL reaches the document.
An `AnyPool` is described by the scheme of its URL, as the broker of that database describes itself.

The document is published and shared, so a password in the database's URL stays in the broker.

## Example

The broker builds its pool from the URL a deployment configures, password included; the document's server is `db.internal:5432` with the protocol `postgres`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_asyncapi.rs#server"
```

</TabItem>
</Tabs>
