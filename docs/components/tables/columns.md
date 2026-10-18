---
sidebar_position: 2
title: Columns
description: "A column is named in one place, sqlx's attributes: rename, rename_all, raw identifiers and skip; a flattened field makes statements select every column."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Columns

A column is named in one place, sqlx's attributes: `rename`, `rename_all`, raw identifiers and `skip`; a flattened field makes statements select every column.

The derive reads a field's column name the way `sqlx::FromRow` reads it, so the statements and the struct never disagree.
`#[sqlx(rename = "..")]` names a field's column as written.
`#[sqlx(rename_all = "..")]` on the struct recases every other field's name.
A raw identifier loses its `r#`: `r#type` reads the column `type`.
A `#[sqlx(skip)]` field reads no column and plays no role.
The other options of `#[sqlx(..)]`, such as `json`, `try_from` and `default`, belong to sqlx's derive and pass through untouched.

The dialect quotes every name in the statements it builds, so a name that needs quoting, a keyword or mixed case, reaches the database intact.
A `#[sqlx(flatten)]` field reads columns the derive cannot see, so the statements select `*`.
A dead-letter move into another table then copies the row by position, and the dead-letter table has the same columns in the same order.
The [headers layout](../headers/inbox-headers.md) is the exception: there a flattened field holds the headers struct, whose columns the derive sees.

By hand, the description names each column as the table writes it, with [`Column::new`](api:ruststream_sqlx_dialect::Column::new), and `.selecting_all()` reads the table with `*`.

## Example

A queue in a table another system created: its id column is `JobID`, its group column is the keyword `type`, and its other columns are in camel case.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/sqlx_tables_columns.rs#table"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/sqlx_tables_columns.rs#table"
```

</TabItem>
</Tabs>
