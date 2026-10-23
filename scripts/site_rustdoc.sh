#!/usr/bin/env bash
# Builds the rustdoc the site's API links resolve against, into target/doc: this crate and the
# dialect crate it re-exports whole, with every feature, as docs.rs renders them, and the core
# release the lock file pins with every feature, as docs.rs renders the core. A page may link
# items of each; the derives have pages of their own in this crate.
set -euo pipefail
cd "$(dirname "$0")/.."
crates=(ruststream-sqlx ruststream-sqlx-dialect)
features="$(cargo metadata --locked --format-version 1 | python3 -c '
import json, sys
crates = sys.argv[1:]
packages = {p["name"]: p for p in json.load(sys.stdin)["packages"]}
names = [
    f"{name}/{feature}"
    for name in crates + ["ruststream"]
    for feature in packages[name]["features"]
    if feature != "default"
]
print(",".join(names))
' "${crates[@]}")"
packages=()
for crate in "${crates[@]}" ruststream; do
  packages+=(-p "$crate")
done
cargo doc --locked --no-deps "${packages[@]}" --features "$features"
