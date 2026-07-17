---
description: Query across multiple SMDT databases with plain SQL using MultiStore. Attach per-dataset Postgres databases into one DuckDB connection and join or union across platforms.
---

# Cross-Platform Analysis with MultiStore

SMDT stores every dataset in its own Postgres database. That's great for isolation, but if you have a `twitter_db` and a `bluesky_db` and want to compare accounts, posts, or networks across them, you need a way to query more than one database at once.

`MultiStore` attaches multiple SMDT-standardized databases into one [DuckDB](https://duckdb.org/) connection, so you can write ordinary SQL joins and unions across them. This works cleanly because every SMDT dataset shares the exact same schema (`accounts`, `posts`, `entities`, `actions`, ...) regardless of platform -- see the project README's Data Model section for the full column list per table.

## Basic Usage

```python
from smdt.multistore import MultiStore

with MultiStore() as ms:
    ms.attach("twitter", db_name="twitter_db")
    ms.attach("bluesky", db_name="bluesky_db")

    # Each attached dataset shows up as a schema, named after its alias.
    df = ms.query("""
        SELECT tw.username, tw.follower_count AS tw_followers, bs.follower_count AS bs_followers
        FROM twitter.accounts tw
        JOIN bluesky.accounts bs ON tw.username = bs.username
    """)
    print(df)
```

`attach()` reuses `DBConfig` (the same connection settings `StandardDB` uses), so by default it reads `DB_HOST`/`DB_USER`/`DB_PASSWORD`/`DB_PORT` from your environment. Pass `cfg=DBConfig(...)` explicitly if different datasets live on different hosts or credentials:

```python
from smdt.config import DBConfig
from smdt.multistore import MultiStore

ms = MultiStore()
ms.attach("twitter", db_name="twitter_db", cfg=DBConfig(host="db1.internal", user="researcher", password="..."))
ms.attach("bluesky", db_name="bluesky_db", cfg=DBConfig(host="db2.internal", user="researcher", password="..."))
```

`MultiStore` attaches everything **read-only** by default -- it's for cross-dataset analysis, not writes. Writes belong to each dataset's own `StandardDB`.

## Combining Datasets

Because every dataset shares the same schema, a common pattern is unioning the same table across datasets rather than joining:

```python
df = ms.query("""
    SELECT platform, body FROM twitter.posts
    UNION ALL
    SELECT platform, body FROM bluesky.posts
""")
```

Since `posts.platform` is already set per-row during standardization, you don't need to track which dataset a row came from separately -- it's already in the data.

## Cross-Platform Identity Linking

`MultiStore` doesn't attempt to automatically match an account on one platform to an account on another -- that's a research decision, not infrastructure. Whatever matching signal you have (exact `username` match, a manually curated crosswalk table, a fuzzy-matching result you've computed separately) is just another join condition:

```python
# Exact match
df = ms.query("""
    SELECT tw.account_id AS twitter_id, bs.account_id AS bluesky_id
    FROM twitter.accounts tw
    JOIN bluesky.accounts bs ON lower(tw.username) = lower(bs.username)
""")

# Using a crosswalk table you've loaded separately (e.g. from a CSV/Parquet file)
ms.connection.sql("CREATE TABLE crosswalk AS SELECT * FROM read_csv('crosswalk.csv')")
df = ms.query("""
    SELECT c.*, tw.follower_count AS tw_followers, bs.follower_count AS bs_followers
    FROM crosswalk c
    JOIN twitter.accounts tw ON tw.account_id = c.twitter_id
    JOIN bluesky.accounts bs ON bs.account_id = c.bluesky_id
""")
```

`ms.connection` exposes the underlying DuckDB connection for anything not covered by `query()` -- reading Parquet/CSV files directly, `.pl()` for a polars DataFrame instead of pandas, or building intermediate tables like the crosswalk example above.

## PostGIS `location` Columns

`accounts.location`/`posts.location` are PostGIS `geometry` columns. DuckDB's Postgres scanner doesn't understand the PostGIS wire type, so a normal attached query returns opaque raw bytes:

```python
ms.query("SELECT location FROM twitter.accounts")
# location column comes back as raw bytes, not usable coordinates
```

Use `raw()` to run PostGIS functions on the Postgres side, before the value ever crosses into DuckDB:

```python
df = ms.raw("twitter", "SELECT account_id, ST_AsText(location) AS location_wkt FROM accounts")
```

`raw()` runs against a single attached dataset's own Postgres connection (not spread across others), so it's the right tool specifically for this kind of Postgres-side-only operation -- for everything else, `query()` is what you want.

## Detaching and Cleanup

```python
ms.detach("bluesky")   # drop one dataset, keep others attached
ms.close()             # or use `with MultiStore() as ms:` to do this automatically
```
