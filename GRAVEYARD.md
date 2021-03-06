# bigquery-etl Code Graveyard

This document records interesting code that we've deleted for the sake of discoverability for the future.

## Smoot Usage v1

- [Removal PR](https://github.com/mozilla/bigquery-etl/pull/460)

The `smoot_usage_*_v1*` tables used a python file to generate the desktop,
nondesktop, and FxA variants, but have been replaced by v2 tables that make
some different design decisions. One of the main drawbacks of v1 was that
we had to completely recreate the final `smoot_usage_all_mtr` table for all
history every day, which had started to take on order 1 hour to run. The
v2 tables instead define a `day_0` view and a `day_13` view and relies on
the Growth and Usage Dashboard (GUD) to query them separately and join the
results together at query time.
