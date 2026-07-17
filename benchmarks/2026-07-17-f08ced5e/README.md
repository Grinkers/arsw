# APSW speedtest baseline: 2026-07-17

Source commit: `f08ced5e5b264955ea4c48182da4be2e13072113`

The tracked worktree was clean before adding this baseline.  APSW was built
from this commit using the repository's canonical bundled SQLite 3.53.3
configuration and a uv-managed CPython 3.13.14 environment.

## Comparison baseline

The 11-iteration run pinned to CPU 7 is the baseline for future comparisons.
Times are median seconds with sample standard deviation in parentheses; lower
is better.

| Test | Elapsed | CPU |
| --- | ---: | ---: |
| `apsw_bigstmt` | 1.063 (0.005) | 1.059 (0.005) |
| `apsw_statements` | 1.191 (0.005) | 1.187 (0.005) |
| `apsw_statements_nobindings` | 1.280 (0.007) | 1.275 (0.007) |

The elapsed and CPU values differ by 4–5 ms, indicating little scheduler wait
during these in-memory runs.  Relative standard deviations are approximately
0.5%, 0.4%, and 0.5%, respectively.

## Default run

The initially requested command used the default four iterations without CPU
affinity:

| Test | Elapsed | CPU |
| --- | ---: | ---: |
| `apsw_bigstmt` | 1.040 (0.016) | 1.036 (0.016) |
| `apsw_statements` | 1.182 (0.027) | 1.177 (0.027) |
| `apsw_statements_nobindings` | 1.264 (0.022) | 1.258 (0.022) |

The pinned run is slightly slower but substantially less variable.  This is
consistent with dynamic frequency behavior and normal run-to-run variation;
it is not evidence of a code-level difference.

## Correctness

The reduced correctness run returned 1,349 rows from each mode and reported:

```text
apsw_bigstmt == apsw_statements True
apsw_statements == apsw_statements_nobindings True
```

## Initial investigation areas

1. Statement-cache hashing and linear lookup in
   `src/statementcache.c:194-235`, especially cache-miss-heavy SQL.
2. Cache insertion, eviction, reset, and finalization in
   `src/statementcache.c:117-164`.
3. Per-execute cursor state and mutex acquisition in `src/cursor.c:594-673`
   and `src/cursor.c:1253-1355`.
4. Sequence binding validation and conversion in `src/cursor.c:824-940`.
5. Python scalar callback conversion and dispatch in
   `src/connection.c:3133-3203`.  At scale 10, the bindings workload invokes
   `number_name` about 200,000 times during timing.
6. Per-row tuple/value conversion in `src/cursor.c:1578-1655` and
   `src/cursor.c:366-433`.
7. Multi-statement suffix preparation in `src/statementcache.c:365-390`.
8. Timed close and garbage collection in `apsw/speedtest.py:447-454`.

Useful first experiments are cache-size sweeps (`0`, `1`, `128`, `512`),
cache statistics collected outside timing, and focused microbenchmarks that
separate binding conversion, Python callbacks, row conversion, and teardown.
