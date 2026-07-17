# Statement cache lookup optimization: 2026-07-17

Source baseline: `22ddc459` (`f08ced5e` plus benchmark documentation)

This experiment adds an exact negative filter for statement-cache hash
lookups and probes the newest cache entry before falling back to the existing
linear scan.

## Implementation

- Caches with at least 64 entries allocate 1,024 `unsigned short` hash-bucket
  counters, adding 2 KiB per connection.
- Statement wrappers retain one bucket-membership field.  Structure alignment
  adds 8 bytes per active or cached statement on this 64-bit build: 1 KiB at
  the default cache size and 4 KiB at the maximum size.
- A zero bucket count proves that the requested hash is not cached, so lookup
  can skip the linear scan.  Collisions always use the existing hash, SQL
  length, SQL bytes, and statement-options comparisons.
- A cache-hit statement keeps its bucket count reserved while in use.  This is
  safe because it can only cause an unnecessary fallback scan during
  overlapping use, and it avoids counter writes on the hot hit path.
- Repeatedly used statements are returned to the newest circular-cache slot,
  which is probed first.
- Caches smaller than 64 entries retain the original lookup path and do not
  allocate counters.

## Speedtest result

Both builds used the same uv environment, source tree, build options, logical
CPU 7, two warmup iterations, and 11 measured iterations.  Times are median
seconds; lower is better.

| Test | Before elapsed | After elapsed | Change | Before CPU | After CPU | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `apsw_bigstmt` | 1.080 | 1.066 | -1.3% | 1.075 | 1.062 | -1.2% |
| `apsw_statements` | 1.215 | 1.192 | -1.9% | 1.210 | 1.187 | -1.9% |
| `apsw_statements_nobindings` | 1.305 | 1.270 | -2.7% | 1.300 | 1.265 | -2.7% |

The targeted `statements` and `statements_nobindings` cases both exceed the
1.5% acceptance threshold.  No speedtest case regressed.

## Focused results

CPU nanoseconds per `Cursor.execute` are medians from 11 measured blocks after
three discarded warmup blocks.

Repeated cache hit after filling the cache:

| Cache size | Before | After |
| ---: | ---: | ---: |
| 1 | 132.9 | 125.7 |
| 128 | 160.8 | 133.9 |
| 512 | 208.2 | 130.7 |

The size-1 path is unchanged by the optimization and acts as a frequency
control.  Relative to that control, the additional cost of a size-128 cache
fell from 21% to 7%, and the size-512 cost fell from 57% to 4%.

Unique SQL cache misses:

| Cache size | Before | After |
| ---: | ---: | ---: |
| 0 | 750.5 | 725.9 |
| 128 | 1,100.2 | 1,064.8 |
| 512 | 1,231.1 | 1,153.9 |

The cache-disabled lane is the frequency control.  Most remaining miss cost
comes from reset, insertion, eviction, and finalization rather than the linear
hash scan.

## Verification

- Speedtest correctness returned 1,349 identical rows in all three modes.
- `.venv-benchmark/bin/python -m apsw.tests` passed all 227 tests.
- Cache hit, miss, and eviction statistics were unchanged in focused runs.
- The release extension built successfully with bundled SQLite 3.53.3.

Raw speedtest results are in `speedtest-before.txt` and
`speedtest-after.txt`.  Apply the accepted change to the source baseline with:

```sh
git apply benchmarks/2026-07-17-statement-cache/statement-cache.patch
```
