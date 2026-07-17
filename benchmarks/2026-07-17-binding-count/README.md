# Cached binding count optimization: 2026-07-17

Source baseline: `22ddc459` (`f08ced5e` plus benchmark documentation)

This experiment caches SQLite's immutable parameter count in each prepared
statement wrapper.

## Implementation

- Record `sqlite3_bind_parameter_count()` once after preparing a statement.
- Skip `sqlite3_clear_bindings()` for statements with no parameters.
- Skip the binding dispatcher when the statement has no parameters and the
  caller supplied no bindings.
- Use the cached value for `Cursor.bindings_count`.
- Supplied empty bindings, the internal NULL-binding sentinel, mappings, and
  multi-statement binding offsets still use the full validation path.

The additional integer increases each statement wrapper by 8 bytes on this
64-bit build because of structure alignment.

## Speedtest result

Both builds used the `22ddc459` source baseline, the same uv environment and
build options, logical CPU 7, two warmup iterations, and 11 measured
iterations.  Baseline raw output is in
`../2026-07-17-statement-cache/speedtest-before.txt`.

| Test | Before elapsed | After elapsed | Change | Before CPU | After CPU | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `apsw_bigstmt` | 1.080 | 1.068 | -1.1% | 1.075 | 1.064 | -1.0% |
| `apsw_statements` | 1.215 | 1.192 | -1.9% | 1.210 | 1.188 | -1.8% |
| `apsw_statements_nobindings` | 1.305 | 1.276 | -2.2% | 1.300 | 1.271 | -2.2% |

The targeted `statements` and `statements_nobindings` cases exceed the 1.5%
acceptance threshold.  No speedtest case regressed.

## Focused result

CPU nanoseconds per cached `Cursor.execute`, using 11 measured blocks after
three discarded warmup blocks:

| Case | Before | After | Change |
| --- | ---: | ---: | ---: |
| No parameters | 125.8 | 113.0 | -10.2% |
| One integer parameter | 149.3 | 150.0 | +0.5% |

The bound path is effectively unchanged.  The optimization specifically
removes work from zero-parameter execution.

## Verification

- Speedtest correctness returned 1,349 identical rows in all three modes.
- `.venv-benchmark/bin/python -m apsw.tests` passed all 227 tests.
- Cache hit and miss statistics were unchanged in focused runs.

Apply the accepted change independently to the source baseline with:

```sh
git apply benchmarks/2026-07-17-binding-count/binding-count.patch
```
