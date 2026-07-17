# GCC profile-guided optimization: 2026-07-17

Source baseline: `1a4b505f` (`22ddc459` source plus round-one benchmark
findings)

This experiment builds APSW and bundled SQLite with GCC profile generation,
trains the instrumented extension, and rebuilds it using the collected profile.

## Result

Both builds used the same source, uv CPython 3.13.14 environment, GCC 15.2.1,
SQLite configuration, logical CPU 7, two warmup iterations, and 11 measured
iterations.  Times are median seconds; lower is better.

| Test | Baseline elapsed | PGO elapsed | Change | Baseline CPU | PGO CPU | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `apsw_bigstmt` | 1.064 | 0.958 | -10.0% | 1.060 | 0.955 | -9.9% |
| `apsw_statements` | 1.193 | 1.107 | -7.2% | 1.188 | 1.103 | -7.2% |
| `apsw_statements_nobindings` | 1.283 | 1.166 | -9.1% | 1.279 | 1.162 | -9.1% |

All three cases exceed the 1.5% acceptance threshold.

## Training

The instrumented extension used the normal interpreter compiler flags plus:

```text
-fprofile-generate=PROFILE_DIR -fprofile-update=atomic
```

Training ran:

1. The complete APSW test suite for broad API and error-path coverage.
2. Five iterations of all three `apsw.speedtest` workloads.
3. A focused workload covering executemany, integer/float/text/blob/NULL row
   conversion, and one-argument Python scalar callbacks.

The atomic profile update mode is required because the APSW tests exercise the
extension concurrently from multiple threads.

The profile-use build added:

```text
-fprofile-use=PROFILE_DIR
-fprofile-correction
-Werror=missing-profile
-Werror=coverage-mismatch
```

The build produced no missing-profile or coverage-mismatch diagnostics.  Both
`apsw` and `_unicode` profile data files were present and consumed.

## Verification

- Speedtest correctness returned 1,349 identical rows in all three modes.
- `.venv-benchmark/bin/python -m apsw.tests` passed all 227 tests.
- The final extension has the same dynamic dependencies as baseline: libc and
  the dynamic loader only.
- Instrumentation training passed all 227 tests before profile use.
- The representative training script completed successfully.

The ordinary extension was 14,162,968 bytes and the PGO extension was
10,085,672 bytes, a 28.8% reduction.  Profile files are not retained because
GCC keys them to the compiler, source, object path, and build flags.

## Integration patch

The patch adds an explicit `make pgo-gcc` workflow.  Ordinary builds are
unchanged.  The target preserves the selected Python installation's compiler
and linker flags because setting `CFLAGS` through distutils replaces them.

```sh
git apply benchmarks/2026-07-17-gcc-pgo/gcc-pgo.patch
make pgo-gcc PYTHON=.venv-benchmark/bin/python
```

This first implementation is GCC-specific and opt-in.  Clang, Apple Clang,
and MSVC require their own profile generation and use workflows.
