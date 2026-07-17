# Performance benchmarks

Benchmark environments use a uv-managed Python selected by
`.python-version`.  This keeps the Python implementation and patch release
consistent across distributions.  `benchmarks/requirements.txt` pins the
Python build tooling used outside the project's build isolation.

uv does not make the host CPU, kernel, C compiler, libc, or power policy
reproducible.  Each baseline must record those separately.

## Results

- [`2026-07-17-f08ced5e`](2026-07-17-f08ced5e/README.md): initial baseline
- [`2026-07-17-statement-cache`](2026-07-17-statement-cache/README.md):
  statement-cache lookup optimization
- [`2026-07-17-binding-count`](2026-07-17-binding-count/README.md): cached
  binding metadata and zero-parameter fast path
- [`2026-07-17-search-notes.md`](2026-07-17-search-notes.md): rejected and
  refined experiments
- [`2026-07-17-gcc-pgo`](2026-07-17-gcc-pgo/README.md): GCC profile-guided
  optimization

Optimization patches are independent and apply directly to the source
baseline identified in their result directory.  Do not stack them while
searching for gains.  For each experiment:

```sh
git apply benchmarks/RESULT/CHANGE.patch
.venv-benchmark/bin/python setup.py build_ext \
  -DSQLITE_ENABLE_COLUMN_METADATA --inplace --force --enable-all-extensions
# Run focused and speedtest measurements.
git apply --reverse benchmarks/RESULT/CHANGE.patch
.venv-benchmark/bin/python setup.py build_ext \
  -DSQLITE_ENABLE_COLUMN_METADATA --inplace --force --enable-all-extensions
```

Only changes improving both a focused benchmark and at least one speedtest
case by at least 1% receive a patch file.  Source and extension binaries
must be restored to the common baseline before evaluating the next change.

## Not worth it: second non-PGO scan

The following independent experiments started from source baseline
`22ddc459`.  They were removed and did not receive patch files:

- A terminal `Cursor.__next__` `C_DONE` fast path is not reached by normal
  iteration and regressed the focused execute loop by 2.7%.
- A shorter uncontended cursor-mutex path improved the focused execute loop
  by 1.4-2.0%, but regressed the 40-iteration `statements` speedtest by about
  1% against bracketing baseline builds.
- Retaining the binding kind improved focused bound execution by 2.1%, but
  regressed the 40-iteration `statements` speedtest by about 1% against
  bracketing baseline builds.
- A description-cache dirty flag regressed focused execution by 0.3%.
- Caching the result column count in each statement regressed focused row
  conversion by 4.5%.
- Reordering the synchronous async-controller check regressed focused row
  conversion by 2.9%.
- Checking text results first in scalar callback result conversion changed
  focused callback cost by less than 0.1%.

## Create the environment

```sh
uv python install
uv venv .venv-benchmark
uv pip sync --python .venv-benchmark/bin/python benchmarks/requirements.txt
```

## Build

The command below is the implementation of the `Makefile`'s `build_ext`
target.  It is used directly because that target's generated documentation
prerequisites import APSW before the extension has been built in a clean
environment.

```sh
.venv-benchmark/bin/python setup.py \
  fetch --version=3.53.3 --all \
  build_ext -DSQLITE_ENABLE_COLUMN_METADATA \
  --inplace --force --enable-all-extensions
```

The fetch step verifies the upstream source checksums.  Optional extensions
that require system development packages can still differ by host.  Record
`apsw.compile_options` with every baseline.

## Run

First verify that all three execution forms produce identical results:

```sh
.venv-benchmark/bin/python -m apsw.speedtest \
  --apsw --correctness --scale 1 --iterations 2 --hide-runs
```

Run the command under investigation unchanged, then take a longer run pinned
to one physical CPU.  Select the CPU using `lscpu -e=CPU,CORE,SOCKET,NODE` and
record it in the baseline.

```sh
.venv-benchmark/bin/python -m apsw.speedtest --apsw
taskset -c 7 .venv-benchmark/bin/python -m apsw.speedtest \
  --apsw --iterations 11
```

The longer pinned run is the comparison baseline.  Preserve the default run
to detect whether changing the harness itself changes behavior.

## Interpretation

The tests measure different paths and must not be combined into one score:

- `bigstmt` stresses execution and preparation of many statements supplied as
  one SQL string.
- `statements` uses bindings and has many statement-cache hits.  It also calls
  the Python `number_name` scalar function during the timed section.
- `statements_nobindings` uses mostly unique SQL text, stressing preparation,
  cache misses, eviction, and finalization.

The harness creates the SQL and binding lists before timing.  Connection
creation and scalar-function registration are also outside timing, while
connection close and a generation-2 garbage collection are inside timing.
Results therefore describe this workload, not an isolated APSW call cost.
