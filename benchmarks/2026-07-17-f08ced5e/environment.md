# Environment

## Source and runtime

- Git commit: `f08ced5e5b264955ea4c48182da4be2e13072113`
- Git describe: `3.53.3.1-2-gf08ced5e`
- Branch: `bench`
- APSW: 3.53.3.1
- SQLite library and headers: 3.53.3
- SQLite source ID:
  `2026-06-26 20:14:12 d4c0e51e4aeb96955b99185ab9cde75c339e2c29c3f3f12428d364a10d782c62`
- Python: uv CPython 3.13.14, standard GIL build, JIT disabled
- Python build compiler: Clang 22.1.3
- Extension compiler: GCC 15.2.1 20260123 (Red Hat 15.2.1-7)
- Build optimization: `-O3`, PGO/BOLT/LTO Python, frame pointers retained
- libc: glibc 2.41
- uv: 0.11.28
- setuptools: 80.9.0

The APSW extension links only to libc and the dynamic loader.  SQLite is the
bundled amalgamation, not the Fedora SQLite library.

## Host

- Environment: Podman container
- Userspace: Fedora 42 x86-64
- Host kernel: Linux 7.1.3-200.fc44.x86_64
- CPU: AMD Ryzen 7 8840U with Radeon 780M Graphics
- Topology: 8 physical cores, 16 threads, one socket and one NUMA node
- Cache: 256 KiB L1d, 256 KiB L1i, 8 MiB L2, 16 MiB L3
- RAM visible to container: 60 GiB
- CPU cgroup quota: none
- Memory cgroup limit: none
- Benchmark CPU: logical CPU 7, physical core 7; sibling logical CPU 15
- Frequency driver: `amd-pstate-epp`, active mode
- Governor: `powersave`
- Energy preference: `balance_performance`
- Frequency range: 419.175 MHz to 5,134.889 MHz
- Frequency boost: enabled
- SMT: enabled
- Workspace filesystem: Btrfs, 4 KiB block size

The benchmark database was `:memory:`, so the workspace filesystem was not on
the measured database path.

## SQLite compile options

```text
ATOMIC_INTRINSICS=1
COMPILER=gcc-15.2.1 20260123 (Red Hat 15.2.1-7)
DEFAULT_AUTOVACUUM
DEFAULT_CACHE_SIZE=-2000
DEFAULT_FILE_FORMAT=4
DEFAULT_JOURNAL_SIZE_LIMIT=-1
DEFAULT_MMAP_SIZE=0
DEFAULT_PAGE_SIZE=4096
DEFAULT_PCACHE_INITSZ=20
DEFAULT_RECURSIVE_TRIGGERS
DEFAULT_SECTOR_SIZE=4096
DEFAULT_SYNCHRONOUS=2
DEFAULT_WAL_AUTOCHECKPOINT=1000
DEFAULT_WAL_SYNCHRONOUS=2
DEFAULT_WORKER_THREADS=0
DIRECT_OVERFLOW_READ
ENABLE_CARRAY
ENABLE_COLUMN_METADATA
ENABLE_DBSTAT_VTAB
ENABLE_FTS3
ENABLE_FTS3_PARENTHESIS
ENABLE_FTS4
ENABLE_FTS5
ENABLE_GEOPOLY
ENABLE_MATH_FUNCTIONS
ENABLE_PERCENTILE
ENABLE_PREUPDATE_HOOK
ENABLE_RTREE
ENABLE_SESSION
ENABLE_SETLK_TIMEOUT
ENABLE_STAT4
HAVE_ISNAN
LIKE_DOESNT_MATCH_BLOBS
MALLOC_SOFT_LIMIT=1024
MAX_ATTACHED=125
MAX_COLUMN=2000
MAX_COMPOUND_SELECT=500
MAX_DEFAULT_PAGE_SIZE=8192
MAX_EXPR_DEPTH=1000
MAX_FUNCTION_ARG=1000
MAX_LENGTH=1000000000
MAX_LIKE_PATTERN_LENGTH=50000
MAX_MMAP_SIZE=0x1000000000000LL
MAX_PAGE_COUNT=0xfffffffe
MAX_PAGE_SIZE=65536
MAX_SQL_LENGTH=1000000000
MAX_TRIGGER_DEPTH=1000
MAX_VARIABLE_NUMBER=32766
MAX_VDBE_OP=250000000
MAX_WORKER_THREADS=8
MUTEX_PTHREADS
OMIT_AUTOINIT
OMIT_DEPRECATED
OMIT_SHARED_CACHE
STRICT_SUBTYPE
SYSTEM_MALLOC
TEMP_STORE=1
THREADSAFE=1
```

ICU was not enabled because ICU development headers were not present.  This
is a host-dependent part of `--enable-all-extensions` and must be checked when
reproducing the build elsewhere.
