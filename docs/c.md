
# Table of Contents

1.  [Scope and method](#orgd85dfa1)
2.  [Executive summary](#org2c62634)
3.  [Findings](#org84dde47)
    1.  [src/apsw.c](#org185f0a8)
        1.  [C001 [MEDIUM] Incorrect `sqlite3_config` variadic signatures](#org3f5c200)
        2.  [C002 [LOW] Legacy global module state is not subinterpreter-safe](#org83ba3c3)
        3.  [C003 [LOW] Large `randomness` and `complete` operations retain the GIL](#org2f5613d)
    2.  [src/argparse.c](#org865394f)
        1.  [C004 [HIGH] Unicode offset parser accepts `len + 1` and feeds OOB reads](#org897a1bb)
        2.  [C005 [MEDIUM] Unicode mapper keyword calls leak keyword tuples](#org87c32d9)
        3.  [C006 [LOW] Embedded-NUL keyword names alias valid keywords](#org363d1f9)
        4.  [C007 [MEDIUM] User-sized VLA is allocated before argument-count validation](#orgc0e867a)
        5.  [C008 [LOW] List-item error reports the list type, not the bad item type](#org648b680)
    3.  [src/async.c](#org742bf8e)
        1.  [C009 [HIGH] Re-entrant BoxedCall clears an outer invocation's live state](#org9c4a916)
        2.  [C010 [MEDIUM] BoxedCall is not visible to cyclic GC](#orge9a0103)
        3.  [C011 [LOW] Zero-argument async dispatch can call `memcpy(NULL, ..., 0)`](#org0c93bf9)
        4.  [C012 [LOW] `PyContext_Exit` failure is ignored](#org02b0ed4)
    4.  [src/backup.c](#org5c86d20)
        1.  [C013 [HIGH] Progress getters race with step and finish](#orgf2b7d63)
        2.  [C014 [CRITICAL] A Python callback can re-entrantly finish an active backup](#org4e6a248)
    5.  [src/blob.c](#org6667218)
        1.  [C015 [LOW] Blob offset arithmetic has reachable signed overflow](#org3474d5d)
        2.  [C016 [MEDIUM] Mutex contention leaks the `read_into` buffer export](#orgcaab23e)
        3.  [C017 [MEDIUM] `Blob.aclose` on a synchronous connection succeeds without closing](#org58e0f21)
        4.  [C018 [LOW] Context exit masks a block exception after manual close](#org331301a)
        5.  [C019 [LOW] `zeroblob.length` truncates on Windows LLP64](#org6cfd93d)
        6.  [C020 [MEDIUM] Potentially blocking Blob I/O holds the GIL](#orgbc6515d)
        7.  [C021 [LOW] An aborted Blob handle can be misreported as EOF or ValueError](#org1c50e4a)
    6.  [src/carray.c](#org4e825fd)
        1.  [C022 [HIGH] Numeric carray accepts unaligned contiguous buffers](#org27e35ec)
        2.  [C023 [HIGH] carray blob lengths above `INT_MAX` enter SQLite through a narrowing cast](#orge267e2d)
        3.  [C024 [HIGH] Writable carray buffers race SQLite scans executed without the GIL](#org53b66b6)
        4.  [C025 [MEDIUM] carray allocation failures return without `MemoryError`](#orgcec6dbe)
        5.  [C026 [MEDIUM] CArrayBind owns references but is not GC tracked](#orgc767958)
        6.  [C027 [LOW] carray error formatting has variadic type mismatches](#orgaccfa95)
        7.  [C028 [LOW] carray bind destructor assumes an attached Python thread state](#org656c2bb)
        8.  [C029 [LOW] Narrow carray slices retain the whole source tuple](#org6626bbe)
    7.  [src/connection.c](#org3aeb375)
        1.  [C030 [HIGH] Failed window-function registration destroys callback state twice](#orgc5fb9ba)
        2.  [C031 [HIGH] Virtual-table registration has double-free and pre-transfer leaks](#org78f551d)
        3.  [C032 [MEDIUM] Dependent registration leaks a Blob weakref or failed Cursor](#org9d64112)
        4.  [C033 [MEDIUM] Multiple progress handlers ignore their own `nsteps`](#org27eb9f9)
        5.  [C034 [MEDIUM] PreUpdate rowids truncate on LLP64](#org23b9c62)
        6.  [C035 [LOW] Empty BLOB comparison can call `memcmp(NULL, NULL, 0)`](#orgc21da80)
        7.  [C036 [LOW] Large database serialization retains the GIL](#org9703a76)
    8.  [src/constants.c](#orgdd2c9bb)
        1.  [C037 [LOW] Generated reverse mapping hides `SQLITE_DBCONFIG_FP_DIGITS`](#org82db247)
    9.  [src/cursor.c](#orgf2c03d0)
        1.  [C038 [HIGH] Cursor mutex wait races close and can dereference NULL or retain a mutex](#org0175593)
        2.  [C039 [HIGH] Cursor user callbacks run before the reentrancy guard](#org5942252)
        3.  [C040 [HIGH] `resetcursor` can return success with an iterator exception pending](#org1cc9807)
        4.  [C041 [HIGH] Python 3.10-3.11 async exception buffering creates tuples with NULL items](#org37244c9)
        5.  [C042 [MEDIUM] Mapping detection loses the tri-state result of `PyObject_IsInstance`](#orgf51f3b5)
        6.  [C043 [MEDIUM] GC-tracked Cursor has no `tp_clear`](#orgbef770c)
    10. [src/exceptions.c](#org68b0100)
        1.  [C044 [HIGH] Exception-class globals are dangling borrowed pointers after attribute replacement](#org3b4d62d)
        2.  [C045 [MEDIUM] Python exception text is omitted when SQLite's errmsg starts NULL](#orge01026b)
        3.  [C046 [LOW] `extendedresult` can contradict the exception's primary SQLite class](#orgf46d240)
    11. [src/faultinject.c](#orgcd8d9f0)
        1.  [C047 [MEDIUM] Fault-injection hooks use unchecked Python allocations](#org0fc7823)
        2.  [C048 [MEDIUM] Python 3.12+ hook shape silently disables named APSW faults](#org320729d)
        3.  [C049 [LOW] Fault hook recursion-limit arithmetic can overflow](#orga49f5c0)
        4.  [C050 [LOW] Temporary recursion-limit changes overwrite user/concurrent changes](#org2126cb8)
        5.  [C051 [LOW] `sys.apsw_should_fault = None` aborts the process](#orga83a04e)
        6.  [C052 [LOW] Fault-control result does not validate its exception class](#org7ca0bf4)
    12. [src/fileio<sub>win32.c</sub>](#orgc832561)
        1.  [C053 [LOW] Loadable-extension SQLite API thunk can be rewritten concurrently](#orgb50665c)
    13. [src/fts.c](#org020c7e6)
        1.  [C054 [HIGH] `xSetAuxdata` decrefs a borrowed value on SQLite failure](#org4970e17)
        2.  [C055 [HIGH] Ignored colocated tokens leak a `PyGILState_Ensure` level](#org7ac86f8)
        3.  [C056 [HIGH] Replacing SQL function `fts5` can cause a NULL API dereference](#orgbbd3254)
        4.  [C057 [LOW] Several FTS tuple builders grow quadratically](#orga8d44a2)
        5.  [C058 [LOW] Tokenizer locale length narrows to `int` without a bound](#orgbb5921d)
        6.  [C059 [LOW] FTS5 v4 callback fields are used without checking `iVersion`](#orgdf0bebc)
    14. [src/jsonb.c](#orga9d438c)
        1.  [C060 [HIGH] Crafted eight-byte JSONB length wraps bounds and drives an OOB scan](#org70a9a30)
        2.  [C061 [MEDIUM] Numeric subclasses are serialized through overridable `__str__`](#orgba64a85)
        3.  [C062 [MEDIUM] libc `realloc` failure returns without `MemoryError`](#orge8d74d5)
        4.  [C063 [LOW] Object decoder leaks its partial builder on insertion failure](#org4c35f2b)
        5.  [C064 [LOW] Hexadecimal `parse_int` callback receives base 0, not documented 16](#org78bc4e1)
        6.  [C065 [LOW] Raw JSONB text is validated and UTF-8 decoded in two full passes](#org93c8d1b)
    15. [src/pyutil.c](#orga6c5389)
        1.  [C066 [LOW] Note-formatting failure loses the pending exception](#orgc6e33cb)
        2.  [C067 [MEDIUM] Exception chaining depends on private CPython symbols](#org86f335f)
        3.  [C068 [LOW] `PyThreadState_GetDict` NULL result is passed to a dict API](#orgfc5ce41)
    16. [src/session.c](#org66c67c5)
        1.  [C069 [HIGH] ChangesetIterator keeps a dangling borrowed TableChange pointer](#org5d95db8)
        2.  [C070 [HIGH] Several Session operations bypass the connection mutex and async affinity](#orgab35dd1)
        3.  [C071 [HIGH] Schema-backed ChangesetBuilder uses a raw database without its mutex](#orga0f3f17)
        4.  [C072 [MEDIUM] Session and ChangesetBuilder traversal is disabled by missing GC flags](#org4e761d4)
        5.  [C073 [HIGH] Builder bookkeeping failure leaves SQLite holding an untracked database pointer](#org3ae5b86)
        6.  [C074 [MEDIUM] Public `session_config` permits SQLite-defined undefined states](#orgfaefcfd)
        7.  [C075 [MEDIUM] Oversized builder BLOB path leaks its buffer export](#org0894a74)
        8.  [C076 [LOW] Large callback-free changeset operations retain the GIL](#orga9d2b34)
    17. [src/statementcache.c](#org02e52b7)
        1.  [C077 [HIGH] Empty SQL with explain mode passes NULL to `sqlite3_stmt_explain`](#org9487f3f)
        2.  [C078 [LOW] Recycle bin uses only three of four slots](#org1282b86)
        3.  [C079 [LOW] Cache statistics wrap after `UINT_MAX`](#org75cb2f0)
        4.  [C080 [HIGH] Valid hash `-1` collides with the empty-slot sentinel on 32-bit](#org31072f6)
        5.  [C081 [MEDIUM] `sqlite3_stmt_explain` can reprepare while holding the GIL](#org5c94d07)
    18. [src/stringconstants.c](#orge445c23)
    19. [src/testextension.c](#orgc3a9ea0)
        1.  [C082 [LOW] Test extension reports successful loading when function registration fails](#orgf3492ac)
        2.  [C083 [LOW] Test extension's standard SQLite API thunk has the same qualified race](#org53ccb28)
    20. [src/traceback.c](#org50bcb7d)
    21. [src/unicode.c](#orgd0e2658)
        1.  [C084 [HIGH] Mapper reinitialization leaks resources and can leave stale/dangling state](#orgabba240)
        2.  [C085 [MEDIUM] OffsetMapper has reachable signed offset overflow](#orgeb83033)
        3.  [C086 [LOW] Mutable UTF-8 buffers invalidate mapper validation](#org447530c)
        4.  [C087 [LOW] Two mapper types can form cycles invisible to GC](#org6dad653)
        5.  [C088 [LOW] Extreme `grapheme_find` bounds underflow signed arithmetic](#orgf282ebb)
        6.  [C089 [LOW] Unicode mapper allocation failures omit `MemoryError`](#orgb03399e)
        7.  [C090 [LOW] Negative grapheme slicing boxes every boundary](#org586b040)
    22. [src/util.c](#org7804d4b)
        1.  [C091 [HIGH] Failed `PyMem_Resize` destroys the pending-call queue pointer](#org7fd6e9f)
        2.  [C092 [MEDIUM] Virtual-table IN conversion discards meaningful SQLite statuses](#orgd296f74)
        3.  [C093 [MEDIUM] Pending destructor queue has quadratic growth and permanent high-water scans](#orgbffef7b)
    23. [src/vfs.c](#orgd75af47)
        1.  [C094 [HIGH] Derived VFS can outlive an unretained APSW base](#org110eee3)
        2.  [C095 [HIGH] `xSetSystemCall(NULL, ...)` passes NULL to `PyUnicode_FromString`](#org59941a8)
        3.  [C096 [MEDIUM] System-call bridge changes valid False/None semantics](#org6127ffa)
        4.  [C097 [MEDIUM] Inherited short reads discard real trailing NUL bytes](#orgbbd0e29)
        5.  [C098 [MEDIUM] User-controlled `mxPathname` causes signed overflow and invalid sizes](#orgef02e48)
        6.  [C099 [LOW] Failed VFS `xOpen` does not explicitly clear `pMethods`](#orgfb2e9e5)
        7.  [C100 [LOW] Every `xSync` callback drops an owned flags-integer reference](#orgecb9db4)
        8.  [C101 [MEDIUM] `xGetLastError` truncates every nonempty message by one byte](#org0e19e86)
        9.  [C102 [MEDIUM] Optional VFS attribute checks suppress real lookup exceptions](#org2eeb05b)
    24. [src/vtable.c](#org6945386)
        1.  [C103 [MEDIUM] BestIndex argument indexes overflow or continue with an exception](#org32412f8)
        2.  [C104 [MEDIUM] BestIndex leaks `idxStr` and an exceptional `idxnum`](#orgb88ae1d)
        3.  [C105 [MEDIUM] Optional virtual-table method checks suppress descriptor exceptions](#orgd222f6b)
        4.  [C106 [MEDIUM] Several vtable allocation/argument failures violate exception invariants](#orgea9827e)
        5.  [C107 [LOW] Embedded NUL in BestIndex `idxStr` is silently truncated](#orgc8623eb)
4.  [Consolidated CPython C-API opportunities](#orga910b17)
5.  [Rejected or consolidated candidates](#orga980bac)
6.  [Per-file coverage](#org616baad)



<a id="orgd85dfa1"></a>

# Scope and method

This is a read-only source audit.  One primary audit and one independent
validation pass were performed for every tracked C file except
`src/_unicodedb.c`, which was excluded at the request of the user because it
is generated.  After rebasing the report onto commit
`69e0f6be7980c1f641f402312a056b9de4d06ba5`, every finding was rechecked and
all file/line references were remapped to that tree.  Related declarations and
call sites were inspected only to validate findings.  No source remediation
was performed.

The checkout did not contain a built APSW extension, so findings were
validated by source/control-flow analysis and local API contracts rather than
by executing the extension.  Items whose manifestation is platform-, fault-,
or schedule-dependent are explicitly qualified.

Severity means:

-   **Critical**: a credible direct path to native use-after-free/corruption during
    ordinary supported operation.
-   **High**: native memory safety, process crash, data race, or major resource
    lifetime failure.
-   **Medium**: definite correctness, leak, portability, concurrency, or material
    performance problem.
-   **Low**: narrow error path, diagnostics, standards-level portability issue, or
    lower-impact optimization.


<a id="org2c62634"></a>

# Executive summary

The highest-priority areas are:

1.  Re-entrant callbacks invalidating active native objects in Backup and
    BoxedCall.
2.  SQLite ownership-transfer mistakes after failed function/module
    registration.
3.  Unsynchronized native-handle access in Cursor, Session, Backup, carray, and
    ChangesetBuilder.
4.  Crafted JSONB lengths causing an out-of-bounds read.
5.  Iterator/TableChange and derived/base VFS lifetime errors.
6.  Missing bounds, alignment, and variadic-signature checks that cause C
    undefined behavior.
7.  Error paths returning without an exception, leaking buffer exports, or
    constructing invalid Python containers.

The module currently relies on the GIL and legacy process-global state.  It
must not claim `Py_MOD_GIL_NOT_USED`.  Subinterpreter support should either be
explicitly rejected or implemented through PEP 489 multi-phase initialization
and per-module state.


<a id="org84dde47"></a>

# Findings


<a id="org185f0a8"></a>

## src/apsw.c


<a id="org3f5c200"></a>

### C001 [MEDIUM] Incorrect `sqlite3_config` variadic signatures

`SQLITE_CONFIG_LOOKASIDE` requires two trailing `int` arguments, but APSW
parses and passes one.  Disabling `SQLITE_CONFIG_LOG` requires a callback and a
context, but APSW passes only one `NULL`.  Both make SQLite consume a missing
variadic argument.  `SQLITE_CONFIG_PMASZ` is read by SQLite as `unsigned int`;
negative values passed as `int` are outside the compatible variadic exception,
and the upper unsigned range is unavailable.

-   Direction: give LOOKASIDE its own two-value parser, pass both typed NULLs for
    LOG, and parse/range-check PMASZ as unsigned.
-   Test: after shutdown, configure lookaside with size/count; disable LOG under
    MSan/Valgrind; verify negative/out-of-range PMASZ values are rejected.


<a id="org83ba3c3"></a>

### C002 [LOW] Legacy global module state is not subinterpreter-safe

The module uses single-phase initialization with `m_size == -1` while keeping
interpreter-owned objects in process-global variables.  A second interpreter
cannot receive isolated state; teardown or callback attachment can affect the
wrong interpreter.  This is an architectural limitation unless APSW claims
subinterpreter support.

-   Direction: reject non-main/isolated interpreters in the short term.  Longer
    term use PEP 489, per-module state, and module-associated heap types.
-   Test: import/use/destroy APSW in multiple subinterpreters under debug CPython;
    until supported, assert deterministic rejection.


<a id="org2f5613d"></a>

### C003 [LOW] Large `randomness` and `complete` operations retain the GIL

`sqlite3_randomness` can fill an `int`-sized buffer and `sqlite3_complete` scans
arbitrarily large SQL while holding the GIL.  Their Python buffers remain
owned for the call, so a narrow allow-threads region appears feasible.

-   Direction: benchmark, then release the GIL only around the SQLite call.
-   Test: run a heartbeat Python thread during large requests and verify progress
    and unchanged results.


<a id="org865394f"></a>

## src/argparse.c


<a id="org897a1bb"></a>

### C004 [HIGH] Unicode offset parser accepts `len + 1` and feeds OOB reads

A valid position after a string is `len`, but the parser admits `len + 1`.
Unicode break sinks in `src/unicode.c` then use `PyUnicode_READ` past even the
guaranteed terminator.  The category-range sink can also inspect the terminator
at `len` as though it were a character when given the invalid `len + 1`
endpoint.

-   Direction: make the inclusive upper bound exactly `PyUnicode_GET_LENGTH`.
-   Test: every offset-taking Unicode API should accept `len` and reject
    `len + 1` under ASan/debug CPython.


<a id="org87c32d9"></a>

### C005 [MEDIUM] Unicode mapper keyword calls leak keyword tuples

`ARG_CONVERT_VARARGS_TO_FASTCALL` creates an owned `fast_kwnames` tuple.  The
two Unicode mapper initializers pass an empty epilog cleanup, leaking the tuple
and keyword-name references on success and failure.

-   Direction: make cleanup intrinsic to the adapter or add
    `Py_XDECREF(fast_kwnames)` at both sites.
-   Test: repeat keyword construction with dynamically created keys and verify
    stable references/tracemalloc totals.


<a id="org363d1f9"></a>

### C006 [LOW] Embedded-NUL keyword names alias valid keywords

Keyword matching uses `strcmp` on Python UTF-8 data.  A key such as
`"statement\0suffix"` therefore matches `"statement"` instead of being
rejected.

-   Direction: on Python 3.10-3.12 use `PyUnicode_AsUTF8AndSize` plus a
    length-aware comparison; on 3.13+ use `PyUnicode_EqualToUTF8`.
-   Test: pass NUL-suffixed keywords and assert `TypeError`.


<a id="orgc0e867a"></a>

### C007 [MEDIUM] User-sized VLA is allocated before argument-count validation

The varargs-to-fastcall adapter allocates stack space from the complete
positional and keyword counts before rejecting excess arguments.  A large
expanded call can exhaust the native stack instead of raising `TypeError`.
This common root affects twelve constructor/initializer call sites.

-   Direction: reject against the known maximum before allocation, or use a
    checked small-stack/heap fallback with `Py_ssize_t` indexes.
-   Test: in a low-stack subprocess, pass oversized `*args` and `**kwargs` and
    require a normal Python exception rather than a signal.


<a id="org648b680"></a>

### C008 [LOW] List-item error reports the list type, not the bad item type

The failing `ARG_list_str` diagnostic calls `Py_TypeName` on the container.
Users see “list” rather than the offending item's type.

-   Direction: retain the item in a local and report its type.
-   Test: pass `[object()]` to the reachable tokenizer argument and assert the
    message names `object` and its index.


<a id="org742bf8e"></a>

## src/async.c


<a id="org9c4a916"></a>

### C009 [HIGH] Re-entrant BoxedCall clears an outer invocation's live state

`BoxedCall` remains in a callable state while executing arbitrary code.  A
recursive call can fail entering the already-entered Context and still run
`BoxedCall_clear`, freeing the outer invocation's context and arguments.
Finalizers can also re-enter while cleanup decrefs objects before marking the
box dormant.

-   Direction: transition to a running/clearing state before any callback or
    decref; detach references before executing finalizers.
-   Test: re-enter through `apsw.aio._tls.current_call.call` and from an argument
    finalizer in an ASan/debug subprocess.


<a id="orge9a0103"></a>

### C010 [MEDIUM] BoxedCall is not visible to cyclic GC

The object owns arbitrary Context, call arguments, connections, and callbacks
but has no GC flag, traverse, or clear slots.  Abandoned call graphs can form
uncollectable cycles retaining connections and worker resources.

-   Direction: make the variable-sized type GC-aware and coordinate `tp_clear`
    with the running-state fix.
-   Test: form a coroutine/argument/BoxedCall cycle, drop external references,
    collect, and verify weakrefs die.


<a id="org0c93bf9"></a>

### C011 [LOW] Zero-argument async dispatch can call `memcpy(NULL, ..., 0)`

Vectorcall permits a NULL argument vector for zero arguments, but the code
unconditionally calls `memcpy`.  ISO C does not make NULL pointer arguments
valid merely because the size is zero.

-   Direction: call `memcpy` only when `total_args > 0`.
-   Test: use `PyObject_CallNoArgs` paths under UBSan.


<a id="org02b0ed4"></a>

### C012 [LOW] `PyContext_Exit` failure is ignored

A failed Context exit can leave an exception pending while the function returns
a non-NULL result.  Normal invariants make this rare and C009 supplies the main
concrete corruption route.

-   Direction: check the return, discard any successful call result, and preserve
    deliberate exception chaining.
-   Test: fault-inject Context exit after successful and failed calls.


<a id="org5c86d20"></a>

## src/backup.c


<a id="orgf2b7d63"></a>

### C013 [HIGH] Progress getters race with step and finish

`step` and `finish` run with the GIL released.  `remaining` and `page_count`
call SQLite without either database mutex.  Concurrent step violates SQLite's
getter contract; concurrent finish can free the handle while a getter uses it.

-   Direction: cache progress while both mutexes are held, or serialize getters
    with the same operation/close state.
-   Test: poll during long step/finish under TSan/ASan.


<a id="org4e6a248"></a>

### C014 [CRITICAL] A Python callback can re-entrantly finish an active backup

During `sqlite3_backup_step`, a Python VFS or busy callback can reacquire the
GIL and call `finish`.  SQLite connection mutexes are recursive on the same
thread, so nested finish can free the active backup.  The outer SQLite call
then resumes on freed state and later dereferences cleared source/destination
objects.

-   Direction: add a per-backup running/closing guard before entering SQLite and
    reject nested step/finish/close/exit.
-   Test: have a VFS callback call `finish` during `step` and require a clean
    reentrancy exception under ASan.


<a id="org6667218"></a>

## src/blob.c


<a id="org3474d5d"></a>

### C015 [LOW] Blob offset arithmetic has reachable signed overflow

`read_into` subtracts an unchecked extreme offset and later adds offset/length.
`seek` adds signed `int` values before range checking.  Python can supply values
that overflow these expressions.

-   Direction: use checked `sqlite3_int64~/~long long` arithmetic and validate
    before deriving lengths or narrowing.
-   Test: exercise LLONG<sub>MIN</sub> and INT<sub>MAX</sub>-relative seeks under UBSan.


<a id="orgcaab23e"></a>

### C016 [MEDIUM] Mutex contention leaks the `read_into` buffer export

`DBMUTEX_ENSURE` returns directly.  The rebased Blob rewrite now acquires the
mutex before allocating the `read` result and before exporting the `write`
buffer, fixing those two manifestations.  `read_into`, however, still exports
its writable buffer before `DBMUTEX_ENSURE`; contention therefore bypasses
`PyBuffer_Release` and can leave a bytearray permanently exported.

-   Direction: acquire the mutex before exporting the `read_into` buffer; if a
    later cleanup route can run without ownership, track acquisition before
    calling `sqlite3_mutex_leave`.
-   Test: force contention in `read_into`, verify a subsequent bytearray resize
    succeeds, and check repeated failures for retained exports.


<a id="org58e0f21"></a>

### C017 [MEDIUM] `Blob.aclose` on a synchronous connection succeeds without closing

On a synchronous connection the async-dispatch macro does nothing; the method
returns a successful coroutine without calling close and ignores `force`.
Although the method is intended for async objects, the runtime API should not
silently report success.

-   Direction: reject async use in a synchronous context or deliberately perform
    and document synchronous close.
-   Test: await `aclose` on a sync Blob and require either closure or the
    documented context error.


<a id="org331301a"></a>

### C018 [LOW] Context exit masks a block exception after manual close

`__exit__` and `__aexit__` check for closed before invoking idempotent close.
Manual close in the body therefore raises `ValueError` at exit and can replace
the body's original exception.

-   Direction: make exits use idempotent close and preserve the active exception.
-   Test: manually close inside sync/async contexts, with and without a marker
    exception.


<a id="org6cfd93d"></a>

### C019 [LOW] `zeroblob.length` truncates on Windows LLP64

The stored size is `long long` but is returned with `PyLong_FromLong`.  A
64-bit Windows `long` is only 32 bits.

-   Direction: use `PyLong_FromLongLong`.
-   Test: on Windows assert `zeroblob(1 << 40).length() == 1 << 40`.


<a id="orgbc6515d"></a>

### C020 [MEDIUM] Potentially blocking Blob I/O holds the GIL

Large reads/writes and pager/VFS work execute while holding the GIL, unlike
other potentially long SQLite calls.  This can stall unrelated Python threads
and async event-loop work.

-   Direction: after fixing operation lifetime/locking, release the GIL only
    around raw SQLite calls and cover Python VFS callback reacquisition.
-   Test: block a VFS read/write while a heartbeat thread must progress.


<a id="org1c50e4a"></a>

### C021 [LOW] An aborted Blob handle can be misreported as EOF or ValueError

After a failed reopen, SQLite can leave a non-NULL aborted handle for which
`sqlite3_blob_bytes` returns zero.  The rebased code now raises
`SQLITE_ABORT` when the current offset is greater than that reported length,
which mitigates some expired-handle cases.  At offset zero, however, preflight
can still return EOF or a local bounds error instead of the handle's
`SQLITE_ABORT`; `seek` and `length` also remain local users of the reported
length.  This does not apply to every ordinary row-update expiration.

-   Direction: track aborted state or ensure operations reach SQLite and preserve
    its error consistently.
-   Test: fail reopen, then exercise read/read<sub>into</sub>/write/seek.


<a id="org4e825fd"></a>

## src/carray.c


<a id="org27e35ec"></a>

### C022 [HIGH] Numeric carray accepts unaligned contiguous buffers

`PyBUF_ANY_CONTIGUOUS` does not promise alignment.  SQLite carray casts the
pointer to `int *`, `sqlite3_int64 *`, or `double *` and dereferences it.
Odd-offset slices can fault on strict-alignment platforms.

-   Direction: enforce `_Alignof` for the selected type or copy into aligned
    owned storage.
-   Test: query numeric memoryviews beginning at offset one under UBSan and on a
    strict-alignment target.


<a id="orge267e2d"></a>

### C023 [HIGH] carray blob lengths above `INT_MAX` enter SQLite through a narrowing cast

APSW permits a `Py_buffer.len` wider than `int`, while SQLite's carray path
casts the iovec length before `sqlite3_result_blob`.  Large lengths can become
negative and violate SQLite's API, potentially causing incorrect or
out-of-bounds reads when API armor is disabled.

-   Direction: reject lengths above the supported range or update the bundled
    carray path to `sqlite3_result_blob64`.
-   Test: use a sparse mapping larger than `INT_MAX` and require a clean
    `OverflowError~/~TooBigError`.


<a id="org53b66b6"></a>

### C024 [HIGH] Writable carray buffers race SQLite scans executed without the GIL

A buffer export pins lifetime and shape, not bytes.  Another Python thread can
mutate a bytearray, array, writable memoryview, or NumPy array while
`sqlite3_step` reads it with the GIL released, producing torn values and native
data races.

-   Direction: copy writable inputs, reject them, or define/enforce a locking
    contract; preserve zero-copy for immutable exporters.
-   Test: mutate numeric and blob buffers concurrently under TSan.


<a id="orgcec6dbe"></a>

### C025 [MEDIUM] carray allocation failures return without `MemoryError`

Three raw Python allocator failures jump to cleanup without setting an
exception, causing `tp_init` to return -1 with no exception and typically a
secondary `SystemError`.

-   Direction: call `PyErr_NoMemory` and add checked multiplication.
-   Test: fault each allocation independently and require `MemoryError` with no
    leak.


<a id="orgc767958"></a>

### C026 [MEDIUM] CArrayBind owns references but is not GC tracked

The object owns a tuple and exporter references through `Py_buffer.obj` but has
no GC protocol.  Custom exporters can hold a back-reference and create a
permanent cycle.

-   Direction: add GC traversal/clear for every active exporter and tuple.
-   Test: form numeric and blob exporter cycles and verify collection/release.


<a id="orgaccfa95"></a>

### C027 [LOW] carray error formatting has variadic type mismatches

Several `PyErr_Format` calls pass `int64_t`, `Py_ssize_t`, or `size_t` to
`%lld`, and one passes `Py_ssize_t` to `%d`.  The mismatch is variadic UB even
where common ABIs happen to print correctly.

-   Direction: use `%zd~/~%zu` or explicit matching casts.
-   Test: compile with format diagnostics and exercise every validation branch on
    LP64 and Windows.


<a id="org656c2bb"></a>

### C028 [LOW] carray bind destructor assumes an attached Python thread state

The SQLite destructor calls bare `Py_DECREF`, unlike APSW's equivalent object
binding destructor which uses `PyGILState_Ensure`.  Current audited finalize and
clear paths appear attached to Python, so a detached-thread invocation was not
confirmed.

-   Direction: defensively acquire/release Python state or formally constrain all
    destructor call sites.
-   Test: instrument binding failure, rebind, clear, cache eviction, schema
    reprepare, and close paths.


<a id="org6626bbe"></a>

### C029 [LOW] Narrow carray slices retain the whole source tuple

Selecting a tiny slice keeps every excluded tuple item alive.  Blob views
already retain selected exporters; text only needs selected strings.

-   Direction: retain selected objects only.
-   Test: weak-reference exporters outside the slice and verify prompt
    collection.


<a id="org3aeb375"></a>

## src/connection.c


<a id="orgc5fb9ba"></a>

### C030 [HIGH] Failed window-function registration destroys callback state twice

SQLite invokes the supplied destructor when
`sqlite3_create_window_function` fails.  APSW then manually calls
`apsw_free_func` on the same `FunctionCBInfo`, causing a double decref/UAF.
Mutex-acquisition failure after allocation can also leak it.

-   Direction: finish all pre-call cleanup first; after passing data to SQLite,
    clear the local pointer regardless of SQLite's result.
-   Test: force registration failure while a function is active and inject mutex
    contention under ASan/ref-debug Python.


<a id="org78f551d"></a>

### C031 [HIGH] Virtual-table registration has double-free and pre-transfer leaks

`sqlite3_create_module_v2` invokes `apswvtabFree` on failure, but APSW's error
path invokes it again.  The datasource reference also leaks if allocation,
module-definition setup, or mutex acquisition fails before a usable owner is
recorded.

-   Direction: record owned references immediately, centralize pre-transfer
    cleanup, and set the local pointer NULL at the SQLite ownership boundary.
-   Test: fault every setup stage and registration itself; assert one datasource
    finalization and no invalid frees.


<a id="org9d64112"></a>

### C032 [MEDIUM] Dependent registration leaks a Blob weakref or failed Cursor

A successful Blob append returns without dropping the local weakref reference.
If Cursor's dependent-list append fails, the owned cursor pointer is overwritten
with NULL without decref, retaining the cursor and normally its connection.

-   Direction: decref the weakref after successful append and clear the cursor on
    append failure.
-   Test: repeat Blob opens for stable allocations; fault Cursor append and verify
    finalizers/weakrefs.


<a id="org27eb9f9"></a>

### C033 [MEDIUM] Multiple progress handlers ignore their own `nsteps`

SQLite is configured with the minimum interval, and every active Python handler
is called at every tick.  A handler requesting one million steps can therefore
run every single step when another handler requests one.

-   Direction: maintain per-handler accumulators and use a base interval/GCD with
    documented approximation semantics.
-   Test: register widely different intervals and compare callback counts.


<a id="org23b9c62"></a>

### C034 [MEDIUM] PreUpdate rowids truncate on LLP64

`sqlite3_int64` rowids are exposed with `PyLong_FromLong`.  Values outside 32
bits are lost on 64-bit Windows.

-   Direction: use a 64-bit Python integer constructor.
-   Test: update rowids around `1 << 40` on Windows.


<a id="orgc21da80"></a>

### C035 [LOW] Empty BLOB comparison can call `memcmp(NULL, NULL, 0)`

SQLite may represent a zero-length BLOB with a NULL data pointer.  The standard
library call is not portably valid with NULL pointers even at zero length.

-   Direction: handle zero bytes as equal before requesting pointers/calling
    `memcmp`.
-   Test: compare independent empty BLOB values under UBSan.


<a id="org9703a76"></a>

### C036 [LOW] Large database serialization retains the GIL

`sqlite3_serialize` may allocate and copy a large database while blocking all
Python threads.  Releasing the GIL requires auditing Python VFS exception paths.

-   Direction: benchmark and use a narrow allow-threads region if callback/error
    handling is safe.
-   Test: serialize a large database while a heartbeat thread progresses.


<a id="orgdd2c9bb"></a>

## src/constants.c


<a id="org82db247"></a>

### C037 [LOW] Generated reverse mapping hides `SQLITE_DBCONFIG_FP_DIGITS`

In pinned SQLite 3.53.4, `SQLITE_DBCONFIG_FP_DIGITS` and the sentinel
`SQLITE_DBCONFIG_MAX` are both 1023.  The later MAX insertion overwrites the
integer-to-name entry, so a real operation is reported as the sentinel.

-   Direction: fix `tools/genconstants.py` to give operational constants
    precedence over `*_MAX` aliases, then regenerate.
-   Test: assert both forward entries exist and reverse 1023 resolves to
    `SQLITE_DBCONFIG_FP_DIGITS`.


<a id="orgf2c03d0"></a>

## src/cursor.c


<a id="org0175593"></a>

### C038 [HIGH] Cursor mutex wait races close and can dereference NULL or retain a mutex

`cursor_mutex_get` repeatedly reads `self->connection` with the GIL released.
Another thread can close the cursor and clear that field, causing a NULL/stale
pointer dereference.  Another interleaving acquires the old mutex but skips
release after observing the cleared connection, permanently retaining it.

-   Direction: hold a strong local Connection and cached mutex across the
    allow-threads region; always release the cached mutex and revalidate state.
-   Test: coordinate holder/waiter/closer threads under ASan/TSan.


<a id="org5942252"></a>

### C039 [HIGH] Cursor user callbacks run before the reentrancy guard

Recursive use of the same cursor can pass the recursive SQLite mutex and
reset/replace the outer operation's statement, iterator, and bindings.  The
window is clearest in `executemany`.  In `execute`, the binding converter and
execution tracer are guarded, but binding normalization and statement
preparation still precede `in_query`.

-   Direction: establish the busy/reentrancy state before any callback and clear
    it on one cleanup path.
-   Test: recurse from a tracer, binding converter, and iterator.


<a id="org1cc9807"></a>

### C040 [HIGH] `resetcursor` can return success with an iterator exception pending

If probing the remaining `executemany` iterator raises, `res` can remain
`SQLITE_OK`.  Debug builds hit an assertion; release callers continue while a
Python exception is pending, leading to secondary errors or `SystemError`.

-   Direction: set a failure result while preserving the iterator exception.
-   Test: use a generator that yields once and then raises, then reset/execute.


<a id="org37244c9"></a>

### C041 [HIGH] Python 3.10-3.11 async exception buffering creates tuples with NULL items

`PyErr_Fetch` may return NULL value or traceback pointers, but they are inserted
with `PyTuple_SET_ITEM`.  This creates an invalid tuple consumed later by C and
Python, replacing the original exception or risking a crash.

-   Direction: normalize the exception or represent missing fields with safe
    sentinels and restore them deliberately.
-   Test: on 3.10/3.11 prefetch rows followed by a C-originated exception without
    traceback.


<a id="orgf51f3b5"></a>

### C042 [MEDIUM] Mapping detection loses the tri-state result of `PyObject_IsInstance`

The helper collapses `-1` and false to zero.  Several callers fail to check the
pending exception and continue treating the object as a sequence, causing
secondary errors/assertions.

-   Direction: return and check an explicit tri-state at every caller.
-   Test: make `__class__` lookup raise during Mapping instance checks.


<a id="orgbef770c"></a>

### C043 [MEDIUM] GC-tracked Cursor has no `tp_clear`

Cursor traverses callbacks, bindings, iterators, prefetched rows, and its
Connection but supplies no clear operation.  Cycles not broken by another
participant can retain statements, databases, and callbacks indefinitely.

-   Direction: add a clear path that breaks Python cycles while preserving enough
    connection state for safe native finalization.
-   Test: form tracer/iterator/prefetched-row cycles and verify collection.


<a id="org68b0100"></a>

## src/exceptions.c


<a id="org3b4d62d"></a>

### C044 [HIGH] Exception-class globals are dangling borrowed pointers after attribute replacement

Most generated exception classes are added with stealing
`PyModule_AddObject`, leaving process-global pointers that do not own a
reference.  Users can replace/delete module attributes, destroying the class
while C later passes the stale pointer to exception APIs.  `APSWException` is
not affected in the same way because it uses `PyModule_AddObjectRef`.

-   Direction: store owned references in per-module state and expose attributes
    with `PyModule_AddObjectRef`; protect lifecycle across surviving instances.
-   Test: replace/delete `apsw.SQLError` and an APSW-specific class, collect, then
    trigger each exception under ASan.


<a id="orge01026b"></a>

### C045 [MEDIUM] Python exception text is omitted when SQLite's errmsg starts NULL

The helper allocates a message only under `if (*errmsg && str)`.  Normal callers
initialize the output to NULL, so the Python exception text is never installed.
UTF-8 conversion is also unchecked.

-   Direction: allocate whenever `str` exists; replace/free an old message only
    after successful conversion/allocation.
-   Test: call with NULL and existing messages; fault string, UTF-8, and SQLite
    allocation paths.


<a id="orgf46d240"></a>

### C046 [LOW] `extendedresult` can contradict the exception's primary SQLite class

Any positive integer in a mutable `extendedresult` replaces the class-derived
code, including values with a different low-byte primary result.  This may be
intentional flexibility; tests already expect explicit extended results.

-   Direction: document arbitrary override, or require the primary byte to match
    the exception class.
-   Test: cover matching, mismatched, zero, negative, and overflowing values.


<a id="orgcd8d9f0"></a>

## src/faultinject.c

All findings in this section affect only `APSW_FAULT_INJECT` builds, not normal
APSW builds.


<a id="org0fc7823"></a>

### C047 [MEDIUM] Fault-injection hooks use unchecked Python allocations

Unchecked objects are installed into tuples or passed to vectorcall.  On real
OOM this can create NULL tuple slots and later unconditionally decref NULL.

-   Direction: build arguments in checked temporaries with partial cleanup.
-   Test: fail every allocation in an FI subprocess.


<a id="org320729d"></a>

### C048 [MEDIUM] Python 3.12+ hook shape silently disables named APSW faults

3.10-3.11 passes `(type, value, traceback)`, but 3.12+ passes `(exception,)`.
`tools/fi.py` compares against `(None, None, None)`, so the no-exception case
never matches and named fault sites are skipped.

-   Direction: keep a stable three-item hook ABI or version all consumers.
-   Test: assert identical shapes and actual named-fault selection on every
    supported Python minor.


<a id="orga49f5c0"></a>

### C049 [LOW] Fault hook recursion-limit arithmetic can overflow

`recursion_limit + 50` overflows signed `int` when the user selected a near
`INT_MAX` limit.

-   Direction: remove the global limit mutation or use checked/clamped arithmetic.
-   Test: set `INT_MAX` in a subprocess and run under UBSan.


<a id="org2126cb8"></a>

### C050 [LOW] Temporary recursion-limit changes overwrite user/concurrent changes

The hook runs arbitrary Python while the interpreter-wide limit is temporarily
changed, then restores a stale saved value.  Hook or concurrent changes can be
silently lost.

-   Direction: avoid changing the global recursion limit per interception.
-   Test: coordinate two hooks and an intentional `setrecursionlimit` call.


<a id="orga83a04e"></a>

### C051 [LOW] `sys.apsw_should_fault = None` aborts the process

Unlike the other hook, this path does not treat `None` as disabled.  It
vectorcalls None, then executes `abort()` on the resulting error.

-   Direction: handle None/missing/non-callable hooks deterministically without
    abort during teardown.
-   Test: set None and trigger a named fault in a subprocess.


<a id="org7ca0bf4"></a>

### C052 [LOW] Fault-control result does not validate its exception class

The contract says `(int, class, str)`, but an arbitrary second item reaches
`PyErr_SetString` and yields an unrelated `SystemError`.

-   Direction: require `PyExceptionClass_Check` and report malformed hook output.
-   Test: return None, object instances, and exception instances as item two.


<a id="orgc832561"></a>

## src/fileio<sub>win32.c</sub>


<a id="orgb50665c"></a>

### C053 [LOW] Loadable-extension SQLite API thunk can be rewritten concurrently

The Windows fileio DLL shares the standard global `sqlite3_api` pointer between
upstream `fileio.c` and this helper.  Concurrent loads write it while callbacks
read it.  In the normal single-SQLite-runtime deployment every write uses the
same stable address, so practical impact is low despite formal C data-race UB.
Loading the same DLL through different SQLite runtimes is materially dangerous
because allocator/function tables can differ.

-   Direction: publish once under process-wide synchronization and reject a
    different API table.
-   Test: concurrent load/callback stress on Windows; separately test two SQLite
    runtimes.


<a id="org020c7e6"></a>

## src/fts.c


<a id="org4970e17"></a>

### C054 [HIGH] `xSetAuxdata` decrefs a borrowed value on SQLite failure

SQLite calls the supplied destructor on `xSetAuxdata` failure.  APSW passes the
borrowed Python value and increments it only after success, so failure consumes
a reference APSW never owned.

-   Direction: incref before the SQLite call; SQLite then owns/consumes that
    reference on both success and failure.  Make FI behavior model the contract.
-   Test: force aux-data failure with a weak-referenceable sentinel under
    ref-debug/ASan.


<a id="org7ac86f8"></a>

### C055 [HIGH] Ignored colocated tokens leak a `PyGILState_Ensure` level

The fast return for an ignored `FTS5_TOKEN_COLOCATED` token bypasses
`PyGILState_Release`.  A third-party tokenizer invoking without the GIL can
return while still holding/attaching Python state, causing deadlock or fatal
thread-state behavior.

-   Direction: route every return through paired release.
-   Test: a C tokenizer releases the GIL, emits normal plus colocated tokens, and
    restores it repeatedly under a watchdog.


<a id="orgbbd3254"></a>

### C056 [HIGH] Replacing SQL function `fts5` can cause a NULL API dereference

The API lookup treats any `SQLITE_ROW` as success and dereferences `api` without
checking that the canonical pointer-binding protocol populated it.  An
application function registered as one-argument `fts5` can return a row while
leaving it NULL.

-   Direction: require a non-NULL API pointer before reading `iVersion`.
-   Test: replace `fts5` with a scalar returning None before first lookup and
    require `NoFTS5Error`, not a crash.


<a id="orga8d44a2"></a>

### C057 [LOW] Several FTS tuple builders grow quadratically

Colocated tokens, instance tokens, and phrase columns repeatedly resize an
immutable tuple by one, producing worst-case O(n²) pointer copying.

-   Direction: accumulate in a list/amortized buffer and convert once.
-   Test: measure scaling at doubled large token/synonym counts.


<a id="orgbb5921d"></a>

### C058 [LOW] Tokenizer locale length narrows to `int` without a bound

A `Py_ssize_t` UTF-8 length is cast to the FTS `int` parameter.  The analogous
extension-API path already performs a bound check.

-   Direction: reject lengths above `INT_MAX` consistently.
-   Test: inject an oversized logical length and ensure the tokenizer is not
    called.


<a id="orgdf0bebc"></a>

### C059 [LOW] FTS5 v4 callback fields are used without checking `iVersion`

`xTokenize_v2` and `xColumnLocale` require extension API version 4.  The pinned
stock SQLite 3.53.4 supplies v4, so this is not reachable in the supported stock
pairing; a custom/replaced provider could supply v3 and cause an out-of-struct
function access.

-   Direction: check `iVersion`; use old `xTokenize` without locale or report
    unsupported locale APIs.
-   Test: use a shim exposing extension API v3.


<a id="orga9d438c"></a>

## src/jsonb.c


<a id="org70a9a30"></a>

### C060 [HIGH] Crafted eight-byte JSONB length wraps bounds and drives an OOB scan

The public nine-byte payload `fa ff ff ff ff ff ff ff ff` encodes
`JT_TEXTRAW` with length `SIZE_MAX`.  `value_offset + tag_len` wraps, passes the
bounds check, and later subtraction becomes `SIZE_MAX`, causing UTF-8 scanning
from one-past-end for an enormous apparent length.  Both detect and decode are
exposed.

-   Direction: parse lengths with checked arithmetic and compare by subtraction
    (`tag_len > end - value_offset`); reject high bytes on narrow platforms.
-   Test: run this and boundary-length corpora through detect/decode under ASan in
    a subprocess.


<a id="orgba64a85"></a>

### C061 [MEDIUM] Numeric subclasses are serialized through overridable `__str__`

With `exact_types=False`, int/float subclasses are accepted as their parent but
formatted by virtual `PyObject_Str`.  An override can change value 1 to 999 or
produce text tagged as numeric that APSW itself cannot decode.  Numeric mapping
keys have the same issue.

-   Direction: normalize to exact built-in numeric values before formatting.
-   Test: subclasses returning a different number and invalid text; compare with
    the documented parent-class semantics.


<a id="orge8d74d5"></a>

### C062 [MEDIUM] libc `realloc` failure returns without `MemoryError`

The code asserts that failed libc `realloc` set a Python exception.  It does
not: debug builds abort, while release builds eventually return NULL without
an exception and produce `SystemError`.

-   Direction: call `PyErr_NoMemory` explicitly.
-   Test: force this exact realloc failure without a preexisting exception.


<a id="org4c35f2b"></a>

### C063 [LOW] Object decoder leaks its partial builder on insertion failure

If tuple creation, list append, or dict insertion fails, key/value are cleaned
but the partially populated list/dict builder is not decref'd.

-   Direction: use one object-error cleanup path.
-   Test: fault second append/set and verify prior sentinel values are released.


<a id="org78bc4e1"></a>

### C064 [LOW] Hexadecimal `parse_int` callback receives base 0, not documented 16

Implementation and generated documentation disagree observably for custom
callbacks.

-   Direction: pass 16 or document and test base 0.
-   Test: decode positive/negative hexadecimal JSONB with a recording callback.


<a id="org93c8d1b"></a>

### C065 [LOW] Raw JSONB text is validated and UTF-8 decoded in two full passes

Non-ASCII `JT_TEXTRAW` is scanned by the custom validator, then processed again
by CPython's Unicode decoder.  This is linear, not a correctness issue.

-   Direction: benchmark a specialized allocation path using strict
    `PyUnicode_DecodeUTF8`, retaining custom validation for detection/escaped
    tags.
-   Test: preserve malformed UTF-8 behavior and benchmark multi-megabyte text.


<a id="orga6c5389"></a>

## src/pyutil.c


<a id="orgc6e33cb"></a>

### C066 [LOW] Note-formatting failure loses the pending exception

The original exception is fetched only after `PyUnicode_FromFormatV` succeeds.
If formatting fails, usually from OOM, its new exception replaces the original
without chaining.

-   Direction: fetch the original first; restore it or chain it to formatting
    failure.
-   Test: fault note formatting while argument conversion has already failed.


<a id="org86f335f"></a>

### C067 [MEDIUM] Exception chaining depends on private CPython symbols

`_PyErr_ChainExceptions*` is private and has already changed across Python
releases.  It is not a present runtime bug, but can become a compile/link/import
failure on a future supported CPython.

-   Direction: on Python 3.12+ use public `PyErr_GetRaisedException`,
    `PyException_SetContext`, and `PyErr_SetRaisedException`; retain a normalized
    fetch/restore implementation for 3.10-3.11.
-   Test: verify exception `__context__` on every supported minor and that no
    private symbol remains in the binary.


<a id="orgfc5ce41"></a>

### C068 [LOW] `PyThreadState_GetDict` NULL result is passed to a dict API

The thread-state dictionary may return NULL without an exception when lazy
creation fails.  The helper and the module-level `async_run_coro` getter pass it
directly to `PyDict_GetItemWithError`, while the setter passes it to
`PyDict_SetItem`; these calls can assert or crash.

-   Direction: check the dictionary first; on 3.13+ optionally use
    `PyDict_GetItemRef` after that check, with the GIL-protected fallback on
    3.10-3.12.
-   Test: fault first thread-state-dict allocation in an attached thread while
    getting, setting, and dispatching through `apsw.async_run_coro`.


<a id="org66c67c5"></a>

## src/session.c


<a id="org5d95db8"></a>

### C069 [HIGH] ChangesetIterator keeps a dangling borrowed TableChange pointer

`last_table_change` is not an owned reference.  Dropping the returned object
before the next iteration leaves a pointer that the iterator writes through.
Conversely, destroying the iterator finalizes SQLite state without
invalidating a surviving TableChange, whose properties then use freed iterator
and table-name storage.

-   Direction: own the last object, invalidate/clear it on advance and iterator
    teardown, and return a separate reference.
-   Test: discard between `next` calls and retain TableChange after deleting its
    iterator under ASan.


<a id="orgab35dd1"></a>

### C070 [HIGH] Several Session operations bypass the connection mutex and async affinity

`table_filter`, `config`, `memory_used`, and `changeset_size` access/mutate
SQLite Session state without `dbmutex` or async worker routing.  Replacing the
filter can decref a callback while SQLite still holds/uses its raw context;
64-bit state reads can race native updates.

-   Direction: route/reject async calls and serialize every Session API.  Swap
    callback references atomically with respect to SQLite work.
-   Test: write while repeatedly replacing filters and reading/configuring under
    ASan/TSan; cover async event-loop calls.


<a id="orga0f3f17"></a>

### C071 [HIGH] Schema-backed ChangesetBuilder uses a raw database without its mutex

`sqlite3changegroup_schema` retains the raw database; subsequent additions that
encounter a table for the first time can consult its schema/defaults.  Schema
setup and those add paths neither hold the connection mutex nor route to the
AsyncConnection worker, so they can race connection execution, schema changes,
or close.

-   Direction: serialize all schema-backed operations and define coherent async
    routing/rejection.
-   Test: concurrently alter/use schema while adding new-table changes under
    TSan; repeat with active async worker work.


<a id="org4e761d4"></a>

### C072 [MEDIUM] Session and ChangesetBuilder traversal is disabled by missing GC flags

Both types implement traverse but omit `Py_TPFLAGS_HAVE_GC`.  A Session/filter
cycle directly leaks native session and connection resources; builder cycles
can occur through connection callbacks/subclasses.

-   Direction: complete the GC protocol with safe clear/native teardown.
-   Test: collect filter-to-Session and callback-to-builder cycles.


<a id="org3ae5b86"></a>

### C073 [HIGH] Builder bookkeeping failure leaves SQLite holding an untracked database pointer

After SQLite successfully stores the raw database, weakref creation or dependent
list append can fail.  The builder remains usable but may not be registered for
closure before the Connection, allowing later use of a closed database.

-   Direction: arrange bookkeeping before commit where possible; otherwise poison
    and close the builder on any post-configuration failure.
-   Test: fault weakref/list append, close the connection, then verify the builder
    is safely unusable.


<a id="orgfaefcfd"></a>

### C074 [MEDIUM] Public `session_config` permits SQLite-defined undefined states

SQLite says `sqlite3session_config` is not thread-safe and is undefined during
Session calls or after any Session-related object exists.  APSW exposes it at
all times with no lifecycle state or process-wide synchronization.  It is a
low-level API whose upstream contract is linked, so impact depends on caller
use.

-   Direction: restrict it to a synchronized pre-object initialization phase and
    document untrackable external SQLite users.
-   Test: reject calls after creating any session/changegroup/rebaser object and
    stress concurrent configuration.


<a id="org0894a74"></a>

### C075 [MEDIUM] Oversized builder BLOB path leaks its buffer export

After buffer acquisition, the `len >= INT32_MAX` branch jumps to failure
without `PyBuffer_Release`, retaining the exporter and potentially preventing
resize forever.

-   Direction: release on every path or use the bounded buffer helper.
-   Test: custom oversized exporter should record exactly one release.


<a id="orga9d2b34"></a>

### C076 [LOW] Large callback-free changeset operations retain the GIL

Invert, concat, changegroup output, and rebase can be CPU/memory intensive, and
all four retain the GIL around the SQLite call.  Invert and concat use acquired
input buffers rather than mutable APSW native handles, but writable exporters
could still be mutated concurrently after releasing the GIL.  Changegroup
output and rebase additionally require native-handle lifetime and operation
synchronization.

-   Direction: before benchmarking allow-threads regions for invert and concat,
    require read-only inputs, copy writable buffers, or otherwise synchronize
    mutation.  Add object locking/lifetime protection before doing the same for
    changegroup output and rebase.
-   Test: process large changesets while a heartbeat thread progresses.


<a id="org02e52b7"></a>

## src/statementcache.c


<a id="org9487f3f"></a>

### C077 [HIGH] Empty SQL with explain mode passes NULL to `sqlite3_stmt_explain`

`sqlite3_prepare_v3` legitimately returns success with a NULL statement for
empty/comment/separator-only SQL.  APSW records that fact, then calls
`sqlite3_stmt_explain` whenever explain is nonnegative.  Without SQLite API
armor, the implementation dereferences NULL.

-   Direction: call explain only for a non-NULL statement.
-   Test: run empty/comment/semicolon SQL with explain 0, 1, and 2 in a subprocess.


<a id="org1282b86"></a>

### C078 [LOW] Recycle bin uses only three of four slots

The insertion condition has an off-by-one, preventing use of index three.

-   Direction: compare `next < entries`.
-   Test: expose counters in a test build and recycle/allocate four entries.


<a id="org75cb2f0"></a>

### C079 [LOW] Cache statistics wrap after `UINT_MAX`

Lifetime counters and entry uses are unsigned 32-bit on common platforms and
wrap after roughly 4.29 billion events.  Wrapping is defined C behavior but
makes long-lived statistics decrease/reset.

-   Direction: use 64-bit counters and Python's unsigned-long-long conversion.
-   Test: initialize near the limit through a test hook and assert monotonicity.


<a id="org31072f6"></a>

### C080 [HIGH] Valid hash `-1` collides with the empty-slot sentinel on 32-bit

The DJB2 hash is narrowed to `Py_hash_t`; on 32-bit, 0xffffffff becomes -1,
the initialized empty marker.  Lookup compares hash before checking the cache
pointer and dereferences NULL.  Valid SQL strings can produce that hash.

-   Direction: use `caches[i] == NULL` as occupancy or remap the sentinel, and
    check the pointer before dereference.
-   Test: execute a known -1-hash SQL string on 32-bit with caching enabled.


<a id="org5c94d07"></a>

### C081 [MEDIUM] `sqlite3_stmt_explain` can reprepare while holding the GIL

SQLite documents that explain mode changes can invoke a full reprepare.  Initial
prepare releases the GIL, but this potentially comparable work does not.

-   Direction: after callback/error audit, use the same allow-threads pattern as
    prepare.
-   Test: large explain-mode reprepare while another Python thread progresses.


<a id="orge445c23"></a>

## src/stringconstants.c

No independent defect was accepted.  The file is generated by
`tools/genstrings.py`.  Its process-global Python object table is evidence for
C002 and should be migrated only as part of the extension-wide module-state
work.  The proposed performance change to intern every cached name was rejected
as speculative without a benchmark.


<a id="orgc3a9ea0"></a>

## src/testextension.c


<a id="orgf3492ac"></a>

### C082 [LOW] Test extension reports successful loading when function registration fails

Both entry points discard `sqlite3_create_function_v2` results and always
return success.  OOM can leave a new function uninstalled; an
active-statement `SQLITE_BUSY` while replacing the same signature can instead
leave the prior registration unchanged.  In both cases, the requested
registration failed while tests believe loading succeeded.

-   Direction: return the registration result and optionally populate the load
    error message.
-   Test: reload while a statement using the function remains active; fault OOM.


<a id="org53ccb28"></a>

### C083 [LOW] Test extension's standard SQLite API thunk has the same qualified race

Concurrent loads rewrite the global standard `sqlite3_api` pointer while
callbacks read it.  Same-runtime loads store the same pointer and practical
impact is low; different SQLite runtimes are dangerous.  This is a test-only
artifact and the standard loadable-extension pattern.

-   Direction: publish once/reject a different thunk if cross-runtime loading is
    within scope.
-   Test: concurrent load/callback TSAN harness.


<a id="org50bcb7d"></a>

## src/traceback.c

No actionable findings.  Exception preservation, ownership, GIL/thread-state
assumptions, and the Python 3.10-only direct frame field access were validated.
The proposed code/frame caching optimization was rejected as low-value work on
an exception-only path.


<a id="orgd0e2658"></a>

## src/unicode.c


<a id="orgabba240"></a>

### C084 [HIGH] Mapper reinitialization leaks resources and can leave stale/dangling state

Python permits explicit repeated `__init__` calls.  To-UTF8 overwrites an
existing export/string without cleanup and a failed decode leaves a callable
with released buffer state.  From-UTF8 leaks its old bytes owner and retains
stale offsets.  OffsetMapper finalizes content but does not reset length, so
later materialization can expose incorrectly initialized Unicode storage.

-   Direction: prohibit repeated init with the project's existing guard, or build
    new state transactionally and reset every field.
-   Test: successful and failing reinit after advancing each mapper under ASan and
    fault injection.


<a id="orgeb83033"></a>

### C085 [MEDIUM] OffsetMapper has reachable signed offset overflow

A source offset of `PY_SSIZE_T_MAX` plus an interpolated character displacement
can overflow without requiring a huge string.  Length/count allocation
arithmetic has additional unchecked sites.

-   Direction: use checked `Py_ssize_t` addition/multiplication and raise
    `OverflowError`.
-   Test: add a multi-character segment at `sys.maxsize` and query its second
    output character under UBSan.


<a id="org447530c"></a>

### C086 [LOW] Mutable UTF-8 buffers invalidate mapper validation

The mapper validates once but retains a live writable `PyBUF_SIMPLE` view and
rescans it later.  Mutation can disagree with the decoded string, return bogus
offsets, or abort assert-enabled builds on malformed continuation bytes.

-   Direction: snapshot immutable bytes, reject writable exporters, or derive
    widths from the immutable decoded string.
-   Test: mutate bytearray/memoryview after construction.


<a id="org6dad653"></a>

### C087 [LOW] Two mapper types can form cycles invisible to GC

To-UTF8 retains arbitrary exporters and OffsetMapper can retain string
subclasses; either can hold a back-reference.  From-UTF8 retains an exact bytes
object and is not clearly affected.

-   Direction: add GC protocol to affected types or snapshot exact built-ins.
-   Test: exporter/string-subclass back-reference cycles should collect.


<a id="orgf282ebb"></a>

### C088 [LOW] Extreme `grapheme_find` bounds underflow signed arithmetic

With `end == PY_SSIZE_T_MIN` and a longer substring, normalization subtracts
before the existing early-out and can underflow.  The result is normally later
discarded, limiting practical impact.

-   Direction: reject longer needles first and use checked/slice-normalization
    arithmetic.
-   Test: compare extreme bounds with `str.find` under UBSan.


<a id="orgb03399e"></a>

### C089 [LOW] Unicode mapper allocation failures omit `MemoryError`

Failed `PyMem_Resize~/~PyMem_Calloc` returns NULL without setting an exception,
leading to `SystemError`.

-   Direction: call `PyErr_NoMemory` while retaining old valid state.
-   Test: fault both allocations independently.


<a id="org586b040"></a>

### C090 [LOW] Negative grapheme slicing boxes every boundary

Resolving a negative endpoint builds a list and `PyLong` for every grapheme,
using O(n) Python objects in addition to the necessary O(n) scan.

-   Direction: count clusters, normalize the slice, then scan to the two required
    boundaries with O(1) auxiliary memory.
-   Test: benchmark peak memory on multi-million-character strings.

C004 is rooted in `src/argparse.c` but has its principal sinks here.  C005 and
C007 also include Unicode mapper call sites and are not duplicated.


<a id="org7804d4b"></a>

## src/util.c


<a id="org7fd6e9f"></a>

### C091 [HIGH] Failed `PyMem_Resize` destroys the pending-call queue pointer

`PyMem_Resize` is an assigning macro.  On failure it overwrites the global
`pending_call_slots` with NULL while count remains nonzero, leaks the old
allocation, and leaves the already registered dispatcher to index NULL.  It
also fails to establish `MemoryError`.

-   Direction: use a temporary `PyMem_Realloc` result with checked size, update
    globals only after success, and set `PyErr_NoMemory`.
-   Test: fail growth with an existing queued destructor; the original callback
    must still run without a crash.


<a id="orgd296f74"></a>

### C092 [MEDIUM] Virtual-table IN conversion discards meaningful SQLite statuses

A non-OK result from `sqlite3_vtab_in_first` falls through to ordinary tagged
pointer/NULL conversion, discarding the status.  Ordinary `SQLITE_ERROR` is
expected when probing a non-IN NULL, but `SQLITE_DONE` should represent an empty
collection and failures such as `SQLITE_NOMEM` should propagate.

-   Direction: distinguish DONE, expected ERROR fallback, and real errors.
-   Test: empty all-at-once IN plus faulted NOMEM.


<a id="orgbffef7b"></a>

### C093 [MEDIUM] Pending destructor queue has quadratic growth and permanent high-water scans

Every enqueue scans from zero.  When all existing slots are occupied, the array
grows by exactly one, so a burst of N simultaneously occupied entries costs
O(n²) slot probes and N one-slot reallocations; copying may also become
quadratic depending on realloc behavior.  The count never shrinks, so later
dispatch scans the largest historical burst.

-   Direction: track capacity separately, grow geometrically, and maintain free
    slots/trim trailing holes.
-   Test: instrument allocations and probes during a large contention burst.

The global queue also reinforces C002: if subinterpreters were supported, a
pending callback could run an object's destructor in the wrong interpreter.
C007 is the accepted common VLA issue and includes the macro definitions here.


<a id="orgd75af47"></a>

## src/vfs.c


<a id="org110eee3"></a>

### C094 [HIGH] Derived VFS can outlive an unretained APSW base

APSW detects whether a base is an APSW object by comparing only `xAccess` with
the APSW trampoline.  A base created with `exclude={"xAccess"}` has NULL there,
so the child takes no strong Python reference.  Deleting the base unregisters
and frees its `sqlite3_vfs` while the child retains a dangling pointer.

-   Direction: carry an explicit APSW base owner independent of excluded methods.
-   Test: derive from a base excluding xAccess, delete it, then use and destroy the
    child under ASan.


<a id="org59941a8"></a>

### C095 [HIGH] `xSetSystemCall(NULL, ...)` passes NULL to `PyUnicode_FromString`

SQLite permits NULL to reset all system calls and APSW documents `name=None`.
The inherited C-to-Python trampoline unconditionally constructs Unicode from
that NULL pointer.

-   Direction: pass `Py_None`, as the next-system-call bridge already does.
-   Test: through two APSW VFS layers call `xSetSystemCall(None, 0)`.


<a id="org6127ffa"></a>

### C096 [MEDIUM] System-call bridge changes valid False/None semantics

The setter ignores its Python result, converting False/unknown into
`SQLITE_OK`.  The getter rejects documented None/unknown because it accepts only
integers.

-   Direction: require bool for set and map False to `SQLITE_NOTFOUND`; map None
    to a NULL function pointer for get.
-   Test: query/set an unknown call through nested VFS layers.


<a id="orgbbd0e29"></a>

### C097 [MEDIUM] Inherited short reads discard real trailing NUL bytes

SQLite zero-fills the unread suffix on short read.  APSW infers actual length by
stripping all trailing zeroes, which also strips bytes genuinely present at
EOF.

-   Direction: determine available bytes from file size/offset or expose an
    unambiguous short-read result; do not infer from byte values.
-   Test: read beyond files ending in one or more NUL bytes.


<a id="orgef02e48"></a>

### C098 [MEDIUM] User-controlled `mxPathname` causes signed overflow and invalid sizes

Negative and near-`INT_MAX` values are accepted.  `mxPathname + 1` and
`512 + mxPathname` are evaluated as signed int before allocation conversion.

-   Direction: validate constructor values and perform checked `size_t` arithmetic.
-   Test: negative and INT<sub>MAX</sub> values under UBSan.


<a id="orgfb2e9e5"></a>

### C099 [LOW] Failed VFS `xOpen` does not explicitly clear `pMethods`

SQLite's contract requires valid methods on success and NULL on failure.
Normal SQLite allocation is zeroed, reducing practical exposure, but this
trampoline returns through many failures without establishing the field for a
direct/nonstandard caller.

-   Direction: set `pMethods` and owned file state NULL immediately on entry.
-   Test: invoke through a C shim with nonzero-prefilled storage and forced Python
    failure.


<a id="orgecb9db4"></a>

### C100 [LOW] Every `xSync` callback drops an owned flags-integer reference

`PyLong_FromLong(flags)` returns an owned reference that is never decref'd on
success or failure.  SQLite sync flags are normally small cached integers and
are immortal on CPython 3.12+, so this is primarily an ownership/refcount
defect rather than an unbounded allocation leak in normal supported use.

-   Direction: decref the argument after vectorcall.
-   Test: on CPython 3.10-3.11, force many syncs and verify that the cached flags
    integer's refcount remains stable; retain static ownership checking for
    versions with immortal small integers.


<a id="org0e19e86"></a>

### C101 [MEDIUM] `xGetLastError` truncates every nonempty message by one byte

The code copies `len` bytes then writes NUL at `len - 1`, replacing the last
payload byte even with ample output space.

-   Direction: reserve one byte, copy at most `nByte - 1`, terminate at the copied
    length.
-   Test: return a unique marker and verify every byte survives.


<a id="org2eeb05b"></a>

### C102 [MEDIUM] Optional VFS attribute checks suppress real lookup exceptions

`PyObject_HasAttr` converts descriptor/~\_<sub>getattribute</sub>\_\_~ failures into absence
(or an unraisable report, depending on Python), and later code may perform a
second lookup.

-   Direction: on Python 3.13+ use `PyObject_GetOptionalAttr` or
    `PyObject_HasAttrWithError`.  On 3.10-3.12 use `PyObject_GetAttr`, clear only
    `AttributeError`, and call the obtained bound method once.
-   Test: descriptors that raise non-AttributeError and whose result changes on a
    second lookup.


<a id="org6945386"></a>

## src/vtable.c


<a id="org32412f8"></a>

### C103 [MEDIUM] BestIndex argument indexes overflow or continue with an exception

Both accepted forms perform `PyLong_AsInt(value) + 1` without checking the
conversion.  `INT_MAX + 1` is signed UB; an oversized Python int leaves
`OverflowError` pending while planning can continue/return success.  Negative
indexes are also invalid for SQLite's positive `argvIndex`.

-   Direction: convert to a temporary and check immediately; require a valid
    zero-based index within the usable-constraint count, then add one, and retain
    the existing contiguity/uniqueness contract.
-   Test: direct and tuple forms containing -2, -1, the usable-constraint count,
    INT<sub>MAX</sub>, and huge integers under UBSan.


<a id="orgb88ae1d"></a>

### C104 [MEDIUM] BestIndex leaks `idxStr` and an exceptional `idxnum`

`PySequence_GetItem` returns owned references.  Successful None/string
`idxStr` paths never decref it; an overflowing `idxnum` jumps away before its
decref.  Planning repeated uncached statements grows references.

-   Direction: one local cleanup path for both values.
-   Test: fresh finalizable string subclasses and oversized index numbers across
    repeated planning.


<a id="orgd222f6b"></a>

### C105 [MEDIUM] Optional virtual-table method checks suppress descriptor exceptions

`PyObject_HasAttr` can turn a real lookup error for Savepoint, Rename,
Integrity, transaction, disconnect, find-function, or shadow-name methods into
“method absent,” allowing SQLite work to continue incorrectly.

-   Direction: use the same versioned optional-attribute helper described in
    C102 and avoid duplicate lookup.
-   Test: raise `RuntimeError` only while looking up each optional method;
    preserve ordinary `AttributeError` absence.


<a id="orgea9827e"></a>

### C106 [MEDIUM] Several vtable allocation/argument failures violate exception invariants

Raw `PyMem_Calloc` failures reach handlers that assume an exception, or return
NULL without one.  Savepoint/Release can fail constructing the level integer
and still return `SQLITE_OK` with an exception pending.

-   Direction: set `PyErr_NoMemory` immediately and route all argument failures
    through the callback's SQLite error conversion.
-   Test: fault every listed allocation/construction and require clean
    ~MemoryError~/SQLite failure, no assertion/SystemError.


<a id="orgc8623eb"></a>

### C107 [LOW] Embedded NUL in BestIndex `idxStr` is silently truncated

Python Unicode can contain NUL, but copying through `sqlite3_mprintf("%s")`
stores only the prefix.  SQLite cannot represent NUL in `idxStr`, so silent
semantic change is inappropriate.

-   Direction: use `PyUnicode_AsUTF8AndSize` and reject embedded NUL explicitly.
-   Test: return/assign `"prefix\0suffix"` and assert a clear planning exception.

C031 is the sole root report for the related module-registration double free;
`apswvtabFree` itself is a normal destructor.


<a id="orga910b17"></a>

# Consolidated CPython C-API opportunities

These are not additional finding IDs unless tied to a defect above.

1.  **PEP 489/per-module state**: multi-phase initialization predates Python 3.10.
    `PyType_FromModuleAndSpec`, `PyType_GetModule`, and
    `PyType_GetModuleState` are available since Python 3.9 and therefore need no
    fallback at the project's 3.10 floor.  This is the correct foundation for
    C002 and C044.
2.  **Interpreter capability slots**: `Py_mod_multiple_interpreters` was added in
    Python 3.12; guard with `PY_VERSION_HEX >= 0x030c0000` and initially declare
    unsupported.  `Py_mod_gil` was added in 3.13; guard with
    `PY_VERSION_HEX >= 0x030d0000` and continue declaring/using the GIL.  Do not
    opt into `Py_MOD_GIL_NOT_USED` with current object/global synchronization.
3.  **Exception APIs**: `PyErr_GetRaisedException` and
    `PyErr_SetRaisedException` were introduced in Python 3.12.  Use them with
    `PyException_SetContext` to replace private chaining APIs; retain a
    fetch/normalize/restore fallback on 3.10-3.11 (C067).
4.  **Optional attributes**: `PyObject_GetOptionalAttr` and
    `PyObject_HasAttrWithError` were introduced in Python 3.13.  Use a
    `PyObject_GetAttr` fallback that clears only `AttributeError` on 3.10-3.12
    (C102/C105).
5.  **Unicode keyword comparison**: `PyUnicode_EqualToUTF8` was introduced in
    Python 3.13.  Use a length-aware UTF-8 fallback on 3.10-3.12 (C006).
6.  **Strong item references**: `PyList_GetItemRef` and `PyDict_GetItemRef` were
    introduced in Python 3.13.  They can harden future no-GIL code but are not a
    substitute for object-level locks.  A checked dict plus `Py_NewRef` remains
    the 3.10-3.12 GIL-protected fallback (C068).
7.  **Managed weakrefs**: `Py_TPFLAGS_MANAGED_WEAKREF` was introduced in Python
    3.12.  It is optional layout cleanup, not a current correctness fix.
8.  **Public allocation APIs**: replacing private `_PyObject_New` and
    `_PyObject_NewVar` with public allocation/type slots is desirable source
    hygiene, but no allocator mismatch was validated in the reviewed sites.


<a id="orga980bac"></a>

# Rejected or consolidated candidates

-   The proposed concurrent logger replacement UAF was rejected as an
    unconditional defect: reproducing it requires calling `sqlite3_config` while
    another SQLite API/log callback is active, contrary to SQLite's global
    quiescence contract.  APSW may still document or defensively enforce that
    precondition.
-   Current free-threaded races premised on `Py_MOD_GIL_NOT_USED` were not logged
    as active defects.  APSW currently causes free-threaded CPython to retain the
    GIL.  They remain blockers to a future no-GIL declaration.
-   Repeated process-global findings in stringconstants, async caches, pending
    calls, and static types were consolidated into C002.
-   The Unicode offset and VLA issues are rooted in argparse/common macros and are
    not duplicated at every sink.
-   The Blob weakref leak is rooted in `src/connection.c` (C032), not
    `src/blob.c`.
-   The virtual-table registration double free is rooted in
    `src/connection.c` (C031), not the normal destructor in `src/vtable.c`.
-   Private allocator/tuple-resize use was not treated as a current correctness
    bug where ownership and matching free paths were valid.
-   Interning every generated string constant was rejected as speculative without
    profile/benchmark evidence.


<a id="org616baad"></a>

# Per-file coverage

-   `src/apsw.c`: C001-C003.
-   `src/argparse.c`: C004-C008.
-   `src/async.c`: C009-C012.
-   `src/backup.c`: C013-C014.
-   `src/blob.c`: C015-C021.
-   `src/carray.c`: C022-C029.
-   `src/connection.c`: C030-C036.
-   `src/constants.c`: C037 (generated; fix generator).
-   `src/cursor.c`: C038-C043.
-   `src/exceptions.c`: C044-C046.
-   `src/faultinject.c`: C047-C052 (fault-injection builds only).
-   `src/fileio_win32.c`: C053 (Windows fileio extension only).
-   `src/fts.c`: C054-C059.
-   `src/jsonb.c`: C060-C065.
-   `src/pyutil.c`: C066-C068.
-   `src/session.c`: C069-C076.
-   `src/statementcache.c`: C077-C081.
-   `src/stringconstants.c`: no independent finding; generated evidence for C002.
-   `src/testextension.c`: C082-C083 (test extension only).
-   `src/traceback.c`: no actionable findings.
-   `src/unicode.c`: C084-C090; sinks/duplicates for C004, C005, C007.
-   `src/util.c`: C091-C093; common definitions/evidence for C002 and C007.
-   `src/vfs.c`: C094-C102.
-   `src/vtable.c`: C103-C107; destructor evidence for C031.
-   `src/_unicodedb.c`: intentionally skipped as generated.
