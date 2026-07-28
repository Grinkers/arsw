
# Table of Contents

1.  [Scope and method](#org8a3471d)
2.  [Executive summary](#org2dee121)
3.  [Findings](#org334ae96)
    1.  [apsw/\_<sub>main</sub>\_<sub>.py</sub>](#orgd6732e4)
    2.  [apsw/aio.py](#org741dcf7)
        1.  [PY001 [MEDIUM] Prefetched exceptions are reconstructed incorrectly on Python 3.10–3.11](#orga72d26e)
        2.  [PY002 [MEDIUM] Cancellation can orphan an asyncio task during callback handoff](#org6f7859a)
        3.  [PY003 [MEDIUM] Auto controller detection rejects valid non-canonical dependency versions](#org0fc43fe)
        4.  [PY004 [LOW] contextvar<sub>set</sub> has an incorrect pre-Python-3.14 return annotation](#org8ae63bf)
    3.  [apsw/bestpractice.py](#org39144c3)
        1.  [PY005 [MEDIUM] Foreign-key helper can silently leave constraints disabled in a transaction](#orgb975fb1)
        2.  [PY006 [LOW] WAL helper discards the effective journal mode](#orgb6ae993)
        3.  [PY007 [LOW] connection<sub>optimize</sub> understates immediate ANALYZE and write behavior](#org1900c8f)
    4.  [apsw/ext.py](#org0a006e5)
        1.  [PY008 [MEDIUM] Default virtual-table rowids are not reliably unique](#org248543e)
        2.  [PY009 [MEDIUM] log<sub>sqlite</sub> permanently lowers the configured severity after notices](#orgb5bed5c)
        3.  [PY010 [MEDIUM] dbinfo mixes attached-schema bytes with main-schema metadata](#org2fcec4f)
        4.  [PY011 [LOW] DataClassRowFactory rename mode can still generate duplicate field names](#orgb09dd1a)
        5.  [PY012 [LOW] Virtual-table declaration identifiers are not safely escaped](#org7c350bd)
        6.  [PY013 [LOW] Nested virtual-table prefixes assign shadow tables to the wrong group](#orgb515d7e)
        7.  [PY014 [MEDIUM] query<sub>info</sub> actions mode bypasses an existing authorizer](#orge426f36)
        8.  [PY015 [LOW] DataClassRowFactory mutates caller-owned namespace mappings](#orgfda8510)
    5.  [apsw/fts5.py](#orgd86b42e)
        1.  [PY016 [MEDIUM] FTS5 option keys are parsed case-sensitively](#org457d07e)
        2.  [PY017 [MEDIUM] NGramTokenizer omits the final fallback query n-gram](#org11feefa)
        3.  [PY018 [MEDIUM] JSONTokenizer rejects valid escaped slashes](#orgd49bfe5)
        4.  [PY019 [LOW] text<sub>for</sub><sub>token</sub> limits occurrences rather than documents](#orgb7dbbed)
        5.  [PY020 [MEDIUM] CLI custom tokenizer registration uses a literal name](#org89af8a7)
        6.  [PY021 [MEDIUM] Tokenizer wrapper factories share mutable callback state](#org9f33f45)
        7.  [PY022 [LOW] more<sub>like</sub> constructs an invalid empty OR query](#org781026b)
        8.  [PY023 [LOW] Cache refresh uses a new lock for every attempt](#org6296976)
    6.  [apsw/fts5aux.py](#org4c5e7ad)
        1.  [PY024 [MEDIUM] BM25 caches first-invocation weights for an entire MATCH query](#orgb4a1108)
        2.  [PY025 [LOW] BM25 does not match SQLite conversion of SQL weight values](#org6d016f8)
    7.  [apsw/fts5query.py](#org4a0e1a9)
        1.  [PY026 [HIGH] QueryTokens encoding is not reversible](#org126e774)
        2.  [PY027 [HIGH] Right-nested NOT is serialized with changed semantics](#org3002e43)
        3.  [PY028 [MEDIUM] Nested column filters serialize to invalid FTS5 syntax](#orgf569ee0)
        4.  [PY029 [MEDIUM] Parser rejects implicit AND after a column-filtered NEAR group](#org2a569c0)
        5.  [PY030 [MEDIUM] QueryTokens cannot use the advertised JSON dictionary representation](#org7a68099)
        6.  [PY031 [MEDIUM] Structured NEAR distances reject zero and accept booleans](#orgde69afe)
        7.  [PY032 [MEDIUM] Parser accepts caret inside NEAR where FTS5 rejects it](#orgb7dc3c6)
        8.  [PY033 [LOW] Structured collection fields treat strings as character sequences](#org1b9e675)
    8.  [apsw/shell.py](#org4cbcbb0)
        1.  [PY034 [MEDIUM] JSON and JSONL emit invalid JSON for blobs over 57 bytes](#org9c5535c)
        2.  [PY035 [MEDIUM] Valid quoted identifiers are rendered as malformed SQL](#org15de914)
        3.  [PY036 [MEDIUM] NO<sub>COLOR</sub> consumes unrelated command-line options](#org258c223)
        4.  [PY037 [MEDIUM] Input decoding ignores the configured error handler](#orge4b6dff)
        5.  [PY038 [MEDIUM] Echo mode replaces binding diagnostics with AttributeError](#orgb416cde)
        6.  [PY039 [LOW] Empty JSON result sets produce no JSON document](#org37294ee)
        7.  [PY040 [LOW] Auto-import classifies impossible calendar dates as dates](#org15af178)
        8.  [PY041 [LOW] &ndash;no-unicode enables Unicode line drawing](#org6380e32)
        9.  [PY042 [LOW] Completion termination indexes past the completion list](#org89ef6b9)
        10. [PY043 [LOW] Unicode SQL error rendering can replace the original exception](#org94e3268)
        11. [PY044 [LOW] .shell reports encoded POSIX wait status rather than exit code](#org9629b2b)
    9.  [apsw/speedtest.py](#org250f3de)
        1.  [PY045 [HIGH] Existing database paths are deleted without explicit consent](#org5b67cb4)
        2.  [PY046 [HIGH] &ndash;data-size makes compared workloads unequal and run-order-dependent](#org891b0c7)
        3.  [PY047 [MEDIUM] Indexed lookup workloads frequently target absent or biased keys](#org898b0ce)
        4.  [PY048 [MEDIUM] Correctness mismatches do not fail the process](#orgf376a48)
        5.  [PY049 [MEDIUM] A single iteration crashes summary generation](#orgf507012)
        6.  [PY050 [MEDIUM] &ndash;vfs is silently ignored by sqlite3 runs](#org13a6edc)
        7.  [PY051 [LOW] Numeric options accept invalid or meaningless configurations](#org07e08ce)
        8.  [PY052 [LOW] Elapsed benchmark timing uses an adjustable wall clock](#org889ad69)
    10. [apsw/sqlite<sub>extra.py</sub>](#org5e5353d)
        1.  [PY053 [MEDIUM] load unnecessarily enables SQL load<sub>extension</sub> and changes state before validation](#orgbbc36fd)
        2.  [PY054 [LOW] Explicit &ndash;help exits unsuccessfully and writes help to stderr](#org9b32e31)
    11. [apsw/tests/\_<sub>main</sub>\_<sub>.py</sub>](#org36f6310)
        1.  [PY055 [MEDIUM] \`assertRaisesRoot\` callable mode succeeds when no exception is raised](#org9dab149)
        2.  [PY056 [MEDIUM] Source-order checks omit required adjacent comparisons](#orgd5baf67)
        3.  [PY057 [MEDIUM] Repeated test runs overwrite an earlier failing exit status](#orgfbe4f04)
        4.  [PY058 [LOW] Nonpositive repeated-run counts leave the exit status uninitialized](#org334e322)
        5.  [PY059 [MEDIUM] \`assertTablesEqual\` compares the left schema with itself](#org245d282)
        6.  [PY060 [LOW] \`testSanity\` discards a mapping consistency comparison](#org292ba09)
    12. [apsw/tests/shelltest.py](#org4089c2e)
        1.  [PY061 [MEDIUM] \`.load\` coverage is disabled by a misspelled capability check](#orgcb06bd7)
        2.  [PY062 [MEDIUM] Expected-error paths pass when \`shellclass.Error\` is not raised](#orgcfc78ec)
        3.  [PY063 [LOW] Invalid-encoding \`.autoimport\` diagnostic fixture is skipped](#org9810752)
        4.  [PY064 [LOW] \`ResourceWarning\` suppression has execution-context-dependent global scope](#orgc41fb93)
    13. [apsw/tests/aiotest.py](#orgaaf7e1c)
        1.  [PY065 [MEDIUM] \`testOverwrite\` leaves \`apsw.async<sub>run</sub><sub>coro</sub>\` modified on the runner thread](#orgaa24785)
        2.  [PY066 [LOW] \`allow<sub>missing</sub><sub>dict</sub><sub>bindings</sub>\` is restored to a hard-coded value](#orgb089a9d)
        3.  [PY067 [LOW] Best-practice test cleanup replaces all pre-existing connection hooks](#org109a28a)
        4.  [PY068 [MEDIUM] Optional dependency version parsing rejects valid PEP 440 release spellings](#org4c2cec6)
        5.  [PY069 [MEDIUM] Effective-deadline assertions accept incorrect deadline propagation](#orge2862f5)
        6.  [PY070 [LOW] Async tests rely on private unittest result internals](#org906375e)
    14. [apsw/tests/async<sub>meta.py</sub>](#org40fc2a4)
        1.  [PY071 [LOW] Async metadata verification is absent from ordinary suite entry points](#org6636dca)
        2.  [PY072 [MEDIUM] Worker-side attribute classification does not await queued requests](#orgc61ac6d)
        3.  [PY073 [LOW] Runtime metadata verification unconditionally requires Session support](#org9986655)
        4.  [PY074 [LOW] \`apsw.async<sub>controller</sub>\` is not restored after metadata testing](#orge1f1a23)
    15. [apsw/tests/carray.py](#org981a16f)
        1.  [PY075 [LOW] Explicit INT32 reinterpretation has an inadequate oracle](#org576f29c)
        2.  [PY076 [LOW] Missing or broken NumPy is reported as a passing test](#orgb96913b)
    16. [apsw/tests/extratest.py](#org1eb3423)
        1.  [PY077 [MEDIUM] FileIO capability guard always bypasses the behavioral test](#org6a3fc12)
        2.  [PY078 [LOW] fsdir validation treats the root directory row as a file](#orga3197d8)
        3.  [PY079 [MEDIUM] sqlite<sub>stmt</sub> is excluded only from the pre-load module snapshot](#orge7c07d4)
        4.  [PY080 [MEDIUM] SQLAR command failures and crashes are accepted](#orgbf77e14)
        5.  [PY081 [MEDIUM] sqlite3<sub>getlock</sub> output and stderr assertion failures are swallowed](#org77be916)
        6.  [PY082 [LOW] OpenBSD bypasses subprocess return-code validation](#orgc1f29a6)
    17. [apsw/tests/fork<sub>checker.py</sub>](#orga6e40b5)
        1.  [PY083 [MEDIUM] Fork-violation checks succeed even when no violation is observed](#org0ad8c49)
        2.  [PY084 [MEDIUM] testManyConnections runs after the checker has been disabled](#org2e5f308)
        3.  [PY085 [LOW] Unexpected child exceptions lose their traceback diagnostic](#org7ec937e)
        4.  [PY086 [LOW] Imported-module warning suppression is ineffective](#orgded37bc)
    18. [apsw/tests/ftstests.py](#org34c3ae7)
        1.  [PY087 [MEDIUM] FTS5 auxiliary test class is omitted from normal-suite discovery](#org6ceb9f5)
        2.  [PY088 [MEDIUM] UnicodeData validation processes only the final record](#orgaef953d)
        3.  [PY089 [LOW] Unicode break subprocess reopens an open NamedTemporaryFile](#org141731a)
        4.  [PY090 [LOW] FTS5 auxiliary comparisons allow truncated actual results](#org0e8c64a)
    19. [apsw/tests/jsonb.py](#org341de51)
        1.  [PY091 [LOW] Tab-to-vertical-tab normalization hides decoding errors](#org51f54db)
        2.  [PY092 [LOW] Core JSONB checks disappear under optimized Python](#org510954d)
        3.  [PY093 [LOW] Decimal context precision leaks across tests](#orgb2e7bb8)
    20. [apsw/tests/sessiontests.py](#org6a81ed7)
        1.  [PY094 [LOW] Global session stream-size configuration leaks process state](#org7a2b003)
        2.  [PY095 [LOW] table<sub>filter</sub> relies on an assertion with required side effects](#orgc9d30e9)
    21. [apsw/trace.py](#orga4c47c8)
        1.  [PY096 [MEDIUM] Unbounded retention and duplication of statement timings](#org7b41c57)
        2.  [PY097 [MEDIUM] Tracer callback slots silently collide with application callbacks](#orgef8bffb)
        3.  [PY098 [MEDIUM] Target executes in the tracer's existing `__main__` namespace](#orga021254)
        4.  [PY099 [MEDIUM] Options intended for the target are rejected](#orgb01a528)
        5.  [PY100 [MEDIUM] Blob logging changes byte values and marks exact-limit values as truncated](#orgf4b46a3)
        6.  [PY101 [MEDIUM] Individual timing output can discard significant leading digits](#org41dff85)
        7.  [PY102 [LOW] String and SQL logging permits ambiguous delimiters and raw terminal controls](#org75f1838)
        8.  [PY103 [LOW] Negative numeric limits receive accidental slicing semantics](#org8aa7c69)
    22. [apsw/unicode.py](#org532b082)
        1.  [PY104 [MEDIUM] `text_wrap` disables hyphenation for multiline input](#orgdfdf5c6)
        2.  [PY105 [MEDIUM] `expand_tabs` does not preserve line-break count or terminal-break state](#org94b51cb)
        3.  [PY106 [LOW] `text_wrap` accepts invalid arguments and can yield invalid results](#org6b38b78)
        4.  [PY107 [MEDIUM] `guess_paragraphs` destroys CRLF before normalizing it](#orga25e4d2)
        5.  [PY108 [LOW] `word_iter_with_offsets` has an incorrect public return annotation](#orgde3b7cb)
        6.  [PY109 [LOW] `grapheme_find` documentation contradicts empty-needle behavior](#org7b7bf9b)
        7.  [PY110 [LOW] CLI `casefold` and `strip` stdin/stdout defaults are ineffective](#org1ec399c)
        8.  [PY111 [LOW] Benchmark mode divides by zero for an empty source file](#orgc865aff)
    23. [doc/conf.py](#org249de59)
        1.  [PY112 [LOW] Missing documentation metadata is checked with `assert`](#orge11505a)
    24. [examples/async.py](#org310a49e)
        1.  [PY113 [HIGH] Python 3.10 cannot parse the TaskGroup cancellation example](#org0510f4e)
        2.  [PY114 [LOW] Backup connections lack deterministic exception-safe cleanup](#orgf4448ee)
        3.  [PY115 [LOW] The second timeout reports cumulative rather than local elapsed time](#org606b8f4)
        4.  [PY116 [LOW] Changeset-size estimate is printed as a literal placeholder](#org6e3f0db)
    25. [examples/fts.py](#org24fc41d)
        1.  [PY117 [LOW] Standalone execution depends on a non-shipped fixture and can create an empty database](#org6b6a36e)
        2.  [PY118 [LOW] Empty FTS results leave the first result variable undefined](#org93c5b6f)
        3.  [PY119 [LOW] The custom tokenizer accepts zero and negative block sizes](#org267dc58)
        4.  [PY120 [LOW] The custom tokenizer factory has an incorrect return annotation](#org0bde99b)
    26. [examples/json.py](#orgff9c43f)
        1.  [PY121 [MEDIUM] \`ContextVar.set()\` is used as a context manager on unsupported Python versions](#orgac88bea)
        2.  [PY122 [LOW] Tagged-object decoding is overbroad](#org79b3173)
        3.  [PY123 [LOW] The introductory typing comment falsely says postponed annotations are mandatory](#org845c6e2)
    27. [examples/main.py](#org11cc2f4)
        1.  [PY124 [HIGH] Fixed working-directory database names can alter or delete user data](#org033ddac)
        2.  [PY125 [MEDIUM] Complex-number conversion fails for valid values](#org72ffd58)
        3.  [PY126 [MEDIUM] Symlink-following filesystem traversal can recurse through cycles](#orgde324c8)
        4.  [PY127 [LOW] The “oldest Python files” query orders by file size](#org2f8d143)
        5.  [PY128 [LOW] Incremental blob cleanup is not exception-safe](#org849d8dd)
        6.  [PY129 [LOW] Trace callback puts required mutations inside an assertion](#orgfa8deeb)
    28. [examples/session.py](#orgf0a6ae7)
        1.  [PY130 [MEDIUM] Bundled SQL is resolved from the process working directory](#org35fc1d4)
        2.  [PY131 [LOW] Session availability is reported but Session APIs are still used unconditionally](#org1ad8336)
        3.  [PY132 [LOW] The rebased changeset is computed but never demonstrated or applied](#orgb69cd0c)
    29. [setup.py](#orgd22f093)
        1.  [PY133 [HIGH] Archive extraction escapes the extraction root](#orge5a8f98)
        2.  [PY134 [MEDIUM] Download timeout absence and unpinned HTTP downgrade](#org504498f)
        3.  [PY135 [LOW] Fetch removes the existing SQLite tree before successful replacement](#orgb0d19eb)
        4.  [PY136 [MEDIUM] Extension-test markers are invisible to in-process tests](#orgade0dab)
        5.  [PY137 [MEDIUM] Source-distribution generation overwrites and leaves `setup.apsw`](#org59e0b08)
        6.  [PY138 [LOW] `--missing-checksum-ok` does not apply to the amalgamation](#orgcaa67f1)
        7.  [PY139 [MEDIUM] No-old-names stub customization mutates the source tree](#orgedfde32)
        8.  [PY140 [MEDIUM] ICU helper failures and quoted flags are mishandled](#org4b9948e)
    30. [src/apswtypes.py](#org1880a64)
        1.  [PY141 [MEDIUM] `Self` and `Buffer` imports exceed supported typing targets](#orgff92416)
        2.  [PY142 [MEDIUM] Window `step` and `inverse` protocols require exactly one value](#orgc3a4e4e)
        3.  [PY143 [MEDIUM] `WindowFinal` incorrectly permits SQL arguments](#org62183a3)
        4.  [PY144 [MEDIUM] SQL callback result aliases omit supported result values](#org67d4f97)
        5.  [PY145 [MEDIUM] `AsyncConnectionController.send` excludes regular methods returning awaitables](#org7e181a4)
        6.  [PY146 [LOW] `JSONBTypes` models tuples as exactly one element](#org5c25aa4)
        7.  [PY147 [LOW] `TokenizerResult` includes malformed empty tuple forms](#orge3d8592)
        8.  [PY148 [LOW] `WindowFactory` excludes runtime-supported list results](#org36ed39c)
    31. [tools/aio<sub>bench.py</sub>](#orga83ea91)
        1.  [PY149 [MEDIUM] APSW and aiosqlite use different transaction regimes](#orgded7de8)
        2.  [PY150 [MEDIUM] CPU component columns combine incompatible clocks](#org64fa448)
        3.  [PY151 [MEDIUM] Direct uvloop modes fail on Python 3.10 and 3.11](#org77fc686)
    32. [tools/checksums.py](#org8a69818)
        1.  [PY152 [LOW] Reported checksum mismatches exit successfully](#org5288365)
        2.  [PY153 [LOW] Downloads have no tool-controlled time or resource bounds](#orgdb111e5)
    33. [tools/checkversion.py](#org66e3519)
        1.  [PY154 [LOW] Minimum SQLite version is selected lexicographically](#org0c57aa8)
        2.  [PY155 [LOW] Version normalization permits collisions and relies on removable assertions](#org35b7a2f)
    34. [tools/coverageanalyser.py](#orgec75114)
        1.  [PY156 [LOW] Fault-injection exclusion misses current conditional form](#orgb1bf955)
        2.  [PY157 [LOW] Gcov files are decoded using the ambient locale](#org83fc80b)
        3.  [PY158 [LOW] Gcov exceptional zero-count marker is counted as executed](#org760d0b7)
    35. [tools/code2rst.py, tools/genconstants.py, tools/gendocstrings.py](#orge70c1b7)
        1.  [PY159 [LOW] Shared TOC URL handling requires a trailing slash and lacks a finite timeout](#org99e88fb)
        2.  [PY160 [LOW] Open temporary TOC databases may not reopen on native Windows](#orgc6d0b6e)
    36. [tools/code2rst.py](#org98614d8)
        1.  [PY161 [MEDIUM] Shared docdb updates race and broad exception silently resets data](#orgbda7fd7)
        2.  [PY162 [LOW] Output publication is non-atomic and path-collision unsafe](#org404e26d)
        3.  [PY163 [LOW] Consecutive SQLite-call markers retain only the final RST marker](#org2af0989)
        4.  [PY164 [LOW] Bad CLI arity handling continues and the SQLite-version argument is dead](#org3243f31)
        5.  [PY165 [LOW] Malformed parser state enters interactive pdb](#org512d809)
    37. [tools/genconstants.py](#orgcecf2aa)
        1.  [PY166 [LOW] Reverse mappings select incidental sentinel aliases](#orgef91e85)
        2.  [PY167 [MEDIUM] Live TOC metadata is not tied to the pinned SQLite version](#org67f1b8c)
    38. [tools/gendocstrings.py](#orga5ceddb)
        1.  [PY168 [MEDIUM] Multi-call expansion deletes following documentation](#org7e56e0b)
        2.  [PY169 [LOW] Generated constant docstrings contain malformed RST links](#org5fc00fa)
        3.  [PY170 [MEDIUM] Validation failures can leave source and artifacts partly updated](#org1ae9df2)
        4.  [PY171 [LOW] Unsupported signatures can invoke an interactive debugger](#org54bf3f8)
        5.  [PY172 [MEDIUM] C block replacement uses ambiguous substring and brace heuristics](#org9fff8a2)
    39. [tools/docmissing.py](#orgaa0caca)
        1.  [PY173 [MEDIUM] Public APSW types are excluded without member audit](#org741a74c)
        2.  [PY174 [LOW] Missing documented members abort before the intended diagnostic](#org761be96)
        3.  [PY175 [LOW] Virtual-table protocols are validated only by member count](#org9ebd323)
        4.  [PY176 [LOW] Pragma consistency checks disappear under optimized Python](#org453f427)
    40. [tools/docupdate.py](#org010fb49)
        1.  [PY177 [LOW] Marker replacement can truncate documents](#org5800369)
        2.  [PY178 [LOW] Existing literal-block markers become triple-colon output](#org6508bdd)
        3.  [PY179 [LOW] Colon lookahead can raise IndexError](#org3a73cca)
    41. [tools/example2rst.py](#org78d9bf7)
        1.  [PY180 [MEDIUM] Final example-section output is never emitted](#orgb74021b)
        2.  [PY181 [MEDIUM] Temporary publication is non-atomic, mode-changing, and Windows-sensitive](#orge2e2892)
        3.  [PY182 [MEDIUM] Locale-dependent encoding breaks current example generation](#org7bac297)
        4.  [PY183 [LOW] gen<sub>rst</sub> flushes an accidental global instead of its output stream](#org7a35078)
    42. [tools/fi.py](#org5073201)
        1.  [PY184 [HIGH] Python 3.12+ suppresses every named APSW<sub>FAULT</sub> injection](#org1403f21)
        2.  [PY185 [MEDIUM] Multi-fault exception verification is under-constrained](#orga234e51)
        3.  [PY186 [MEDIUM] Fixed /tmp/fitesting database escapes managed run isolation](#orga6cbe69)
        4.  [PY187 [LOW] Emergency traceback printing raises a secondary TypeError](#orge45cbd2)
        5.  [PY188 [MEDIUM] Leak detection can block or continue instead of failing](#org8c5f634)
        6.  [PY189 [LOW] Unknown faults enter an interactive debugger](#orgc4cb7e7)
        7.  [PY190 [LOW] Missing example name raises NameError while building its diagnostic](#org573361b)
    43. [tools/gencompilecommands.py](#orgd481463)
        1.  [PY191 [MEDIUM] Compilation entries pair file values with another translation unit](#org986dd69)
        2.  [PY192 [MEDIUM] Compound compiler commands are emitted as one executable argument](#orgb037a52)
        3.  [PY193 [MEDIUM] Standard Windows Python configurations are unsupported](#orge1a8415)
    44. [tools/genfaultinject.py](#org54351de)
        1.  [PY194 [MEDIUM] Fault inventory omits actively used faultable Python APIs](#org0177fdf)
        2.  [PY195 [LOW] Regeneration bypasses the configured Python](#orgde83027)
        3.  [PY196 [LOW] nm can interpret the inspected filename as an option](#org8cfde4c)
        4.  [PY197 [LOW] Generated-header publication is non-atomic](#org3fcb126)
    45. [tools/gensqlitedebug.py](#org1491b3a)
        1.  [PY198 [HIGH] Broad session patterns suppress required mutex wrappers](#org986a6b6)
        2.  [PY199 [LOW] Generated macros evaluate the checked argument twice](#org555695b)
        3.  [PY200 [LOW] git grep errors are treated as ordinary absence](#orgfa0b56f)
    46. [tools/genstrings.py](#orgfbf29cd)
        1.  [PY201 [LOW] Failed generation can leave a partial target considered current](#orgd144dd8)
    47. [tools/import<sub>enron.py</sub>](#org217770c)
        1.  [PY202 [MEDIUM] Multipart message headers are taken from the selected MIME leaf](#orgd35cadb)
        2.  [PY203 [MEDIUM] MIME bodies are not transfer-decoded and body selection can choose an attachment](#org9ed2b86)
        3.  [PY204 [MEDIUM] Base64 cleanup regex deletes legitimate text and leaves quoted blocks incomplete](#org0b5934b)
        4.  [PY205 [MEDIUM] Raw SQL table-name interpolation affects both importers](#org68228ce)
        5.  [PY206 [LOW] Progress output has a duplicate terminal percentage and unsupported fixed denominator](#orgeeecb15)
        6.  [PY207 [LOW] Automatic VACUUM can obscure completed import work](#orgdcbe408)
    48. [tools/import<sub>html.py</sub>](#org68a7d0b)
        1.  [PY208 [MEDIUM] Strict UTF-8 decoding rejects valid HTML in other encodings](#orgbab504a)
        2.  [PY209 [LOW] HTML-suffixed ZIP directory entries are imported as documents](#orga94029e)
    49. [tools/json<sub>bench.py</sub>](#org041c028)
        1.  [PY210 [HIGH] Generated JSON is decoded only for a discarded validation result](#org4441b5d)
        2.  [PY211 [HIGH] The row labelled stdlib json.loads measures json.dumps](#orgfdc83af)
        3.  [PY212 [LOW] Claimed JSONB warm-up does not call either function](#org4d90c88)
        4.  [PY213 [LOW] Help and argparse diagnostics require benchmark dependencies first](#org6cac451)
        5.  [PY214 [LOW] Source vocabulary is read using the locale default encoding](#org8378334)
    50. [tools/jsonb<sub>proportion.py</sub>](#orgb1c8bef)
        1.  [PY215 [LOW] Monte Carlo estimates omit uncertainty information](#org833fb12)
        2.  [PY216 [LOW] Configuring a lower bound above one raises KeyError](#org95c1e18)
    51. [tools/megatest.py](#org69b3584)
        1.  [PY217 [MEDIUM] System-Python jobs misrepresent configuration and GIL coverage](#orgee032ab)
        2.  [PY218 [HIGH] Installed-wheel validation can import the in-tree extension](#orgcff315f)
        3.  [PY219 [HIGH] Worker failures are reported but do not fail megatest](#orgca921ad)
        4.  [PY220 [MEDIUM] Startup cleanup can delete host user-site packages and sibling-tree data](#org0129574)
        5.  [PY221 [MEDIUM] Shell interpolation permits command injection and unsafe path handling](#orgbb17a66)
        6.  [PY222 [MEDIUM] Free-threaded jobs suppress broad warning and bytes strictness](#orgebcd386)
    52. [tools/names.py](#org7d31076)
        1.  [PY223 [HIGH] Legacy-name test failures do not affect process status](#orgb71f4b5)
        2.  [PY224 [LOW] Subcommands depend inconsistently on the caller's working directory](#org4542838)
        3.  [PY225 [LOW] Locale-dependent decoding can prevent legacy-name test execution](#orgaa9185e)
    53. [tools/recipe.py](#org1ccb4ea)
        1.  [PY226 [MEDIUM] Recipe archive decoding depends on the process locale](#org7008972)
    54. [tools/sqlite<sub>compile</sub><sub>info.py</sub>](#orgdf7150c)
        1.  [PY227 [MEDIUM] Compile-option values containing = are parsed incorrectly](#org6968eac)
        2.  [PY228 [MEDIUM] Decimal coercion hides distinct C compile-time values](#orgdfd6c33)
        3.  [PY229 [LOW] Dynamically loaded SQLite reporting is Linux-specific and can be misleading](#org636a0b4)
        4.  [PY230 [LOW] Standard-library sqlite3 is labeled pysqlite3](#orgeb1791c)
    55. [tools/sqliteliboptions.py](#orgab9216e)
        1.  [PY231 [MEDIUM] Valueless SQLite options become empty macro definitions](#org89eaf27)
        2.  [PY232 [LOW] Diagnostic options are serialized as source definitions](#orgce86d8e)
        3.  [PY233 [LOW] The checked path can differ from the library loaded](#orgc5ee837)
    56. [tools/types2rst.py](#org8d81db9)
        1.  [PY234 [LOW] Blind identifier substitution can corrupt existing RST markup](#org7fbb645)
        2.  [PY235 [LOW] Global asterisk replacement can change literal values](#org1e19d83)
        3.  [PY236 [LOW] Output encoding depends on the process locale](#org103948d)
        4.  [PY237 [LOW] Destination is truncated before generation succeeds](#org874bdc6)
        5.  [PY238 [LOW] Python data objects use class roles](#orgc4e1809)
    57. [tools/ucdnames.py](#org267fb32)
        1.  [PY239 [MEDIUM] Numbered Unicode-name suffixes are not validated](#orge6d87f5)
        2.  [PY240 [MEDIUM] A Unicode-17 Tangut range is emitted for older source data](#org62ac65c)
        3.  [PY241 [LOW] Standalone generation decodes official UCD input using the process locale](#org2d0a6e8)
    58. [tools/ucdprops2code.py](#org6ae7c13)
        1.  [PY242 [MEDIUM] Strip mappings retain decomposable intermediates](#org6418c6d)
        2.  [PY243 [LOW] Output publication can truncate generated C before generation succeeds](#org59a5355)
        3.  [PY244 [MEDIUM] Unicode inputs are not bound to one immutable release](#org474e423)
        4.  [PY245 [MEDIUM] Optimized Python disables external-UCD integrity checks](#orgaada512)
        5.  [PY246 [MEDIUM] Marker-based update can target or truncate the wrong Python source](#orgce3ebd5)
        6.  [PY247 [LOW] Invalid `--table-limit` values are accepted until late generation](#org92f29cc)
        7.  [PY248 [LOW] Repository Python-source I/O depends on the locale encoding](#org5961240)
    59. [tools/vend.py](#orga70d023)
        1.  [PY249 [MEDIUM] Bootstrap and prerequisite failures can report successful builds](#orgf91341a)
        2.  [PY250 [MEDIUM] Extension-only builds compile and publish executable-only prerequisites](#orgb4a7f47)
        3.  [PY251 [MEDIUM] RST generation can leave a partial target that Make treats as current](#org3c178cb)
    60. [tools/vtbench.py](#orgc50872d)
        1.  [PY252 [MEDIUM] Repr variants use independently generated data and confound comparisons](#orgc24a659)
        2.  [PY253 [MEDIUM] Fixed ordering and absent warm-up may bias benchmark variants](#org8f4aa40)
4.  [Rejected-candidate and deduplication method](#org3bd71e0)
5.  [Coverage index](#orgb7effe0)



<a id="org8a3471d"></a>

# Scope and method

This is a read-only source audit of every Python file tracked at commit
`69e0f6be7980c1f641f402312a056b9de4d06ba5`.  One primary audit and one
independent validation pass were performed for each of the 59 files.  After
rebasing, every finding was rechecked and all file/line references were
remapped to that tree.  Related C implementations, generated artifacts, tests,
documentation, build recipes, and call sites were inspected only to validate
Python findings and identify canonical fix locations.  No C or Python source
remediation was performed.

The checkout did not contain a built APSW extension.  Pure-Python behavior was
reproduced where practical; APSW-, platform-, schedule-, benchmark-, and
fault-injection-dependent manifestations remain explicitly marked for runtime
validation.  Findings concerning generated output identify the generator or
canonical source rather than recommending hand edits to generated files.

Severity means:

-   **High**: credible data loss, arbitrary archive-path write, native-code loading
    exposure, broad false-green release/test results, or a major benchmark result
    that measures the wrong operation.
-   **Medium**: definite product/API correctness, concurrency, portability,
    generation-integrity, test false-confidence, or material benchmark defect.
-   **Low**: narrow edge case, diagnostics, annotation/documentation mismatch,
    conditional portability issue, or limited test/generator robustness defect.

Status means:

-   `open`: source evidence establishes the defect and its actionable core.
-   `runtime validation`: a retained finding whose manifestation or material
    impact depends on platform, scheduling, optional features, generated builds,
    external data, or benchmark execution.
-   `benchmark`: a performance recommendation requiring measurement before a
    source change is justified.

Validator labels such as ACCEPT and QUALIFY are intentionally not report
statuses.  Qualified findings below retain only the independently supported
core and state the relevant limitation in their evidence or status.


<a id="org2dee121"></a>

# Executive summary

The most urgent Python-side areas are destructive file/database handling,
archive extraction containment, false-success test/release tooling, fault
injection on Python 3.12+, and benchmark rows that execute or compare the wrong
work.  Product-facing medium-severity clusters include async exception and
cancellation handling, SQL/identifier construction, FTS query/tokenizer
correctness, generator publication, and public typing contracts.

Test false-confidence is substantial: expected-exception helpers permit normal
return, copied table-comparison helpers compare the left schema twice, optional
feature guards disable coverage, assertions remove checks under `-O`, and
subprocess/fork tests accept missing expected behavior.  Generator reliability
is another recurring theme: several tools mutate shared or canonical files
before validation, publish via direct truncation, or consume mutable network
metadata without a finite timeout or version binding.

The report contains 253 findings: 14 High, 113 Medium, and 126 Low.
There are 244 open findings, eight awaiting runtime/platform validation, and
one benchmark-methodology finding awaiting measurement.

One tracked file, `apsw/__main__.py`, has no actionable finding.  Every other
tracked Python file has at least one retained finding.  Related roots were
consolidated where appropriate, notably table-comparison helpers, fixed example
database names, importer table-name interpolation, shared SQLite TOC handling,
and generated-stub compatibility.  The process-global string/subinterpreter
issue in `tools/genstrings.py` is omitted because it is already C finding C002.


<a id="org334ae96"></a>

# Findings


<a id="orgd6732e4"></a>

## apsw/\_<sub>main</sub>\_<sub>.py</sub>

No actionable findings. The module is a correct minimal adapter to
`apsw.shell.main()` and supports the documented `python -m apsw` entry point.


<a id="org741dcf7"></a>

## apsw/aio.py


<a id="orga72d26e"></a>

### PY001 [MEDIUM] Prefetched exceptions are reconstructed incorrectly on Python 3.10–3.11

Evidence: The pre-3.12 path reconstructs a captured exception with \`exc[0](exc[1]).with<sub>traceback</sub>(exc[2])\`. For tuples whose value is already a normalized exception instance, \`exc[1]\` is that instance; constructing a new instance changes its arguments, loses instance state and, on Python 3.11, notes, and can invoke a constructor that raises a different exception. The C prefetch path captures a legacy \`(type, value, traceback)\` tuple, but does not explicitly normalize it before dispatch, so the Python helper must not assume a normalized instance without ensuring it.

Impact: A callback exception raised after prefetched rows may be replaced, corrupted, or turned into \`TypeError\` on Python 3.10–3.11. Existing coverage only verifies that a simple \`ZeroDivisionError\` is catchable, not preservation of exception identity or state.

-   Direction: Normalize exceptions at the capture boundary and re-raise the normalized exception value with its traceback; otherwise handle the full legacy tuple semantics rather than reconstructing blindly.
-   Test: On Python 3.10 and 3.11, prefetch rows before a UDF or row trace raises a custom exception with restrictive constructor arguments and custom attributes. Assert the eventual exception retains its class, arguments, state, and traceback; on Python 3.11 also assert that notes are preserved.


<a id="org6f7859a"></a>

### PY002 [MEDIUM] Cancellation can orphan an asyncio task during callback handoff

Evidence: Each asyncio-version branch creates a task, publishes \`tracker.cancel<sub>async</sub><sub>cb</sub> = task.cancel\`, then returns immediately if cancellation was observed. Cancellation between the worker’s initial check and publication sets \`is<sub>cancelled</sub>\` while no callback is installed. The helper can then abandon the newly created task, while the worker unconditionally closes the coroutine that task owns.

Impact: A callback can run or fail after the database request was canceled, producing side effects after cancellation, unhandled task exceptions, or invalid task/coroutine lifecycle handling. The current cancellation test waits until the callback has started, missing this handoff interval.

-   Direction: After publishing the cancellation callback, if cancellation is already set, cancel and await/drain the task before returning; preserve the second cancellation check.
-   Test: Deterministically cancel between task creation and assignment of \`cancel<sub>async</sub><sub>cb</sub>\`. Assert the task reaches a terminal state, no callback work occurs after cancellation, no unobserved task exception is reported, and the worker never closes a coroutine owned by an active task. Exercise all Python-version branches.


<a id="org0fc43fe"></a>

### PY003 [MEDIUM] Auto controller detection rejects valid non-canonical dependency versions

Evidence: AnyIO and Trio detection parse package versions by splitting on periods and converting every component with \`int()\`. Valid PEP 440 versions such as \`4.11.0+vendor\`, \`4.11.0.post1\`, or \`0.31.0+local.1\` raise \`ValueError\`, which broad \`except:\` handlers suppress. A qualifying AnyIO build can consequently fall back to an underlying controller; a malformed Trio version can instead end in generic environment-detection failure.

Impact: Supported vendor, local, or post-release builds may receive a controller with different cancellation and timeout behavior, or an unhelpful generic detection error. The broad catches also obscure unexpected metadata failures, though those are a related exception-scoping concern rather than proof that every malformed version selects the wrong controller.

-   Direction: Use PEP 440-aware comparison or feature detection, separate loop detection from metadata/version handling, and catch only expected no-active-backend exceptions without swallowing \`BaseException\`.
-   Test: Mock package metadata with local, post-release, prerelease, and below-minimum versions. Under AnyIO and Trio, assert qualifying versions select the intended controller and unexpected metadata exceptions remain visible.


<a id="org8ae63bf"></a>

### PY004 [LOW] contextvar<sub>set</sub> has an incorrect pre-Python-3.14 return annotation

Evidence: On Python 3.10–3.13, \`contextvar<sub>set</sub>()\` is annotated as returning \`contextvars.Token[T]\`, but actually returns the context-manager wrapper generated by \`@contextlib.contextmanager\`; the token is yielded by entering that manager. The Python 3.14 branch differs because \`ContextVar.set()\` itself returns a context-manager-capable token.

Impact: Type checkers can reject the documented \`with contextvar<sub>set</sub>(&hellip;) as token:\` usage, while code trusting the declared token return type can access token attributes on a wrapper and fail at runtime.

-   Direction: Annotate the pre-3.14 return as an \`AbstractContextManager[contextvars.Token[T]]\` (or a common context-manager protocol) and annotate the inner generator consistently.
-   Test: Add a mypy or pyright fixture targeting Python 3.10/3.11 that verifies the call expression is a context manager and the entered value is \`contextvars.Token[T]\`.


<a id="org39144c3"></a>

## apsw/bestpractice.py


<a id="orgb975fb1"></a>

### PY005 [MEDIUM] Foreign-key helper can silently leave constraints disabled in a transaction

Evidence: \`connection<sub>enable</sub><sub>foreign</sub><sub>keys</sub>()\` promises to enable foreign keys but uses \`PRAGMA foreign<sub>keys</sub>=ON\`. SQLite makes that pragma a no-op while a transaction or savepoint is active. Individual invocation of best-practice helpers is documented and supported, so this is reachable outside the usual connection-hook timing. APSW exposes \`Connection.config(SQLITE<sub>DBCONFIG</sub><sub>ENABLE</sub><sub>FKEY</sub>, 1)\`, which reports the effective setting.

Impact: A caller can invoke the helper during a transaction, believe constraints are active, and insert rows violating declared foreign-key relationships. The impact is loss of referential-integrity enforcement, not physical database-file corruption.

-   Direction: Enable foreign keys through \`Connection.config(apsw.SQLITE<sub>DBCONFIG</sub><sub>ENABLE</sub><sub>FKEY</sub>, 1)\` and verify the reported effective state, or explicitly reject calls made while autocommit is off.
-   Test: Begin a transaction or savepoint with foreign keys initially disabled, invoke the helper, then assert enforcement is active and an orphan insert raises \`apsw.ConstraintError\`.


<a id="orgb6ae993"></a>

### PY006 [LOW] WAL helper discards the effective journal mode

Evidence: \`connection<sub>wal</sub>()\` says it turns on WAL but discards the result of \`PRAGMA journal<sub>mode</sub>=wal\`, even though that result is the mode actually in effect. SQLite can retain the prior mode when WAL cannot be enabled or mode changes are attempted during a transaction; in-memory and transient databases intentionally remain non-WAL.

Impact: For persistent databases, callers can assume WAL concurrency behavior while remaining in another journal mode. Silent non-WAL operation for \`:memory:\` and empty-filename transient databases is expected compatibility behavior, so this is a low-severity status/contract issue rather than an unconditional failure.

-   Direction: Capture the resulting mode. Document transient and in-memory behavior, and for persistent writable databases either return explicit status or raise a clear error when WAL was not enabled.
-   Test: Cover a persistent writable file, \`:memory:\`, an empty-filename transient database, and invocation during an active transaction; assert the documented status or failure behavior in each case.


<a id="org1900c8f"></a>

### PY007 [LOW] connection<sub>optimize</sub> understates immediate ANALYZE and write behavior

Evidence: The helper is documented as enabling planner tracking for optimization “later,” but immediately executes \`PRAGMA optimize=0x10002\`. The \`0x10000\` flag considers all tables, including unqueried ones, and SQLite may immediately run bounded \`ANALYZE\` work that writes planner statistics. The helper is included in the universal recommended policy, so it can run during every connection open; only \`ReadOnlyError\` is suppressed.

Impact: Opening a writable database can unexpectedly write \`sqlite<sub>stat1</sub>\`, acquire a write lock, wait for a busy timeout, or fail with \`BusyError\`. The operation is SQLite-recommended for long-lived connections, so the issue is inappropriate universal policy and understated documentation, not that optimization itself is invalid.

-   Direction: Document the immediate bounded ANALYZE/write behavior and distinguish long-lived connection startup (\`0x10002\`) from periodic or close-time optimization suitable for short-lived connections.
-   Test: Use an indexed, unanalyzed table to establish whether statistics are created; hold a write transaction from another connection and assert the intended busy, skip, or retry policy. Cover the chosen long-lived and short-lived policies separately.


<a id="org0a006e5"></a>

## apsw/ext.py


<a id="org248543e"></a>

### PY008 [MEDIUM] Default virtual-table rowids are not reliably unique

Evidence: The default rowid is \`id(self.current<sub>row</sub>)\`. Advancing replaces and releases the prior row, allowing object addresses to be reused; a callable yielding the same mutable row object gives every logical row the same rowid deterministically. SQLite requires every virtual-table row to have a unique 64-bit rowid.

Impact: \`SELECT rowid\`, rowid predicates, joins, grouping, and application logic can conflate distinct logical rows. The virtual table is read-only, so update/delete corruption is not implicated.

-   Direction: Maintain a cursor-owned integer sequence, reset it in \`Filter()\`, advance it for each row, and use it when no explicit primary key is supplied. Require an explicit key where stable identity across nondeterministic scans is required.
-   Test: Register generators yielding newly allocated rows and repeatedly yielding the same row object. Assert \`count(\*)\` equals \`count(DISTINCT rowid)\` and retain explicit-primary-key coverage.


<a id="orgb5bed5c"></a>

### PY009 [MEDIUM] log<sub>sqlite</sub> permanently lowers the configured severity after notices

Evidence: The callback declares \`nonlocal level\` and changes that shared closure variable for warning, notice, and schema messages. Every later message uses the mutated value, so an ordinary SQLite error following a notice can be logged at warning, or after a schema message at info.

Impact: Later serious SQLite errors can bypass error-level logging handlers, alerting, metrics, and filters for the callback lifetime. This handler is installed by the recommended best-practice configuration.

-   Direction: Keep the configured base level immutable and derive a callback-local \`message<sub>level</sub>\` for each message.
-   Test: Invoke the registered callback sequentially with warning or schema messages followed by errors; assert only the initial message is downgraded and subsequent errors remain error-level.


<a id="org2fcec4f"></a>

### PY010 [MEDIUM] dbinfo mixes attached-schema bytes with main-schema metadata

Evidence: \`dbinfo(db, schema)\` reads database and sidecar bytes for the requested schema, but reports \`db.filename\`, \`db.filename<sub>wal</sub>\`, \`db.filename<sub>journal</sub>\`, and \`db.pragma("journal<sub>mode</sub>")\`, which are main-schema-only properties. An attached database with a different path or journal mode can therefore have its bytes parsed and labeled using main-database metadata.

Impact: Attached-schema diagnostic output can be internally inconsistent, select the wrong WAL-versus-rollback sidecar parser, and report main-database paths. This is a public diagnostic/API correctness defect rather than a modifying operation.

-   Direction: Use \`db.db<sub>filename</sub>(schema)\` and \`db.pragma("journal<sub>mode</sub>", schema=schema)\`; add schema-aware wrappers for WAL and rollback-journal filename lookup rather than constructing VFS-unsafe paths.
-   Test: Attach a database with a distinct path and journal mode, preserve its applicable sidecar, then assert \`dbinfo(&hellip;, "attached")\` reports attached filenames, mode, sidecar type, and parsed fields.


<a id="orgb09dd1a"></a>

### PY011 [LOW] DataClassRowFactory rename mode can still generate duplicate field names

Evidence: The rename loop checks accepted original names but unconditionally emits positional fallback names such as \`"<sub>1</sub>"\` without checking collisions. Columns named \`"<sub>1</sub>"\` and \`"bad name"\` therefore both become \`"<sub>1</sub>"\`, causing \`dataclasses.make<sub>dataclass</sub>()\` to reject the duplicate.

Impact: Row conversion fails for valid SQL result column names despite \`rename=True\` promising to handle invalid and duplicate names. The issue is limited to edge-case aliases and has no data-integrity or security consequence.

-   Direction: Track every emitted name and generate fallback names until an unused valid name is found; reserve valid original names as needed to avoid later collisions.
-   Test: Query columns named \`"<sub>1</sub>"\` and \`"bad name"\`, plus duplicate aliases that collide with generated fallbacks. Assert construction succeeds, fields are unique and valid, and values remain independently accessible.


<a id="org7c350bd"></a>

### PY012 [LOW] Virtual-table declaration identifiers are not safely escaped

Evidence: Virtual-table declarations interpolate names as bracket-quoted identifiers, \`[{name}]\`. SQLite bracket quoting cannot robustly represent embedded \`]\`; dictionary-backed column names may contain arbitrary such characters. A name like \`a]b\` produces malformed declaration SQL, while crafted text can render unintended declaration columns.

Impact: Module registration can fail or expose a declaration whose column count/schema no longer matches the module’s callbacks, causing indexing errors or incorrect values. No arbitrary SQL execution was established; the scope is virtual-table declaration construction.

-   Direction: Quote every regular and hidden identifier with SQL-standard double quotes, escaping embedded double quotes by doubling them.
-   Test: Register dictionary-backed modules with names containing \`]\`, quotes, empty strings, spaces, keywords, and non-ASCII text. Inspect \`table<sub>xinfo</sub>\` and select each exact identifier, asserting no extra columns appear.


<a id="orgb515d7e"></a>

### PY013 [LOW] Nested virtual-table prefixes assign shadow tables to the wrong group

Evidence: Shadow-table grouping is documented as longest-name-first, but candidate virtual tables are sorted by increasing name length. For \`foo\` and \`foo<sub>bar</sub>\`, a \`foo<sub>bar</sub><sub>\*</sub>\` shadow table matches both prefixes and is assigned to \`foo\` first.

Impact: \`analyze<sub>pages</sub>(scope=1)\` reports incorrect virtual-table ownership, page and payload totals, cell counts, and \`PageUsage.tables\` membership; downstream SVG output is correspondingly wrong. Database contents are not modified.

-   Direction: Sort candidate virtual-table names by descending length before prefix matching, or use an authoritative shadow-table ownership mechanism if available.
-   Test: Create overlapping-prefix FTS5 tables such as \`a\` and \`a<sub>b</sub>\`; assert each \`a<sub>b</sub><sub>\*</sub>\` shadow table belongs only to the \`a<sub>b</sub>\` group.


<a id="orge426f36"></a>

### PY014 [MEDIUM] query<sub>info</sub> actions mode bypasses an existing authorizer

Evidence: To collect actions, \`query<sub>info</sub>()\` saves and replaces \`db.authorizer\` with a callback that records actions and always returns \`SQLITE<sub>OK</sub>\`. It never invokes the original callback while preparing the query, then restores it afterward. SQL normally denied or ignored by connection policy can therefore be prepared and inspected through \`query<sub>info</sub>(&hellip;, actions=True)\`.

Impact: Connection authorization policy is silently bypassed during analysis, allowing metadata about denied objects, columns, functions, or operations to be returned and permitting preparation-time behavior the policy intended to block. Execution tracing aborts query execution, so direct data extraction, DML, or DDL through this helper was not established.

-   Direction: Compose the collection callback with the original authorizer: record the action, invoke the original callback when present, and return its exact result. Make any diagnostic policy bypass explicit opt-in.
-   Test: Install an authorizer denying reads of a protected table and assert ordinary preparation and \`query<sub>info</sub>(&hellip;, actions=True)\` both raise \`apsw.AuthError\`; also verify forwarding of \`SQLITE<sub>IGNORE</sub>\`, action collection, and restoration after success and failure.


<a id="orgfda8510"></a>

### PY015 [LOW] DataClassRowFactory mutates caller-owned namespace mappings

Evidence: The factory shallow-copies \`dataclass<sub>kwargs</sub>\` but retains its nested \`namespace\` mapping, then writes \`\_<sub>description</sub>\_\_\` into that caller-owned mapping before calling \`make<sub>dataclass</sub>()\`. The write can overwrite an existing description. The behavior also needlessly fails for \`namespace=None\` or read-only mappings that could otherwise be accepted.

Impact: Callers receive unexpected configuration mutation; reused shared namespaces expose the most recently processed description and can cause cross-description contamination.

-   Direction: Copy the nested namespace before adding APSW-generated metadata, handle \`None\` as an empty namespace, and avoid mutating mappings supplied by callers.
-   Test: Supply a namespace containing a sentinel \`\_<sub>description</sub>\_\_\`, build dataclasses for multiple descriptions, and assert the input mapping is unchanged. Also cover \`None\` and read-only namespace mappings.


<a id="orgd86b42e"></a>

## apsw/fts5.py


<a id="org457d07e"></a>

### PY016 [MEDIUM] FTS5 option keys are parsed case-sensitively

SQLite accepts and preserves mixed- or uppercase FTS5 option keys such as `TOKENIZE`, `Prefix`, and `DETAIL`, but the parser compares option keys to lowercase literals and raises `Unknown option`. Consequently, `apsw.fts5.Table` cannot wrap otherwise valid FTS5 tables with non-lowercase option names. The qualification is that the claimed failure for lowercase `CREATE VIRTUAL TABLE` keywords is not established on the public `Table.structure` path: SQLite canonicalizes those leading schema keywords.

-   Direction: Normalize only option keys before dispatch, for example with `key = vals["columns"].pop().lower()`, while preserving column names and option values.

-   Test: Create FTS5 tables using `TOKENIZE`, mixed-case `Prefix`, and `DETAIL` options; assert that `apsw.fts5.Table(...).structure` succeeds.


<a id="org11feefa"></a>

### PY017 [MEDIUM] NGramTokenizer omits the final fallback query n-gram

When query text is too short for the largest configured n-gram but long enough for a smaller configured size, the fallback loop uses `range(0, len(stream) - largest)`. A window of size *n* over *L* graphemes requires `L - n + 1` starts, so the final valid suffix window is omitted. For n-grams `3,7` and input `abcde`, the tokenizer emits `abc` and `bcd` but misses `cde`. This produces incomplete query terms and can miss suffix-related matches.

-   Direction: Include the final start index: `range(len(grapheme_cluster_stream) - largest + 1)`.

-   Test: With `ngrams=3,7`, assert query tokenization of `abc`, `abcd`, and `abcde` yields respectively `[abc]`, `[abc, bcd]`, and `[abc, bcd, cde]`, including correct offsets.


<a id="orgd49bfe5"></a>

### PY018 [MEDIUM] JSONTokenizer rejects valid escaped slashes

JSON permits `\/`, but `_json_backslash_mapping` lacks a mapping for `"/"`. Non-Unicode escapes are looked up without validation, so valid JSON such as `{"url":"https:\/\/example.com"}` raises `KeyError("/")`. Valid JSON containing escaped URLs therefore cannot be indexed by `JSONTokenizer`.

-   Direction: Add `"/": "/"` to `_json_backslash_mapping`.

-   Test: Exercise both `extract_json()` and registered `JSONTokenizer` with one and multiple escaped slashes; verify decoded text and source-offset mapping.


<a id="orgb7dbbed"></a>

### PY019 [LOW] text<sub>for</sub><sub>token</sub> limits occurrences rather than documents

The documented `doc_limit` is a maximum number of documents to examine, but the query selects from `fts5vocab(instance)` and applies SQL `LIMIT` directly to instance rows. Each row is a token occurrence, so repeated occurrences in the newest document can consume the entire limit without examining another document. This biases source spelling/casing selection and can reduce `query_suggest()` representativeness. Severity is Low because this is a heuristic-quality defect rather than corruption.

-   Direction: Select the newest distinct document IDs up to `doc_limit` first, then retrieve relevant instances for those documents.

-   Test: Insert a newest document with many occurrences of a token and an older document with a different spelling. With `doc_limit=2`, assert that both distinct rowids are examined.


<a id="org89af8a7"></a>

### PY020 [MEDIUM] CLI custom tokenizer registration uses a literal name

The CLI parses `--register name=module.callable` into `name` and a factory, but registers the factory under the literal string `"name"`. A subsequent positional lookup uses the caller-supplied tokenizer name, so `--register custom`&hellip; custom= fails; multiple registrations also collide on the same literal name.

-   Direction: Pass the parsed variable: `con.register_fts5_tokenizer(name, obj)`. Reject empty names and report malformed registrations as argparse-facing errors.

-   Test: Invoke the module with `--register custom=<factory> custom ...` and verify successful lookup and tokenization; register two distinct names in one invocation.


<a id="org9f33f45"></a>

### PY021 [MEDIUM] Tokenizer wrapper factories share mutable callback state

`SynonymTokenizer`, `StopWordsTokenizer`, and `TransformTokenizer` overwrite a `nonlocal` callback while instantiating tokenizer instances. Later instances mutate factory-wide state, prior instances observe the new callback, and callback options disappear from the advertised specification after first use. Concurrent initialization can additionally bind an instance to whichever callback was assigned last. These are one shared-closure-mutation root, not three separate findings.

-   Direction: Keep the factory callback immutable and derive an invocation-local callback, such as `instance_get = options.get("get", get)`; close each tokenizer over its instance-local callback. Apply the same pattern to `test` and `transform`.

-   Test: For each wrapper, instantiate a callback-configurable factory twice, first with callback A and then callback B; verify both returned tokenizers retain their independent behavior. Repeat in reverse order and add coordinated concurrent initialization coverage where supported.


<a id="org781026b"></a>

### PY022 [LOW] more<sub>like</sub> constructs an invalid empty OR query

`key_tokens()` can legitimately return no tokens—for empty IDs, `token_limit=0, a one-row corpus, or statistically unqualified tokens. =more_like()` nevertheless creates an OR dictionary with an empty `queries` list, which `fts5query.from_dict()` rejects. Valid no-similarity requests therefore expose an internal AST-validation error instead of yielding no results.

-   Direction: Return an empty iterator before constructing the query when no tokens were selected. Validate zero or negative `token_limit` separately only if those values are intended to be invalid public arguments.

-   Test: Assert empty results for `more_like([])`, `more_like([rowid], token_limit=0)`, a one-row table, and rows whose tokens do not qualify.


<a id="org6296976"></a>

### PY023 [LOW] Cache refresh uses a new lock for every attempt

Each refresh enters `with threading.Lock():` with a freshly allocated lock, so competing callers cannot synchronize and may duplicate expensive vocabulary/statistics refresh work. Cache assignments can transiently race. The qualification is that a persistent stale-cache return is not established: after publishing, the surrounding loop rechecks the change cookie and normally refreshes again before returning if its result is stale. Observable cost and scheduling depend on runtime interleaving.

-   Direction: Create one per-`Table` lock in `__init__` and use it around refresh and cookie recheck. Retain post-acquisition and post-refresh cookie validation; use an appropriate SQLite read transaction if a multi-statement snapshot is required.

-   Test: Coordinate two permitted cross-thread callers entering cache refresh for the same cookie and assert that one refresh supplies both after the shared lock is introduced. Include an intervening write and assert final cookie validation prevents returning already-stale cache data.


<a id="org4c5e7ad"></a>

## apsw/fts5aux.py


<a id="orgb4a1108"></a>

### PY024 [MEDIUM] BM25 caches first-invocation weights for an entire MATCH query

`_Bm25GetData()` stores both query-wide statistics and current callback weights in FTS5 auxiliary data. Later invocations use the cached object and ignore their own SQL arguments. Thus row-dependent expressions and multiple calls with different argument lists use the first row’s weights. `subsequence()` and `position_rank()` inherit the stale weights for both BM25 and their boost calculations. SQLite’s BM25 obtains each current weight per invocation.

-   Direction: Cache only query-wide data—phrase count, average document length, and IDF—in auxiliary data. Normalize and use current `args` separately on every callback invocation, including boost calculations.

-   Test: Compare Python and built-in BM25 with a row-dependent weight expression, and select `pybm25(ft, 1.0)` alongside `pybm25(ft, 100.0)` in one statement. Add corresponding row-varying tests for `subsequence()` and `position_rank()`.


<a id="org6d016f8"></a>

### PY025 [LOW] BM25 does not match SQLite conversion of SQL weight values

The Python BM25 translation retains raw callback values and later performs Python arithmetic, so `NULL`, numeric text, nonnumeric text, and buffers can raise incidental `TypeError`. SQLite BM25 applies `sqlite3_value_double()` and accepts these values, including numeric text. This is a confirmed SQLite-parity and diagnostics gap affecting BM25, `subsequence()`, and `position_rank()`. The qualification is that the callback transport type does not itself promise every SQLite value type is a meaningful supported weight.

-   Direction: Prefer SQLite-compatible numeric conversion for every current invocation, implemented alongside the per-invocation-weight fix. Alternatively, explicitly adopt and document strict numeric-only validation with stable, argument-specific errors.

-   Test: Compare Python and built-in BM25 for `NULL`, `'2'`, nonnumeric text, and `zeroblob(2)`. If strict validation is selected, assert stable public errors instead of arithmetic exceptions.


<a id="org4a0e1a9"></a>

## apsw/fts5query.py


<a id="org126e774"></a>

### PY026 [HIGH] QueryTokens encoding is not reversible

The encoding uses unescaped `$!ZeRo`, `|`, and `>` delimiters, and suppresses separators for leading empty positions. Decoding blindly splits those delimiters. Empty token lists, leading empty tokens, delimiter-bearing tokens, the literal sentinel, and colocated delimiter-bearing tokens therefore do not round trip. Custom FTS5 tokenizers may emit such tokens, so callers requesting exact pre-tokenized terms can silently execute a materially different search.

-   Direction: Replace the format with a versioned unambiguous representation, preferably length-prefixed UTF-8 or structured binary data. Preserve decoding compatibility for previously persisted values if needed.

-   Test: Round trip `[]`, `[""]`, `["", "a"]`, `["a|b"]`, `["a>b"]`, `["$!ZeRo"]`, `["\0"]`, and `[["a>b", "c|d"]]` through both `QueryTokens` and `QueryTokensTokenizer`.


<a id="org3002e43"></a>

### PY027 [HIGH] Right-nested NOT is serialized with changed semantics

Serialization parenthesizes solely by precedence and treats both children of `NOT` identically. A right-nested NOT is emitted without parentheses: `a NOT b NOT c`. SQLite parses NOT left-associatively, so this reparses as `(a NOT b) NOT c` rather than `a NOT (b NOT c)`. Parsed-to-string conversion can therefore silently broaden or narrow searches.

-   Direction: Make parenthesization associativity- and child-position-aware; at minimum, always parenthesize a `NOT` right child that is itself `NOT`.

-   Test: Assert that serializing and reparsing `NOT(PHRASE("a"), NOT(PHRASE("b"), PHRASE("c")))` preserves the AST, and execute both forms against rows that distinguish their result sets.


<a id="orgf569ee0"></a>

### PY028 [MEDIUM] Nested column filters serialize to invalid FTS5 syntax

A `COLUMNFILTER` containing another `COLUMNFILTER` is emitted without parentheses, such as `a: b: x`. SQLite permits a nested filter only as a parenthesized general expression, `a: (b: x)`. Consequently a valid public query structure, including one reachable through `extract_with_column_filters()`, cannot be converted into executable FTS5.

-   Direction: Parenthesize a nested `COLUMNFILTER` child, and preferably model the distinction between FTS5 nearsets and general expressions explicitly.

-   Test: Serialize, reparse, and execute nested include and exclude column filters; extend helper coverage to serialize and reparse extracted filters.


<a id="org2a569c0"></a>

### PY029 [MEDIUM] Parser rejects implicit AND after a column-filtered NEAR group

When deciding whether a closing parenthesis may be followed by implicit AND, the parser infers syntax from the resulting node type. A closing `NEAR(...)` wrapped in `COLUMNFILTER` is no longer directly a NEAR node and is mistaken for a grouped expression. Valid SQLite expressions such as `a:NEAR(x) y`, `a:NEAR(x) -b:y`, and `a:NEAR(x) NEAR(y)` are rejected; the serializer can generate the affected form.

-   Direction: Track whether the consumed parenthesis closed a NEAR group or a grouping expression, rather than inferring that fact from the final AST node class.

-   Test: Parse, round trip, and execute the valid filtered-NEAR examples while retaining rejection of invalid grouped adjacency such as `(x OR y) z`.


<a id="org7a68099"></a>

### PY030 [MEDIUM] QueryTokens cannot use the advertised JSON dictionary representation

The module describes its dictionary representation as suitable for JSON persistence, but `to_dict()` places a live `QueryTokens` dataclass instance into phrase data. `json.dumps(to_dict(PHRASE(QueryTokens(["a"]))))` raises `TypeError`. A supported pre-tokenized phrase therefore cannot use the advertised persistence path.

-   Direction: Represent `QueryTokens` with a JSON-safe tagged object and reconstruct it in `from_dict()`. Do not rely on the current ambiguous string encoding.

-   Test: Assert `from_dict(json.loads(json.dumps(to_dict(query)))) =` query= for ordinary, colocated, delimiter-containing, and sentinel-containing pre-tokenized terms.


<a id="orgde69afe"></a>

### PY031 [MEDIUM] Structured NEAR distances reject zero and accept booleans

`from_dict()` requires NEAR distance at least one, although SQLite and this module’s string parser accept zero. Thus `to_dict()` can emit a zero distance which `from_dict()` rejects. Its `isinstance(value, int)` check also accepts `True`, which serializes as `NEAR(a, True)` and SQLite rejects. The structured conversion pipeline is not closed over valid syntax and can emit invalid syntax from malformed input.

-   Direction: Require `type(distance) is int` and `distance >` 0=; distinguish type errors from negative-distance value errors.

-   Test: Accept distances `0`, `1`, and `10`; reject `-1`, booleans, floats, and numeric strings. Include a query-string-to-dict-to-query-string round trip for `NEAR(a, 0)`.


<a id="orgb7dc3c6"></a>

### PY032 [MEDIUM] Parser accepts caret inside NEAR where FTS5 rejects it

The phrase parser accepts an optional caret in every context, including NEAR members. APSW therefore accepts `NEAR(^a)`, `NEAR(^a+b)`, and `NEAR(a ^b)`, but SQLite’s grammar permits caret only for an initial top-level or column-filtered phrase, not inside `NEAR(...)`. The parser is not a faithful FTS5 syntax validator and produces queries that fail only at execution.

-   Direction: Pass parsing context into phrase parsing, or use a dedicated NEAR-member parser that disallows caret.

-   Test: Reject the invalid caret-in-NEAR examples while retaining acceptance of `^a`, `column:^a`, and `NEAR(a b)`.


<a id="org1b9e675"></a>

### PY033 [LOW] Structured collection fields treat strings as character sequences

Collection-valued fields accept arbitrary `Sequence` values without excluding strings. As a result, `"queries": "ab"` becomes two phrases, `"phrases": "ab"` becomes two NEAR terms, and `"columns": "title"` becomes six one-character columns. Malformed or simplified JSON is silently converted into a different query instead of being rejected.

-   Direction: Explicitly reject `str`, `bytes`, and `bytearray` for collection-valued fields, while retaining the separate documented top-level phrase-string shorthand.

-   Test: Reject string values for `queries`, `phrases`, and `columns`; verify valid lists and tuples remain accepted.


<a id="org4cbcbb0"></a>

## apsw/shell.py


<a id="org9c5535c"></a>

### PY034 [MEDIUM] JSON and JSONL emit invalid JSON for blobs over 57 bytes

Blob values are encoded with `base64.encodebytes()`, which inserts line breaks. `strip()` removes only the trailing newline, leaving an unescaped internal newline once the encoded value wraps. Both JSON output modes embed that text directly in a JSON string, so ordinary blobs of 58 bytes or more can make machine-readable output fail strict JSON parsing.

-   Direction: Use unwrapped base64, such as `base64.b64encode(v).decode("ascii")`, and serialize the resulting string with `json.dumps()`.

-   Test: In both `json` and `jsonl` modes, select blobs of 57, 58, 100, and several thousand bytes; parse the output with a strict JSON parser and verify base64 decoding restores the original bytes.


<a id="org15de914"></a>

### PY035 [MEDIUM] Valid quoted identifiers are rendered as malformed SQL

`_fmt_sql_identifier()` switches to bracket quoting for identifiers containing a double quote, but bracket quoting cannot represent an embedded `]`. A valid identifier such as `a"]b` is consequently rendered as malformed SQL. Separate SQL construction in `.dbinfo` also interpolates bracket-quoted schema names, while `.ftsq` interpolates its table name unquoted. Valid unusual table, column, and schema names can therefore break dump/restore, import, auto-import, insert output, `.dbinfo`, or `.ftsq`.

-   Direction: Use standard double-quoted SQL identifiers everywhere: `'"' + value.replace('"', '""') + '"'`. Route `.dbinfo` and `.ftsq` through the same helper rather than constructing identifier syntax locally.

-   Test: Create tables, columns, and attached schemas containing double quotes, closing brackets, both characters, spaces, and keywords. Exercise dump/restore, import, auto-import, insert mode, `.dbinfo`, and `.ftsq`.


<a id="org258c223"></a>

### PY036 [MEDIUM] NO<sub>COLOR</sub> consumes unrelated command-line options

The no-colour parser branch matches either an explicit no-colour option **or** the presence of `NO_COLOR`. When the environment variable is set, the first argument reaching this late branch is removed without its own action being performed; earlier parser branches such as \`-header\` still run normally. For example, `-csv` can be consumed without selecting CSV mode; filename-only and empty invocations also do not consistently apply the environment policy.

-   Direction: Apply `NO_COLOR` once during initialization or before option parsing. Limit the parser branch to explicit no-colour option spellings.

-   Test: With `NO_COLOR` set, process `-csv`, `-json`, `-header`, a filename-only invocation, and an empty argument list. Verify each option remains effective and colours are disabled.


<a id="orge4b6dff"></a>

### PY037 [MEDIUM] Input decoding ignores the configured error handler

`.encoding utf8:replace` stores an encoding and error-handler pair, but the file opens used by `.import`, `.autoimport`, and non-Python `.read` pass only `encoding=self.encoding[0]`. Python consequently defaults to strict decoding, despite the command documentation saying the setting affects imports and files opened by the shell. Malformed input that should be replaced or ignored instead raises `UnicodeDecodeError`.

-   Direction: Pass `errors=self.encoding[1]` at both input open sites, preferably through a shared text-input helper.

-   Test: For `.read`, `.import`, and `.autoimport`, use malformed UTF-8 under `utf8:replace`, `utf8:ignore`, and `utf8:strict`. Confirm replacement and ignore behave as configured and strict reports the decoding failure.


<a id="orgb416cde"></a>

### PY038 [MEDIUM] Echo mode replaces binding diagnostics with AttributeError

Binding-error paths return query details with `query=None`. With echo enabled, `process_sql()` calls `fmt_sql(qd.query)` before handling `qd.error_text`; `fmt_sql()` immediately calls `strip()`. Missing named bindings and incompatible binding forms therefore raise an internal `AttributeError` instead of emitting the actionable binding diagnostic.

-   Direction: Process query errors before echoing, or ensure every error result has a displayable query. Do not address the issue solely by making `fmt_sql(None)` produce an empty string.

-   Test: Enable echo and execute SQL with an unset named parameter and positional placeholders against the shell mapping bindings. Verify the original binding diagnostic is retained and no `AttributeError` is raised.


<a id="org37294ee"></a>

### PY039 [LOW] Empty JSON result sets produce no JSON document

JSON array delimiters are emitted only while outputting rows. An empty result set therefore produces no bytes rather than a parseable JSON value, making empty results indistinguishable from statements with no result columns for machine consumers.

-   Direction: If strict JSON-document semantics are intended, emit `[]\n` for result-producing queries with zero rows, while retaining no output for statements with no result set. This is qualified because SQLite’s command-line shell has compatible empty-output behavior and intended compatibility should be confirmed.

-   Test: Specify the desired compatibility contract, then test an empty `SELECT` separately from DDL/DML without result columns. Confirm JSON produces the chosen document and JSONL remains zero lines for zero rows.


<a id="org15af178"></a>

### PY040 [LOW] Auto-import classifies impossible calendar dates as dates

Date and date-time converters check only broad month and day ranges, so values such as `2023-02-29`, `2024-02-31`, and `2024-04-31` are accepted and normalized as date-shaped values. Such columns can be inferred as dates instead of remaining textual or producing a documented import error.

-   Direction: After optional day/month swapping, validate dates with `datetime.date()` or `datetime.datetime()` and document whether invalid date-like values fall back to text or fail import. This is qualified because SQLite has no native date type and the intended inference policy is not explicit.

-   Test: Cover valid `2024-02-29` and invalid `2023-02-29`, February 31, and April 31. Assert the documented fallback or error behavior.


<a id="org6380e32"></a>

### PY041 [LOW] &ndash;no-unicode enables Unicode line drawing

Both `--unicode` and `--no-unicode` use `action`"store<sub>true</sub>"= for `use_unicode`. The negative option leaves Unicode enabled in box and qbox modes and turns it on in table mode, where the default is ASCII. Users cannot reliably force ASCII output.

-   Direction: Define `--no-unicode` with `action`"store<sub>false</sub>", dest="use<sub>unicode</sub>"=.

-   Test: For box, qbox, and table modes, verify parsed option values and rendered border characters with both Unicode flags.


<a id="org89ef6b9"></a>

### PY042 [LOW] Completion termination indexes past the completion list

`complete()` returns `None` only when `state > len(self.completions)`. At the normal terminating state, `state =` len(self.completions)=, it indexes one past the list and raises `IndexError`. An empty completion list fails at state zero.

-   Direction: Change the termination check to `state >` len(self.completions)=.

-   Test: Mock completion generation and cover empty, one-entry, and multi-entry lists through the first out-of-range state.


<a id="org94e3268"></a>

### PY043 [LOW] Unicode SQL error rendering can replace the original exception

SQLite error offsets are UTF-8 byte offsets. The shell takes arbitrary 35-byte context slices around the offset and strictly decodes each slice. Either slice boundary can split a multibyte character, causing `UnicodeDecodeError` after the SQLite message is written but before the excerpt/caret and original APSW exception are delivered.

-   Direction: Convert the UTF-8 byte offset to a character offset before selecting context, or adjust byte boundaries to character boundaries. A replacement-decoding fallback can preserve the original diagnostic if exact context positioning is unavailable.

-   Test: Construct malformed SQL with a valid SQLite error offset and multibyte characters positioned across both 35-byte context boundaries. Verify the original SQL error and caret context are emitted and the original APSW exception propagates without a secondary decoding failure.


<a id="org9629b2b"></a>

### PY044 [LOW] .shell reports encoded POSIX wait status rather than exit code

`os.system()` returns a platform wait status, not necessarily the child process’s exit code. On POSIX, a command exiting with code 1 is commonly reported as 256. The shell displays that encoded status as “Exit code,” giving users an incorrect diagnostic.

-   Direction: Prefer `subprocess.run(...)` and report `returncode`, or decode the `os.system()` status with the appropriate platform APIs.

-   Test: Run commands that exit with 1 and another nonzero code; on POSIX assert the displayed values are the child exit codes rather than shifted wait statuses.


<a id="org250f3de"></a>

## apsw/speedtest.py


<a id="org5b67cb4"></a>

### PY045 [HIGH] Existing database paths are deleted without explicit consent

Before every selected driver/test run, the benchmark deletes an existing `--database` path with `os.remove()`. The option help describes only “The database file to use” and does not disclose destructive replacement. Supplying an existing database path can therefore silently and irreversibly destroy user data.

-   Direction: Refuse an existing path by default and require a clearly documented explicit overwrite option. Prefer tool-owned temporary database paths unless a user opts into a pathname.

-   Test: Create a sentinel file, run with its path without overwrite permission, and assert a nonzero status and unchanged contents. Repeat with explicit overwrite and verify benchmark creation succeeds.


<a id="org891b0c7"></a>

### PY046 [HIGH] &ndash;data-size makes compared workloads unequal and run-order-dependent

With `--data-size`, `number_name()` consumes global random state. No-bindings SQL materializes text during workload generation, whereas bindings SQL invokes `number_name()` during database execution. Reused binding workloads consequently generate different data across drivers and iterations from mutable seeded state, while alternating driver order changes which driver consumes each portion of that state. Timings can compare different row values, text sizes, query hits, and sorting work rather than the wrappers.

-   Direction: Precompute one deterministic logical workload, including each generated text value, and render it as either binding parameters or SQL literals. Do not consume mutable random state through a scalar function during timed execution.

-   Test: For several nonzero data sizes, execute bindings and no-bindings workloads under both drivers, reverse driver order, and repeat iterations. Assert identical returned rows, database contents, and generated text values in every variant.


<a id="org898b0ce"></a>

### PY047 [MEDIUM] Indexed lookup workloads frequently target absent or biased keys

Text lookup keys collected while inserting `t2` are subsequently queried against `t1.c`, so at the default scale of 10 most supposed indexed point lookups miss. Numeric rowid and `a` lookups always sample `1..50001` regardless of the actual table size; small scales have substantial miss rates, while larger scales sample only an initial prefix. The lookup workload therefore changes semantics and hit distribution with scale.

-   Direction: Record keys actually inserted into `t1` and sample lookups from those keys across the intended full row range. If misses are intended, expose and document a fixed hit/miss ratio.

-   Test: At scales 1, 5, and 10, record each generated lookup key and verify it exists unless a documented miss rate is configured. Verify sampling covers the intended table range.


<a id="orgf376a48"></a>

### PY048 [MEDIUM] Correctness mismatches do not fail the process

`--correctness` prints whether adjacent result sets compare equal but does not retain a failure flag, raise an error, or request a nonzero exit status. A displayed `False` comparison can therefore be followed by timing and a successful process exit. With fewer than two result sets, no comparison is performed and no explicit diagnostic says that cross-result correctness was not checked.

-   Direction: Track comparison failures and exit nonzero after diagnostics. Emit an explicit status when there are fewer than two comparable result sets, and avoid timing after a failed gate unless continuation is explicitly requested.

-   Test: Verify matching results exit zero, deliberately mismatched results exit nonzero, and one or zero comparable results produce a documented diagnostic and status.


<a id="orgf507012"></a>

### PY049 [MEDIUM] A single iteration crashes summary generation

The parser accepts any integer iteration count, but the summary unconditionally calls `statistics.stdev()`, which requires at least two samples. `--iterations 1` completes work and then raises an internal exception; zero and negative counts later fail when calculating a maximum over no results.

-   Direction: Reject values below one and either support one iteration by displaying standard deviation as unavailable or require at least two iterations with an argparse validation error.

-   Test: Run subprocess tests for negative, zero, one, and two iterations. Assert documented statuses and output with no traceback.


<a id="org13a6edc"></a>

### PY050 [MEDIUM] &ndash;vfs is silently ignored by sqlite3 runs

APSW setup passes `options.vfs` to its connection, while sqlite3 setup does not. The tool nevertheless prints one global VFS value and accepts the option for sqlite3-only and mixed runs. A sqlite3 run can therefore appear to use the requested VFS while actually using its default VFS, making mixed-driver comparisons misleading.

-   Direction: Reject `--vfs` for sqlite3-only runs. In mixed runs, identify the effective VFS per driver and document that the option applies only to APSW unless independently supported by sqlite3.

-   Test: Verify sqlite3-only plus an unavailable VFS and a non-memory database path fails during argument validation, APSW-only reports the APSW open failure, and mixed output identifies each driver’s effective VFS.


<a id="org07e08ce"></a>

### PY051 [LOW] Numeric options accept invalid or meaningless configurations

Plain integer and float conversions accept values outside their meaningful domains. Examples include negative `--data-size` producing an internal `randint()` failure, out-of-range Unicode percentages silently changing semantics, negative scale producing a degenerate workload, and invalid cache values producing malformed SQL or tracebacks. Statement-cache values can also have different effective behavior across drivers. Iteration handling is excluded here because it is covered by the separate iteration finding.

-   Direction: Use option-specific argparse validators: Unicode percentage 0–100, data size at least zero, a documented valid scale domain, and finite cache values in documented ranges. Restrict or accurately report statement-cache values common to both drivers.

-   Test: Parameterize invalid, boundary, NaN, and infinite values. Assert argparse-style failures identify the option, emit no traceback, and boundary configurations have documented effective behavior.


<a id="org889ad69"></a>

### PY052 [LOW] Elapsed benchmark timing uses an adjustable wall clock

Elapsed duration is computed with `time.time()`, an adjustable wall clock. Clock changes from synchronization, VM behavior, or administration can produce negative or distorted elapsed values, invalidating performance summaries.

-   Direction: Use `time.perf_counter()` or `time.perf_counter_ns()` for elapsed timing while retaining `process_time()` for CPU duration.

-   Test: Inject a wall-clock regression while the monotonic timer advances and verify reported elapsed duration remains nonnegative and follows the monotonic interval.


<a id="org5e5353d"></a>

## apsw/sqlite<sub>extra.py</sub>


<a id="orgbbc36fd"></a>

### PY053 [MEDIUM] load unnecessarily enables SQL load<sub>extension</sub> and changes state before validation

`load()` calls `db.enable_load_extension(True)` before resolving the requested extra or verifying that it is a loadable extension. This enables SQLite’s SQL `load_extension()` function as well as the C loading API, and invalid names, executables, unavailable extras, and load failures can still leave that state changed. A connection that subsequently executes untrusted SQL can thereby expose native-library loading through SQL. The severity is medium because exploitation depends on later attacker-controlled SQL and a suitable loadable library; persistent C-API enablement is also partly documented by this API.

-   Direction: Resolve and validate the requested extension first. Enable only the C loading API with `db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1)`, keeping SQL `load_extension()` disabled. Decide and document whether C-API enablement is temporary; if temporary, restore the prior setting in `finally`.

-   Test: On a fresh connection, successfully load an available extension and verify `SELECT load_extension(?)` remains unauthorized. Also test unknown names, executable entries, unavailable binaries, and forced load failures, verifying none enables SQL extension loading.


<a id="org9b32e31"></a>

### PY054 [LOW] Explicit &ndash;help exits unsuccessfully and writes help to stderr

`usage()` passes help text to `sys.exit()`, so explicit `--help` exits 1 and writes normal help to stderr.

-   Direction: Print explicit help to stdout and exit zero; send missing/invalid argument usage to stderr and nonzero status.

-   Test: In subprocesses, require `--help` status zero/stdout and invalid invocations nonzero/stderr.


<a id="org36f6310"></a>

## apsw/tests/\_<sub>main</sub>\_<sub>.py</sub>


<a id="org9dab149"></a>

### PY055 [MEDIUM] \`assertRaisesRoot\` callable mode succeeds when no exception is raised

The callable branch at lines 442–447 returns normally if the tested callable does not raise, unlike the context-manager branch which explicitly fails. Callback, VFS, and virtual-table tests using this form can therefore remain green when an expected root exception is suppressed.

-   Direction: After invoking the callable, call \`self.fail()\` if execution returns normally. Preserve the existing root-exception traversal for raised exceptions.

-   Test: Verify that a non-raising callable causes \`assertRaisesRoot(ValueError, &hellip;)\` to fail; verify direct and chained exceptions with the expected root type pass, while direct and chained exceptions with the wrong root type fail.


<a id="orgd5baf67"></a>

### PY056 [MEDIUM] Source-order checks omit required adjacent comparisons

The source-order checker at lines 6244–6249 performs no comparison for two-item rules and omits the final pair in longer rules. This disables checks such as \`CHECKVFSPY\` before \`VFSNOTIMPLEMENTED\` and \`FILEPREAMBLE\` before \`FILEPOSTAMBLE\`. Correcting the bound also exposes stale \`"use"\` entries that are absent from several corresponding \`req\` mappings.

-   Direction: Iterate through \`range(len(order) - 1)\`. Repair or remove stale \`"use"\` ordering entries before enabling the comparison, and assert that every \`order\` key exists in \`req\` so configuration errors are explicit.

-   Test: Use synthetic configurations to prove reversed two-item rules fail, a reversed final pair in a three-item rule fails, correct ordering passes, and an \`order\` key absent from \`req\` reports a clear configuration failure.


<a id="orgfbe4f04"></a>

### PY057 [MEDIUM] Repeated test runs overwrite an earlier failing exit status

In the repeated-run branch at lines 10335–10352, each caught \`SystemExit\` overwrites \`exitcode\`. A sequence of statuses such as \`[1, 0]\` exits successfully, concealing an intermittent or state-dependent failure discovered by the first iteration.

-   Direction: Initialize an aggregate status before the loop and retain a nonzero result—preferably stopping on the first failure, or preserving a nonzero code across all iterations.

-   Test: Exercise normalized results \`[1, 0]\`, \`[0, 1]\`, and \`[0, 0]\`; the first two must produce nonzero overall status and the last must produce zero.


<a id="org334e322"></a>

### PY058 [LOW] Nonpositive repeated-run counts leave the exit status uninitialized

With \`APSW<sub>TEST</sub><sub>ITERATIONS</sub>=0\` or a negative value, the loop does not execute and \`sys.exit(exitcode)\` at line 10373 raises \`UnboundLocalError\`. This is a robustness/configuration failure distinct from overwriting an earlier failed status.

-   Direction: Parse the iteration count before running tests and reject values below one with a clear configuration error.

-   Test: Verify that zero and negative counts fail before invoking the runner, while a positive count follows the normal aggregation path.


<a id="org245d282"></a>

### PY059 [MEDIUM] \`assertTablesEqual\` compares the left schema with itself

At lines 480–482, the right connection executes \`PRAGMA table<sub>info</sub>\` using \`left\` rather than \`right\`. The helper consequently verifies row count and values but not right-hand column names. The affected \`\_<sub>main</sub>\_<sub>.py</sub>\` calls at lines 3820 and 3825 can miss schema-name preservation regressions. The same copied root exists in \`apsw/tests/shelltest.py:74–75\`, where the \`.autoimport\` comparison at line 1555 can similarly accept incorrect imported column names despite matching rows.

-   Direction: Change the right-hand query in both copies to use \`right\`, or factor the comparison into one shared helper so the invariant has a single implementation.

-   Test: In both helper contexts, use one connection containing two differently named tables. Compare identical rows with different column names and assert failure; retain matching-schema success and differing column-order failure cases.


<a id="org292ba09"></a>

### PY060 [LOW] \`testSanity\` discards a mapping consistency comparison

Line 551 evaluates whether \`mapping<sub>file</sub><sub>control</sub>["SQLITE<sub>FCNTL</sub><sub>SIZE</sub><sub>HINT</sub>"]\` equals \`SQLITE<sub>FCNTL</sub><sub>SIZE</sub><sub>HINT</sub>\` and discards the resulting boolean. A future constants-generation mismatch would leave this sanity check green.

-   Direction: Replace the bare expression with \`self.assertEqual(&hellip;)\`.

-   Test: Validate the extracted check or temporarily substitute a mismatched mapping value and confirm that the sanity test fails.


<a id="org4089c2e"></a>

## apsw/tests/shelltest.py


<a id="orgcb06bd7"></a>

### PY061 [MEDIUM] \`.load\` coverage is disabled by a misspelled capability check

The condition at line 1658 checks \`load<sub>rxtension</sub>\`, which does not exist. The entire \`.load\` test block is therefore skipped even on builds exposing \`Connection.load<sub>extension</sub>\`, omitting shell command parsing, diagnostics, alternate entry points, and successful extension loading.

-   Direction: Check \`hasattr(s.db, "load<sub>extension</sub>")\`. Keep argument-count checks independent of the optional extension artifact, and narrowly skip only positive loading cases when that artifact is unavailable.

-   Test: On an extension-enabled build, cover no/too-many argument diagnostics, missing-file behavior, alternate entry-point loading, normal loading, and resulting SQL functions. Verify targeted skipping when the test extension artifact is absent.


<a id="orgcfc78ec"></a>

### PY062 [MEDIUM] Expected-error paths pass when \`shellclass.Error\` is not raised

The invalid \`.read\`, missing \`-init\` filename, direct \`&ndash;init\`, unknown option, and invalid init-file cases at lines 133–174 catch \`shellclass.Error\` but contain no \`else\`, \`fail\`, or \`assertRaises\`. A regression that returns normally instead of raising leaves each test green and also skips its diagnostic assertion.

-   Direction: Use \`assertRaises\` or \`assertRaisesRegex\`, with separate assertions for captured diagnostics where output is part of the contract.

-   Test: For each path, assert the required exception and diagnostic; exercise the assertion helper with a deliberately non-raising substitute to prove normal return fails the test.


<a id="org9810752"></a>

### PY063 [LOW] Invalid-encoding \`.autoimport\` diagnostic fixture is skipped

The malformed UTF-8 fixture at lines 1604–1608 is the sole \`"current encoding"\` case, but the loop skips all \`bytes\` content at lines 1610–1611. The intended decoding-error diagnostic and no-destination-table behavior receive no coverage.

-   Direction: Write byte fixtures with \`Path.write<sub>bytes</sub>()\` and text fixtures with \`write<sub>text</sub>(&hellip;, encoding="utf8")\`, then run the existing assertions for both.

-   Test: Write the malformed byte sequence under strict UTF-8, run \`.autoimport\`, assert the \`"current encoding"\` diagnostic, and verify that no destination table remains.


<a id="orgc41fb93"></a>

### PY064 [LOW] \`ResourceWarning\` suppression has execution-context-dependent global scope

\`suppress<sub>warning</sub>()\` at lines 48–50 mutates global warning filters when \`\_<sub>builtins</sub>\_\_\` is a module, and the call at line 1244 never restores them. Under ordinary aggregate CPython import, \`\_<sub>builtins</sub>\_\_\` is normally a dictionary and the intended suppression does nothing; under direct/module execution it can suppress \`ResourceWarning\` for the rest of the process. The standard shared-suite global-leak claim is therefore not established, but behavior is inconsistent and direct execution can hide later warnings.

-   Direction: Avoid dynamic \`\_<sub>builtins</sub>\_\_\` probing. Scope suppression with \`warnings.catch<sub>warnings</sub>()\` around only the operation that requires it, or eliminate the warning source by closing the resource.

-   Test: Install a sentinel \`ResourceWarning\` error filter and snapshot \`warnings.filters\` before the relevant operation; assert exact restoration afterward, then emit the sentinel warning and confirm it is still raised under both ordinary import and direct execution.


<a id="orgaaf7e1c"></a>

## apsw/tests/aiotest.py


<a id="orgaa24785"></a>

### PY065 [MEDIUM] \`testOverwrite\` leaves \`apsw.async<sub>run</sub><sub>coro</sub>\` modified on the runner thread

Lines 61–65 replace \`apsw.async<sub>run</sub><sub>coro</sub>\` with a lambda and provide no cleanup. The value is stored in the current thread state, so it remains installed on the unittest runner thread. Repeated in-process execution fails deterministically on the next iteration and subsequent same-thread tests can observe the fake coroutine runner.

-   Direction: Save the original value and restore it unconditionally in \`finally\` or registered cleanup.

-   Test: Run the test twice on one thread, or assert after one run that the slot equals its original value. Verify \`APSW<sub>TEST</sub><sub>ITERATIONS</sub>=2\` reaches the second iteration without failing its initial \`assertIsNone\`.


<a id="orgb089a9d"></a>

### PY066 [LOW] \`allow<sub>missing</sub><sub>dict</sub><sub>bindings</sub>\` is restored to a hard-coded value

The test enables missing dictionary bindings at lines 595–596 but always restores \`False\` at lines 744–747. A focused or programmatic run entered with the option already enabled changes caller state on exit.

-   Direction: Preserve the return value from \`allow<sub>missing</sub><sub>dict</sub><sub>bindings</sub>(True)\` and restore that exact prior value in cleanup.

-   Test: Start with the option enabled, execute the relevant test path including an exception path, and confirm it remains enabled afterward.


<a id="org109a28a"></a>

### PY067 [LOW] Best-practice test cleanup replaces all pre-existing connection hooks

The test applies recommended best practices at lines 776–778, appending a hook, then assigns \`apsw.connection<sub>hooks</sub> = []\` at lines 785–786. This removes caller-installed hooks and replaces the original list object in focused or embedded execution.

-   Direction: Preserve both the original list object and contents, restoring them in place, or remove only the hook installed by this test.

-   Test: Install a sentinel hook in a retained list, run the test, and verify the sentinel and original list identity remain while the temporary best-practice hook is removed.


<a id="org4c2cec6"></a>

### PY068 [MEDIUM] Optional dependency version parsing rejects valid PEP 440 release spellings

The Trio and AnyIO gates at lines 1350, 1379, and 1396 apply \`tuple(map(int, version.split(".")))\`. Valid local and post releases such as \`0.31.0+vendor\` and \`4.11.0.post1\` raise \`ValueError\`; abbreviated stable versions such as \`4\` compare incorrectly with \`4.0\`. Optional backend coverage can error or skip based solely on distributor version spelling. Prerelease handling requires an explicit policy because prereleases ordinarily sort below a final minimum.

-   Direction: Prefer API capability checks, or adopt a declared PEP 440-aware version dependency/parser with documented prerelease policy.

-   Test: Mock version values for abbreviated, local, post, prerelease, and genuinely older releases; assert intended run/skip behavior and no parsing failure.


<a id="orge2862f5"></a>

### PY069 [MEDIUM] Effective-deadline assertions accept incorrect deadline propagation

At lines 969–980, any AnyIO run accepts an infinite effective deadline, although current dedicated AnyIO controllers should provide one. The finite assertion only rejects deadlines more than one second late; a substantially early deadline passes. Regressions in timeout propagation or controller selection can therefore be hidden.

-   Direction: Permit infinity only for the documented fallback controller that lacks native AnyIO deadline extraction, and compare finite deadlines with an absolute one-second tolerance.

-   Test: Assert failure for infinity under the dedicated AnyIO controller and for a substantially early future deadline; retain passing cases for the documented fallback and small positive/negative rounding differences.


<a id="org906375e"></a>

### PY070 [LOW] Async tests rely on private unittest result internals

Lines 52–54 read \`self.<sub>outcome.result.showAll</sub>\`. Plain \`unittest.TestResult\`, \`TestCase.debug()\`, and valid custom runners need not expose \`showAll\`, so setup can fail before any async test executes.

-   Direction: Default verbosity safely with guarded lookup, or pass project verbosity through supported configuration rather than reading private unittest state.

-   Test: Run an \`Async\` case using plain \`unittest.TestResult()\` and through \`debug()\`, then retain a verbose text-runner case for optional diagnostics.


<a id="org40fc2a4"></a>

## apsw/tests/async<sub>meta.py</sub>


<a id="org6636dca"></a>

### PY071 [LOW] Async metadata verification is absent from ordinary suite entry points

\`AsyncMeta.testMetaJson\` is absent from the \`apsw.tests.\_<sub>main</sub>\_\_\` import block, so normal test runs, \`setup.py test\`, megatest, and wheel smoke tests do not validate runtime async classifications against \`async<sub>meta.json</sub>\`. The dedicated coverage script does run it under a Session-enabled build, so this is an ordinary-suite gap rather than a complete absence of project coverage.

-   Direction: Add a feature-aware verifier to the normal/CI paths after handling unavailable Session support and controller cleanup, or explicitly invoke the separate module in megatest and release-relevant workflows.

-   Test: Load the normal suite and assert it contains \`AsyncMeta.testMetaJson\`; introduce a deliberately mismatched metadata entry and verify the chosen normal or CI command fails.


<a id="orgc61ac6d"></a>

### PY072 [MEDIUM] Worker-side attribute classification does not await queued requests

At lines 91–104, \`classifyOne()\` calls \`send()\` for worker-side attribute access and immediately returns \`"value"\`. \`send()\` only queues a request; worker exceptions are delivered when its request is awaited. In contrast, method classification at lines 225–231 awaits its request. A worker-only getter failure can therefore be accepted as a usable value and its exception is lost.

-   Direction: Await every request returned by \`send()\` and apply the same expected-exception classification rules used by the method path. If writable attributes are intended to be tested in the worker, route the actual writable-attribute validation through the corrected setter path.

-   Test: Use a fake property getter that raises only in the worker thread and verify classification propagates or explicitly records the failure rather than returning \`"value"\`.


<a id="org9986655"></a>

### PY073 [LOW] Runtime metadata verification unconditionally requires Session support

\`Session\` is always included in the metadata/runtime object set and the test unconditionally calls \`apsw.aio.make<sub>session</sub>()\`, which rejects builds without \`SQLITE<sub>ENABLE</sub><sub>SESSION</sub>\`. The separate coverage invocation enables Session, and the normal suite does not currently import this module, limiting present impact; nevertheless direct execution on supported no-Session builds cannot validate the remaining asyncable classes and this blocks broad suite integration.

-   Direction: Retain static Session metadata, but skip only Session runtime introspection and Session-only constants when the build lacks \`apsw.Session\`, clearly reporting the feature-based skip.

-   Test: Run on a no-Session build and assert Connection, Cursor, Blob, and Backup validation succeeds while Session is skipped; retain full Session validation on a Session-enabled build.


<a id="orge1f1a23"></a>

### PY074 [LOW] \`apsw.async<sub>controller</sub>\` is not restored after metadata testing

At line 258, the test installs \`SimpleController\` in \`apsw.async<sub>controller</sub>\` without a matching reset. Later connections in the same context can select the blocking test controller. Current coverage normally runs this module in its own process, but programmatic loading or normal-suite integration makes behavior order-dependent.

-   Direction: Scope the value with \`apsw.aio.contextvar<sub>set</sub>(&hellip;)\`, or register immediate cleanup using the token returned by \`set()\`.

-   Test: Set a sentinel controller, run the test on both success and injected-failure paths, and verify the sentinel is restored afterward.


<a id="org981a16f"></a>

## apsw/tests/carray.py


<a id="org576f29c"></a>

### PY075 [LOW] Explicit INT32 reinterpretation has an inadequate oracle

The explicit `SQLITE_CARRAY_INT32` case reinterprets the final `array.array("q", range(20))` buffer as 32-bit elements, but only asserts that SQL output is **not equal** to the original 20-element list. Wrong row counts, offsets, stride, signedness, or arbitrary non-original values would pass.

-   Direction: Derive the exact native-endian signed-32-bit sequence from `arr.tobytes()`, sort it for `ORDER BY value`, and assert exact equality and cardinality.

-   Test: Use deliberately mixed 32-bit values, including negative and boundary values, and verify exact results for explicit `INT32` binding. Cover `start=/=stop` semantics using the explicitly selected item width.


<a id="orgb96913b"></a>

### PY076 [LOW] Missing or broken NumPy is reported as a passing test

`testNumpy` returns normally on any `ImportError`. Under `unittest` this is a pass, not a skip. The broad handler also suppresses import failures from an installed but unusable NumPy installation, so the compatibility path may not run while reports show success. NumPy itself is optional, so an absent package is not lost required coverage.

-   Direction: Skip only when the top-level `numpy` package is absent; re-raise import errors from a discoverable/installed package. For example, use `ModuleNotFoundError` and check `exc.name =` "numpy"= before calling `skipTest()`.

-   Test: Verify a missing top-level NumPy produces one recorded skip, while a discoverable package failing during import produces an error. With working NumPy, assert that the carray query path executes.


<a id="org1eb3423"></a>

## apsw/tests/extratest.py


<a id="org6a3fc12"></a>

### PY077 [MEDIUM] FileIO capability guard always bypasses the behavioral test

The guard checks `hasattr(apsw, "enable_load_extension")`, but extension loading is a `Connection` method, not a module-level APSW attribute. Consequently the dedicated FileIO test returns even when the packaged `fileio` extension is available, leaving Unicode filenames, `writefile`, `readfile`, and `fsdir` behavior untested.

-   Direction: Create a connection before capability detection and check `hasattr(db, "enable_load_extension")`, or load through `apsw.sqlite_extra.load(db, "fileio")`. Use an explicit skip when the extra or extension loading is unavailable.

-   Test: On an artifact containing FileIO, verify that the case executes, writes and reads all Unicode filenames, and validates exact file bytes and directory entries. Verify unavailable configurations are recorded as skips.


<a id="orga3197d8"></a>

### PY078 [LOW] fsdir validation treats the root directory row as a file

The `fsdir` loop requires every returned `data` value to equal the file blob. SQLite `fsdir` returns a root-directory row whose `data` is NULL, followed by regular-file rows containing bytes. After the dead FileIO guard is repaired, this assertion would falsely fail against conforming behavior. It is latent while the guard still bypasses the block.

-   Direction: Query name, mode, and data; separately assert one root directory with NULL data and the expected regular-file rows with exact bytes and names.

-   Test: Run against packaged FileIO on Unix and Windows, asserting the exact root-plus-files row shape, expected names, and regular-file data.


<a id="orge7c07d4"></a>

### PY079 [MEDIUM] sqlite<sub>stmt</sub> is excluded only from the pre-load module snapshot

`sqlite_stmt` is filtered from `mod_before` but retained in `mod_after`. On builds where SQLite already exposes that module, every post-load set difference contains `sqlite_stmt`, so the assertion that an extension registered something can pass for an inert or broken extra. Applicability is conditional on a build with built-in `sqlite_stmt`.

-   Direction: Use the same module-snapshot helper and exclusions before and after loading, or include `sqlite_stmt` in both sets and explicitly handle only any legitimately redundant packaged extension.

-   Test: Verify equal snapshots containing `sqlite_stmt` yield no difference, a genuinely added module is detected, and an inert extension fails the registration assertion.


<a id="orgbf77e14"></a>

### PY080 [MEDIUM] SQLAR command failures and crashes are accepted

The SQLAR branch catches `subprocess.CalledProcessError` and treats it as success, including signal termination such as the documented segfault case and silent nonzero exits that reach return-code checking. The test also does not verify that a usable archive was created.

-   Direction: Remove the broad catch. For a narrowly identified unsupported platform/toolchain combination, use a documented explicit skip or expected failure tied to that condition rather than accepting arbitrary failures.

-   Test: Require zero exit status, open the produced archive, and verify its schema, expected pathname, size, and extracted bytes. Confirm both ordinary nonzero exit and signal termination fail the test.


<a id="org77be916"></a>

### PY081 [MEDIUM] sqlite3<sub>getlock</sub> output and stderr assertion failures are swallowed

Catching every `AssertionError` around `run_cmd()` suppresses unexpected stderr, missing or wrong stdout, and some nonzero-status paths that first fail output assertions. Thus a broken `sqlite3_getlock` utility can be accepted. The WAL fixture likely needs correction rather than broad suppression.

-   Direction: Do not catch generic assertion failures. Repair the fixture or encode the exact supported-environment exception; separately validate unlocked and locked behavior.

-   Test: Assert exact zero-status unlocked output, expected locked output, and failure for empty/wrong stdout, unexpected stderr, and nonzero status.


<a id="orgc1f29a6"></a>

### PY082 [LOW] OpenBSD bypasses subprocess return-code validation

On OpenBSD, the helper skips `check_returncode()` for commands not expecting stderr. A command that emits expected stdout and then exits nonzero or crashes can therefore pass. Unexpected stderr and missing required stdout still fail, and OpenBSD runtime behavior was not available for confirmation.

-   Direction: Always check return status. Handle a precisely recognized loader failure by skipping that executable or repairing packaging/runtime search paths, not by globally disabling status validation.

-   Test: On OpenBSD, use a helper command that prints expected stdout, emits no stderr, and exits nonzero; require `CalledProcessError`. Test any explicitly recognized loader failure separately as a documented skip.


<a id="orga6e40b5"></a>

## apsw/tests/fork<sub>checker.py</sub>


<a id="org0ad8c49"></a>

### PY083 [MEDIUM] Fork-violation checks succeed even when no violation is observed

Expected `ForkingViolationError` instances are swallowed without recording whether each inherited object actually raised. The child exits zero regardless, and the unraisable-error counter is never required to be nonzero. If fork detection stopped firing entirely, inherited operations could succeed and this test would remain green.

-   Direction: Record outcomes for each inherited Connection, Cursor, Blob, and Backup operation; require the defined direct and/or unraisable violation outcomes, reject unexpected unraisable exceptions, and make bailout paths fail. Return structured child results to the parent or use a nonzero child-status protocol.

-   Test: Verify child failure when no operation raises, when only a prefix is checked before interruption, and when unexpected/excess unraisable notifications occur; verify success only when every required inherited-object violation is observed. Run on an eligible fork-checker build with `os.fork` and fork multiprocessing support.


<a id="org2e5f308"></a>

### PY084 [MEDIUM] testManyConnections runs after the checker has been disabled

Normal unittest lexical ordering runs `testForkChecker` before `testManyConnections`. The first installation is followed by shutdown, which restores original mutex methods but leaves the installation guard set. The later `install()` therefore does not reinstall fork-checker mutex methods, so the 50-connection test does not exercise the intended dynamic checker allocation/free path in aggregate-suite execution.

-   Direction: Run checker-lifecycle scenarios in fresh subprocesses, or move the allocation exercise into the first successful checker installation. A product-level reinstallation change requires a complete audit of retained wrapper state.

-   Test: Run the full class in normal order, then fork with an inherited connection after `testManyConnections` setup and require a real fork violation. Confirm that result matches a fresh-interpreter execution of the connection-stress scenario.


<a id="org7ec937e"></a>

### PY085 [LOW] Unexpected child exceptions lose their traceback diagnostic

The child exception handler calls `traceback.print_exc()` without importing `traceback`. An unexpected exception is consequently obscured by `NameError` and bypasses the intended immediate `os._exit(1)` path. Standard unittest should still report the propagated error; a false-green parent result is not established under the supported runner.

-   Direction: Import `traceback` and ensure reporting cannot bypass termination, for example by placing `traceback.print_exc()` in a `try` block with `os._exit(1)` in `finally`.

-   Test: Inject an unexpected child `RuntimeError` and assert prompt nonzero exit, the original exception in stderr, no traceback-name `NameError`, and no continuation into copied subsequent unittest cases.


<a id="orgded37bc"></a>

### PY086 [LOW] Imported-module warning suppression is ineffective

When imported normally, `__builtins__` is a dictionary, so `hasattr(__builtins__, "DeprecationWarning")` is false and the suppression helper does nothing. On eligible pre-3.13 CPython environments that emit a multithreaded-fork `DeprecationWarning`, the deliberate fork can remain noisy or fail when warnings are errors. The runtime warning is platform/state dependent.

-   Direction: Scope the precise suppression around the deliberate fork with `warnings.catch_warnings()` and `warnings.simplefilter("ignore", DeprecationWarning)`; do not alter later tests’ global warning policy.

-   Test: On an eligible runtime, promote deprecation warnings to errors, exercise the deliberate fork under a multithreaded condition, and confirm it succeeds while a later deprecation warning remains visible or raises.


<a id="org34c3ae7"></a>

## apsw/tests/ftstests.py


<a id="org6ceb9f5"></a>

### PY087 [MEDIUM] FTS5 auxiliary test class is omitted from normal-suite discovery

`FTS5Aux` defines tests for bm25, inverse document frequency, subsequence, and position rank, but is omitted from the FTS5-enabled `__all__`. Since the aggregate runner imports this module by star import, normal suite execution never discovers those tests. Conversely, direct discovery without FTS5 can retain the class and attempt unsupported FTS5 setup.

-   Direction: Include `FTS5Aux` in the FTS5-enabled export set and omit it consistently when FTS5 is unavailable, preferably through explicit `skipUnless` reporting.

-   Test: On an FTS5 build, assert that the normal aggregate suite contains an `FTS5Aux` test ID. On a non-FTS5 build, assert that every FTS-dependent class is explicitly skipped or unavailable.


<a id="orgaef953d"></a>

### PY088 [MEDIUM] UnicodeData validation processes only the final record

Only splitting each `UnicodeData.txt` line is inside the loop; conversion and assertion are dedented and run once after iteration, using the final record. This defeats the intended independent cross-check of generated Unicode names against UnicodeData and leaves errors in nonfinal records undetected. The test remains conditional on externally installed `UCD.zip` data.

-   Direction: Move record conversion and checking inside the loop, explicitly handle First/Last range records and other angle-bracket pseudo-names, and count applicable records to ensure broad validation occurred.

-   Test: Factor parsing/checking into a helper and feed ordinary named records, pseudo-names, and a First/Last range pair. Assert every applicable record calls `codepoint_name()` rather than only the final record.


<a id="org141731a"></a>

### PY089 [LOW] Unicode break subprocess reopens an open NamedTemporaryFile

The test keeps a `NamedTemporaryFile` open while starting a child that reopens its pathname. On Windows configurations where the default temporary-file sharing mode denies that second open, the optional Unicode break suite fails before segmentation behavior is tested. The repository already has a closed-before-use temporary-file workaround; exact behavior remains Python/Windows-version dependent.

-   Direction: Write through `BecauseWindowsTempfile` or another explicitly closed temporary file before launching the child, retaining reliable cleanup.

-   Test: On Windows, create a minimal valid break-test file through the selected helper, invoke `python -m apsw.unicode breaktest`, and require successful child opening and execution. A synthetic UCD archive may exercise the full path.


<a id="org0e8c64a"></a>

### PY090 [LOW] FTS5 auxiliary comparisons allow truncated actual results

IDF, subsequence, and position-ranking expectations are compared with non-strict `zip()`. A correct prefix, including an empty actual result, can pass despite omitted expected values or fewer-than-three ranking rows.

-   Direction: Assert result cardinality before item comparison, or use `zip(..., strict=True)`, which is available under the project’s supported Python versions.

-   Test: Exercise comparison logic with exact-length results, one missing final value, an empty actual result, and an extra result. Require all cardinality mismatches to fail.


<a id="org341de51"></a>

## apsw/tests/jsonb.py


<a id="org51f54db"></a>

### PY091 [LOW] Tab-to-vertical-tab normalization hides decoding errors

`testAllowedChars()` accepts any decoded result equal after replacing every tab with a vertical tab. This can hide a regression that decodes raw or escaped tab characters as `"\v"`. The compatibility rationale is obsolete because supported APSW builds require SQLite newer than the version where the relevant `\v` behavior was fixed.

-   Direction: Remove the broad normalization and require ordinary exact equality.

-   Test: Add direct assertions for raw-tab, JSON `\t`, and JSON5 `\t` inputs returning `"\t"`, plus JSON5 `\v` returning `"\v"`. A tab-to-vertical-tab decoder regression must fail.


<a id="org510954d"></a>

### PY092 [LOW] Core JSONB checks disappear under optimized Python

Round-trip and cross-implementation checks are bare Python `assert` statements. Under `python -O`, their expressions are removed, so key SQLite JSON and APSW encode/decode checks do not execute, and the `make_item` comparison is absent. Standard project test targets do not use optimized Python, making this conditional rather than normal-CI behavior.

-   Direction: Replace test-method assertions with `self.assertEqual()`. Outside a `TestCase`, use an explicit comparison that raises `AssertionError` or move the check into a test method.

-   Test: Run a targeted test in a `python -O` subprocess with a monkeypatched wrong encoder or decoder and require the test to fail after the change.


<a id="orgb2e7bb8"></a>

### PY093 [LOW] Decimal context precision leaks across tests

Conversion tests set the current decimal context precision to 128 and do not restore it. The mutable current context remains changed after each test and can drift into embedding code or future/order-dependent tests. No current in-tree later decimal arithmetic consumer was identified.

-   Direction: Use `decimal.localcontext()` for decimal-specific work, or save a copy and register cleanup with `decimal.setcontext`.

-   Test: Set precision to a distinctive value, run each conversion test independently, and assert the original precision is restored afterward.


<a id="org6a81ed7"></a>

## apsw/tests/sessiontests.py


<a id="org7a2b003"></a>

### PY094 [LOW] Global session stream-size configuration leaks process state

`testConfig` reads the global session stream size, sets it to `val + 1`, and leaves that value installed. This causes process-global state leakage and deterministic drift under repeated in-process iterations. SQLite also requires session configuration before session objects exist and without concurrent session activity; normal lexical test order has already created a Session before this test, so simple late restoration is not fully lifecycle-safe.

-   Direction: Run this global-configuration test in an isolated subprocess before session-object creation. If retained in process, enforce ordering, preserve and restore in `finally`, and ensure no concurrent session activity.

-   Test: In a subprocess, query, set, verify, and restore stream size; confirm the parent value is unchanged and repeated runs do not drift. Include a forced assertion failure to verify parent-process isolation.


<a id="orgc9d30e9"></a>

### PY095 [LOW] table<sub>filter</sub> relies on an assertion with required side effects

`table_filter` uses `assert 0 <` int(name) < 20=, and the test relies on `int("dummy")` inside that assertion to raise `ValueError`. Under `python -O`, the assertion and conversion disappear; the callback returns true instead, and the test fails through `assertRaises` without exercising intended callback-exception propagation.

-   Direction: Parse and validate the name unconditionally, explicitly raising `ValueError` for nonnumeric or out-of-range names. Do not rely on `assert` evaluation inside the callback.

-   Test: Run `Session.testAttach` under normal and optimized Python. Both modes must pass while confirming that `dummy` raises the intended `ValueError`, numbered tables preserve include/exclude behavior, and other callback exceptions still propagate.


<a id="orga4c47c8"></a>

## apsw/trace.py


<a id="org7b41c57"></a>

### PY096 [MEDIUM] Unbounded retention and duplication of statement timings

The profile callback appends every completed duration to a per-SQL list, and the individual report then builds and sorts another list containing every execution even though it returns only the configured number of rows. Memory therefore grows with every completed statement, and final reporting requires an additional linear allocation and `O(n log n)` sort. A long-running or statement-heavy traced process can consume substantial memory and suffer a disruptive final reporting cost.

-   Direction:

Maintain per-SQL totals and counts rather than every duration. Maintain individual top results in a bounded min-heap limited to `reportn` entries, while preserving exact aggregate totals.

-   Test:

Feed millions of synthetic profile callbacks with a small report limit. Verify exact totals and counts, equality of aggregate and top-individual results with a reference implementation, and that retained individual timing entries never exceed the configured limit.


<a id="orgef8bffb"></a>

### PY097 [MEDIUM] Tracer callback slots silently collide with application callbacks

The connection hook unconditionally replaces the connection profile, execution-trace, and row-trace callback slots. Existing execution abort policies, row transformations or suppression, and profile callbacks can consequently be lost. Conversely, callbacks installed later, including cursor-specific tracers that take precedence, can make tracer output incomplete without warning. Later replacement and cursor precedence are documented APSW behavior, but silently overwriting already-installed callbacks and presenting incomplete observation as authoritative are actionable defects.

-   Direction:

Where callbacks are already installed when the hook runs, snapshot and compose them while preserving execution-tracer abort results, row transformations/suppression, and profile invocation. Prefer additive `Connection.trace_v2()` tracing where suitable. Detect or document unsupported later replacement and cursor-specific overrides, and emit a clear incomplete-trace warning where feasible.

-   Test:

Exercise hook ordering before and after the tracer with execution, row, and profile callbacks, including execution aborts, row transformations, and row suppression. Include cursor-specific tracers. Verify preserved application behavior where composition is supported and an explicit warning or incomplete-trace state for unsupported overrides.


<a id="orga021254"></a>

### PY098 [MEDIUM] Target executes in the tracer's existing `__main__` namespace

The target is compiled and executed in `vars(__main__)` while `__main__` remains the module created for `apsw.trace`. The target therefore inherits tracer metadata and globals, including an incorrect `__file__`, `__package__ =` "apsw"`, the tracer =__spec__`, and tracer implementation names. This differs from normal `python target.py` semantics and can break resource lookup, package/import behavior, or code that imports `__main__`.

-   Direction:

Run the target with real script semantics, using a clean temporary `__main__` module with target `__file__`, `__name__ = "__main__"`, `__package__ = None`, `__spec__ = None`, and appropriate module metadata; verify any `runpy` based implementation also gives `import __main__` that same clean namespace.

-   Test:

Trace a script that verifies its own `__file__`, requires `__package__` and `__spec__` to be `None`, imports `__main__` to inspect the same metadata, confirms tracer globals are absent, and opens a resource relative to its file. Compare its behavior with direct script execution.


<a id="orgb01a528"></a>

### PY099 [MEDIUM] Options intended for the target are rejected

The target argument positional uses `nargs`"\*"`, so normal =argparse` processing continues to interpret post-script tokens beginning with `-` as tracer options. Commands such as `python -m apsw.trace app.py --foo` fail rather than passing the option to the target, despite documented invocation syntax that presents the target's options directly.

-   Direction:

Use `argparse.REMAINDER` for target arguments, or split tracer arguments at the first script positional. Every token after the target script path should be passed through unchanged.

-   Test:

Run a target that prints `sys.argv` with `--foo`, `-x 1`, `--name=value`, mixed positional and option arguments, and an option name recognized by the tracer. Verify all post-script tokens reach the target without requiring a `--` separator.


<a id="orgf4b46a3"></a>

### PY100 [MEDIUM] Blob logging changes byte values and marks exact-limit values as truncated

Blob bytes are formatted with `%x` rather than a zero-padded byte representation. For example, `b"\x01\x02"` is logged as `X'12'`, which denotes a different one-byte blob, and values containing low nibbles can yield invalid odd-length literals. The formatter also treats a blob whose length equals the configured limit as truncated. Diagnostic output can therefore misrepresent bound or returned bytes and falsely claim truncation.

-   Direction:

Use the bytes hexadecimal encoding and an inclusive exact-length check, for example returning `X'<hex>'` when `len(obj) <` limit= and a clearly marked prefix only when bytes were omitted. Define zero-limit behavior explicitly.

-   Test:

Cover empty data, zero bytes, low-nibble byte sequences, `0xff`, and lengths one below, equal to, and one above the limit. Decode every non-truncated rendered hex value and verify exact round-trip equality.


<a id="org41dff85"></a>

### PY101 [MEDIUM] Individual timing output can discard significant leading digits

The width for individual timing output is calculated from a stale/rebound `total` variable instead of the current individual duration. The formatter then forces that width by retaining the rightmost characters, so insufficient width removes leading digits rather than merely misaligning output. A duration such as `12345.000` can be reported as `45.000`.

-   Direction:

Calculate the width from the longest individual duration, or pre-render individual rows and use their maximum rendered width. Replace destructive string slicing with `rjust` or normal field formatting so an incorrect width cannot delete digits.

-   Test:

Populate synthetic timing data with a long individual duration and a small final aggregate duration that leaves stale narrow state. Capture the individual report and verify every significant duration digit is retained under default and selectively disabled report combinations.


<a id="org75f1838"></a>

### PY102 [LOW] String and SQL logging permits ambiguous delimiters and raw terminal controls

The string formatter escapes only LF and CR. It leaves quote delimiters, backslashes, tab, NUL, ESC, and other controls unchanged; SQL is formatted unquoted and can likewise emit raw controls. Values containing the active delimiter are visually ambiguous, and untrusted SQL, bindings, filenames, or rows can alter terminal presentation through terminal-control sequences. This does not establish a machine-readable logging contract or universal terminal side effects, but it does make diagnostic output misleading and terminal-unsafe.

-   Direction:

Use a common escaping routine that escapes the active delimiter and backslash and renders C0 controls, DEL, ESC, and control-sequence introducers textually. Apply equivalent control escaping to unquoted SQL while retaining ordinary readable Unicode.

-   Test:

Format SQL and values containing both quote styles, backslashes, tab, NUL, LF, CR, DEL, ESC/CSI, and OSC sequences. Verify each event remains one physical line, delimiters are unambiguous, and no raw terminal-control bytes appear apart from the output record terminator.


<a id="org8aa7c69"></a>

### PY103 [LOW] Negative numeric limits receive accidental slicing semantics

The `--length` and `--report-items` options accept unrestricted integers. Negative lengths are used as Python slice endpoints, silently removing trailing text or producing nonsensical blob prefixes; negative report-item counts mean “all except the last N” rather than a count. Invalid CLI input thus produces misleading output instead of an option-specific error.

-   Direction:

Validate both options with an `argparse` integer type that rejects negative values. Define and document whether zero is valid, including its behavior for output length and top-list rows.

-   Test:

Invoke both options with `-1` and larger negative values and verify exit status 2, concise option-specific diagnostics, and no traceback. Also cover documented boundary behavior for zero and one.


<a id="org532b082"></a>

## apsw/unicode.py


<a id="orgdfdf5c6"></a>

### PY104 [MEDIUM] `text_wrap` disables hyphenation for multiline input

`text_width()` returns `-1` for text containing line breaks, but `text_wrap()` treats any measurement less than or equal to the requested width as evidence that hyphenation is unnecessary. Consequently, multiline input disables hyphenation before individual hard lines are processed. A long unbreakable segment receives different wrapping merely because another line exists in the input.

-   Direction:

Only disable hyphenation after a valid nonnegative width measurement, such as `0 <` measured <= width=. A per-hard-line optimization is also appropriate.

-   Test:

Compare wrapping `abcdefghij` and `abcdefghij\nx` at width 5 with the configured/default hyphen marker. Verify that broken portions of the first line retain the marker in both cases and retain the requested display width.


<a id="org94b51cb"></a>

### PY105 [MEDIUM] `expand_tabs` does not preserve line-break count or terminal-break state

`expand_tabs()` receives content-only lines from `split_lines()` and reconstructs output by joining them, adding a terminal newline solely when more than one content line exists. It cannot distinguish an existing terminal break from no terminal break. It removes breaks from inputs such as `"\n"` and `"a\n"`, while adding one to `"a\nb"`. This changes text structure contrary to its documented newline-normalizing behavior.

-   Direction:

Track consumed hard breaks and append one normalized newline for each actual delimiter rather than inferring break state from the number of content lines. Offset-aware iteration or an internal splitter that reports delimiters can provide that information.

-   Test:

Cover empty input, a lone newline, terminal and nonterminal LF, CRLF, and Unicode hard-line separators. Verify every actual hard break becomes exactly one newline, including CRLF, and that no terminal newline is added or removed.


<a id="org6b38b78"></a>

### PY106 [LOW] `text_wrap` accepts invalid arguments and can yield invalid results

A width of zero can consume nonempty input while yielding empty strings. An unknown `justify` value falls through `do_justify()` and yields `None` despite the declared string iterator contract. Invalid-width hyphen or replacement values can also enter width arithmetic and violate output-width invariants. The confirmed issue is inadequate public-argument validation; claims of one specific negative-width failure mode should not be retained.

-   Direction:

At generator execution, require an integer width of at least one; require or explicitly coerce a valid `Justify` member; reject hyphen and invalid-replacement values with negative display width; and make `do_justify()` raise a clear error for unsupported values.

-   Test:

Assert clear exceptions for widths zero and negative one, an unknown justification value, a newline-containing hyphen, and an invalid replacement used with invalid-width source text. For valid calls, verify every yielded value is a string with the requested display width.


<a id="orga25e4d2"></a>

### PY107 [MEDIUM] `guess_paragraphs` destroys CRLF before normalizing it

The function replaces every CR with a paragraph separator before its later CRLF normalization step. Thus the CR in every CRLF pair has already been removed, making the CRLF normalization unreachable for original CRLF input. Windows-style line breaks can become a paragraph separator plus LF rather than the same soft break represented by LF input.

-   Direction:

Normalize CRLF to LF before converting standalone CR to a paragraph separator, then process other definite paragraph separators.

-   Test:

Verify `guess_paragraphs("one\ntwo")` equals `guess_paragraphs("one\r\ntwo")`. Include leading, trailing, and repeated CRLF cases and verify a single CRLF does not create an empty or separate paragraph.


<a id="orgde3b7cb"></a>

### PY108 [LOW] `word_iter_with_offsets` has an incorrect public return annotation

The function is annotated as returning `Iterator[str]` but yields three-tuples of start offset, end offset, and word text. Parallel offset iterator APIs use the tuple annotation, and the FTS tokenizer forwards these tuples. Static type checking therefore rejects valid tuple unpacking and misstates the runtime contract.

-   Direction:

Change the return annotation to `Iterator[tuple[int, int, str]]`.

-   Test:

Add a static typing fixture that unpacks `start`, `end`, and `word` and verifies their inferred types. Supplement it with a runtime assertion of tuple shape and element types.


<a id="org7b7bf9b"></a>

### PY109 [LOW] `grapheme_find` documentation contradicts empty-needle behavior

The public documentation says a zero-length substring returns `-1`, but the C implementation often returns the normalized start offset. It special-cases `start =` 0= even for some excluded ranges, and otherwise accepts the initial start candidate without establishing that it is a grapheme boundary. Existing tests compare against `str.find()` only for simple strings; empty-needle behavior for starts inside multi-codepoint graphemes is not covered. Users following the documentation therefore receive results contrary to the stated contract.

-   Direction:

Choose and document the intended empty-needle contract. If `str.find()` compatibility is intended, remove the zero-length exclusion and make range normalization fully compatible. If only grapheme-boundary matches are intended, validate the initial start offset and document the boundary-specific semantics.

-   Test:

Test an empty needle at the beginning, inside, and end of a multi-codepoint grapheme, including restricted and negative end values. Verify the selected contract is stable and documented.


<a id="org1ec399c"></a>

### PY110 [LOW] CLI `casefold` and `strip` stdin/stdout defaults are ineffective

The input and output positionals have defaults but lack `nargs`"?"`. They remain required under =argparse`, so invoking either command without both file arguments fails instead of using the advertised standard-stream defaults. Pipeline use and input-file-to-stdout use therefore do not work as intended.

-   Direction:

Add `nargs`"?"= to both input and output positionals for both commands, retaining the existing standard-stream defaults.

-   Test:

For both commands, subprocess-test no file arguments with piped stdin, one input filename with stdout output, explicit input/output filenames, and explicit `-` standard streams.


<a id="orgc865aff"></a>

### PY111 [LOW] Benchmark mode divides by zero for an empty source file

Benchmark setup computes a scale factor using `len(base_text)` as a divisor. An empty source file deterministically raises `ZeroDivisionError` before benchmark generation or reporting, resulting in an unhelpful traceback. Whether zero or negative requested benchmark sizes should be allowed is a separate policy question.

-   Direction:

Detect an empty benchmark source before scaling and issue a concise command-line diagnostic stating that the source file must not be empty.

-   Test:

Invoke benchmark mode with an empty temporary file and a normal positive requested size. Verify a concise nonzero-exit diagnostic without a Python traceback.


<a id="org249de59"></a>

## doc/conf.py


<a id="orge11505a"></a>

### PY112 [LOW] Missing documentation metadata is checked with `assert`

`version` and `today` are validated only with `assert`. Under `python -O` or `PYTHONOPTIMIZE`, that check is removed, allowing a direct misconfigured Sphinx build to continue with missing values and produce malformed documentation metadata such as an `APSW None documentation` title. Repository-owned HTML, link-check, publish, and source-package paths provide these variables, so the issue is limited to optimized, misconfigured direct builds.

-   Direction:

Replace the assertion with explicit validation that identifies missing variables, for example by raising a `RuntimeError` or Sphinx configuration error when `VERSION` or `RELEASEDATE` is absent.

-   Test:

Load the configuration or run a minimal Sphinx build with both variables present and with each absent, under normal and optimized Python. Verify present values succeed and missing values always fail clearly regardless of optimization.


<a id="org310a49e"></a>

## examples/async.py


<a id="org0510f4e"></a>

### PY113 [HIGH] Python 3.10 cannot parse the TaskGroup cancellation example

\`examples/async.py:169\` uses \`asyncio.TaskGroup\` and line 189 uses \`except\*\`, both Python 3.11 features.  The project declares Python 3.10 support.  Python 3.10 cannot parse \`except\*\`; consequently \`tools/example2rst.py\`, which compiles the entire source before executing it, cannot generate this example’s documentation on the supported minimum interpreter.

Python 3.10 users cannot run any part of the example, and Python 3.10 documentation builds fail before producing the async tour.

-   Direction:

Rewrite the cancellation example using Python-3.10-compatible task coordination and explicit cancellation, or isolate the 3.11-only source so Python 3.10 never parses it.  A runtime version guard alone is insufficient for \`except\*\`.

-   Test:

Run \`python3.10 -m py<sub>compile</sub> examples/async.py\`; execute the cancellation demonstration on Python 3.10; verify the intentional failure is observed, other tasks are cancelled, and the database remains usable.  Generate the async documentation with Python 3.10.


<a id="orgf4448ee"></a>

### PY114 [LOW] Backup connections lack deterministic exception-safe cleanup

Four connections are acquired at lines 579–590, while only the two async connections are explicitly closed at lines 625–627, after all backup work succeeds.  No \`finally\` or exit stack protects partial setup or cancellation.  Async connections own controller threads that are signalled to stop on close.  APSW connection finalization normally closes unreferenced connections, so the synchronous connections and ordinary successful completion are not established leaks.

An exception or cancellation can retain open handles and async worker threads while a traceback or another reference retains coroutine locals.  The example therefore does not provide deterministic cleanup.

-   Direction:

Use \`contextlib.AsyncExitStack\`, registering each async connection with \`contextlib.aclosing\` and each synchronous connection with \`contextlib.closing\` immediately after acquisition.

-   Test:

Inject a \`Backup.step()\` failure after all connections are created and retain its traceback.  Verify every connection closes and controller threads terminate within a bound.  Repeat under cancellation.


<a id="org606b8f4"></a>

### PY115 [LOW] The second timeout reports cumulative rather than local elapsed time

\`start\` is assigned before the first timeout at line 270 and used for its report.  The second timeout at lines 280–283 does not reset \`start\`, but its handler again reports \`end - start\` at lines 284–287.  Documentation generation deliberately exercises this long-running-query path.

The second nominal half-second timeout is reported as roughly the combined duration of both demonstrations, misleading readers about cancellation timing.

-   Direction:

Reset \`start = trio.current<sub>time</sub>()\` immediately before the second \`trio.fail<sub>after</sub>(0.5)\` block.

-   Test:

Capture both timeout reports and assert each is near 0.5 seconds with a non-flaky tolerance; specifically assert the second is not cumulative.


<a id="org6e3f0db"></a>

### PY116 [LOW] Changeset-size estimate is printed as a literal placeholder

At line 734, \`print("Size estimate {session.changeset<sub>size</sub>}")\` lacks an \`f\` prefix.  Size estimation was enabled immediately beforehand, and the actual changeset size is printed on the following lines.

The example and generated documentation display the literal placeholder rather than demonstrating the enabled \`changeset<sub>size</sub>\` API.

-   Direction:

Use \`print(f"Size estimate {session.changeset<sub>size</sub>}")\`.

-   Test:

Capture the session-section output and verify the estimate line contains an integer, does not contain \`{session.changeset<sub>size</sub>}\`, and is followed by the actual size.


<a id="org24fc41d"></a>

## examples/fts.py


<a id="org6b6a36e"></a>

### PY117 [LOW] Standalone execution depends on a non-shipped fixture and can create an empty database

Lines 27–29 open \`recipes.db\` with default read-write/create flags, then assume its schema at lines 33–38 and a fixed dataset rowid at lines 232–235.  The repository does not ship that database.  The Makefile copies the canonical external fixture before documentation generation.

Running without the fixture creates an empty \`recipes.db\` before failing on schema use.  A compatible but different dataset can later fail at the hard-coded rowid.

-   Direction:

Document and validate the fixture prerequisite, accept a database path, and open it read-write without create mode.  Either explicitly bind the fixed rowid to the canonical fixture or select a suitable existing row.

-   Test:

From an empty temporary directory, verify a clear prerequisite error and no database creation.  Verify unchanged output with the Makefile-provisioned fixture; if dataset independence is intended, test a compatible fixture with different rowids.


<a id="org93c5b6f"></a>

### PY118 [LOW] Empty FTS results leave the first result variable undefined

Lines 151–163 assign \`row\` only inside a \`for row in search<sub>table.search</sub>("lemon OR guava")\` loop, break after its first result, and unconditionally use \`row.rowid\` afterward.  \`Table.search()\` may legitimately yield no rows.  At module scope this produces \`NameError\`.

A normal zero-result search crashes instead of showing readers how to handle it.

-   Direction:

Use \`next(search<sub>table.search</sub>(&hellip;), None)\` and branch explicitly, printing a controlled no-results result before calling \`row<sub>by</sub><sub>id</sub>()\` only when a row exists.

-   Test:

Use one FTS fixture with no matching terms and assert the controlled no-results behavior; use another with a match and assert its rowid is passed to \`row<sub>by</sub><sub>id</sub>()\`.


<a id="org267dc58"></a>

### PY119 [LOW] The custom tokenizer accepts zero and negative block sizes

The tokenizer argument schema at lines 613–620 converts \`block\` with plain \`int\`.  The callback uses it as the \`range()\` step at lines 627–635.  \`block=0\` reaches a generic \`ValueError\`, while a negative value generally yields no tokens for non-empty text.

Copied or directly reused tokenizer code can fail late with an unhelpful error or silently tokenize nothing.

-   Direction:

Use a converter that rejects values below one with a parameter-specific error during tokenizer argument parsing.

-   Test:

Exercise \`block=0\`, \`block=-1\`, \`block=1\`, and \`block=5\`; invalid values should fail at factory setup and valid values should preserve expected token offsets.


<a id="org0bde99b"></a>

### PY120 [LOW] The custom tokenizer factory has an incorrect return annotation

\`atokenizer\` is annotated as returning \`apsw.FTS5Tokenizer\` at lines 607–610, but lines 627–637 return a nested string-tokenizing Python callback.  The APSW wrapper type is instead returned by \`Connection.fts5<sub>tokenizer</sub>()\`.

The instructional annotation gives readers and static analyzers the wrong contract for the decorator callback. The related `StringTokenizer` decorator is also declared as returning `apsw.Tokenizer` at `apsw/fts5.py:243-265` although it returns a tokenizer factory, reflecting the lack of a precise public string-tokenizer callback/factory contract.

-   Direction:

Remove the local return annotation, or define a public string-tokenizer callback/factory contract and annotate the callback as accepting `(str, int, str | None)` and yielding `(start, end, token)` tuples; declare `StringTokenizer` as returning `apsw.FTS5TokenizerFactory`.

-   Test:

Add a type-checking fixture for the decorated factory and retain a runtime registration test that verifies tokens and UTF-8 offsets.


<a id="orgff9c43f"></a>

## examples/json.py


<a id="orgac88bea"></a>

### PY121 [MEDIUM] \`ContextVar.set()\` is used as a context manager on unsupported Python versions

Line 232 uses \`with volume.set("loud"):\`.  Before Python 3.14, \`ContextVar.set()\` returns a \`Token\` without context-manager methods.  The project supports Python 3.10 onward, and the example is compiled and executed during documentation generation.

The JSON example fails on Python 3.10 through 3.13 before completing its final query, and documentation generation can fail on those supported interpreters.

-   Direction:

Store the token, execute the query in a \`try\` block, and call \`volume.reset(token)\` in \`finally\`.

-   Test:

Run the complete example on Python 3.10 through 3.14.  Verify callbacks observe \`"loud"\` during the query, \`"quiet"\` afterward, and that reset occurs when the query raises.


<a id="org79b3173"></a>

### PY122 [LOW] Tagged-object decoding is overbroad

Lines 194–203 pass every decoded object through an \`object<sub>hook</sub>\` that treats any non-\`None\` \`"$py$type"\` as a protocol tag.  An ordinary object with an unknown tag raises \`ValueError\`; a \`"complex"\` object with extra fields is converted and loses those fields.  The encoder describes the key as merely unlikely, without reserving it or validating an exact tagged shape.

Otherwise ordinary dictionaries can be rejected or silently transformed when they collide with the informal discriminator.

-   Direction:

Leave unknown tags unchanged and convert only objects with the complete expected complex representation.  If arbitrary dictionaries must be lossless, document a reserved namespace and provide an envelope or escaping scheme.

-   Test:

Verify unknown tags remain dictionaries; malformed or extra-field complex-tagged objects do not convert; and the exact intended complex representation still becomes a \`complex\`.


<a id="org845c6e2"></a>

### PY123 [LOW] The introductory typing comment falsely says postponed annotations are mandatory

Lines 3–6 state that using annotations requires \`from <span class="underline"><span class="underline">future</span></span> import annotations\`.  The annotations used in this Python-3.10+ module reference names available at definition time, and the file compiles with only that future import removed.

The tour teaches an incorrect rule about ordinary type annotations and optional postponed evaluation.

-   Direction:

State that annotations are optional and that the future import postpones evaluation but is not required for these annotations, or remove the unnecessary explanation and import.

-   Test:

Optionally compile the definitions without the future import on Python 3.10 and the newest supported Python version.


<a id="org11cc2f4"></a>

## examples/main.py


<a id="org033ddac"></a>

### PY124 [HIGH] Fixed working-directory database names can alter or delete user data

\`examples/main.py\` opens or creates \`dbfile\` at lines 65–75, modifies it extensively, and later opens \`myobfudb\`, writes it, and unconditionally deletes its pathname at lines 1218–1243.  \`examples/session.py\` similarly backs up into \`alice.db\` and \`bob.db\` at lines 248–254 and creates tables in \`diff<sub>demo.db</sub>\` and attached \`other.db\` at lines 489–502.  The Makefile and fault-injection tool work around both examples by deleting or rewriting these names, but direct execution remains unprotected.

Direct execution can modify an unrelated \`dbfile\`, overwrite pre-existing backup destinations, delete a compatible pre-existing \`myobfudb\`, leave artifacts, and make later runs non-idempotent.  The filename-based Makefile cleanup can itself delete unrelated same-named files in its working directory.

-   Direction:

Use a dedicated \`tempfile.TemporaryDirectory\` and absolute paths for every disposable database in both examples.  Close all connections before its cleanup, including on Windows.  Parameterize attached database paths rather than embedding fixed names, and remove fixed-name Makefile cleanup once the examples are self-contained.

-   Test:

Run both examples twice in temporary working directories containing sentinel files for every affected name.  Assert both runs succeed, sentinels are unchanged, all demonstration databases are confined to their dedicated temporary directories, and no database, journal, WAL, or SHM artifacts remain in the caller’s directory.


<a id="org72ffd58"></a>

### PY125 [MEDIUM] Complex-number conversion fails for valid values

Lines 512–535 serialize complex values as \`f"{c.real}+{c.imag}"\` and deserialize by splitting on \`"+"\`.  Valid exponent notation contains plus signs: \`1e20 + 4j\` serializes as \`1e+20+4.0\`, and negative exponential imaginary components similarly split into too many pieces.

The type-conversion example works for the chosen simple value but fails for ordinary valid complex numbers.

-   Direction:

Use an unambiguous representation, such as \`repr(value)\` with \`complex(value)\`, a two-element JSON representation, or two numeric columns.

-   Test:

Round-trip ordinary values, positive and negative scientific exponents, signed zero, infinities, and NaNs, using appropriate non-finite comparison rules.


<a id="orgde324c8"></a>

### PY126 [MEDIUM] Symlink-following filesystem traversal can recurse through cycles

At lines 1052–1069, \`entry.is<sub>dir</sub>()\` follows directory symlinks when \`ignore<sub>symlinks</sub>=False\`, while recursion has no visited-directory tracking.  A symlink to an ancestor repeatedly re-enters the same directory graph until an OS or recursion/resource failure.

The advertised symlink-following mode can perform unbounded repeated traversal and fail instead of producing a bounded result.

-   Direction:

When following links, track visited directories using followed \`(st<sub>dev</sub>, st<sub>ino</sub>)\` identity and document whether links may escape the supplied roots.

-   Test:

Create a tree containing \`child/back -> root\`.  Verify default mode skips it and follow-links mode terminates with each physical directory handled according to a documented deduplication policy.


<a id="org2f8d143"></a>

### PY127 [LOW] The “oldest Python files” query orders by file size

The section says it finds the three oldest Python files, but its SQL orders \`ORDER BY st<sub>size</sub> DESC\`, selecting the largest files rather than the oldest.

The output contradicts the stated purpose and teaches an incorrect query.

-   Direction:

Order by `st_ctime ASC` and label it earliest metadata-change time, or consistently select, display, and order `st_mtime` for the ordinary “oldest files” meaning.

-   Test:

Use synthetic rows whose size, metadata-change time, and modification-time orders differ; assert the query selects the three earliest values of the chosen and accurately labelled timestamp regardless of size.


<a id="org849d8dd"></a>

### PY128 [LOW] Incremental blob cleanup is not exception-safe

The blob is opened at line 755 and closed only at line 762.  Exceptions from intermediate \`write\`, \`seek\`, or \`read\` calls bypass the close.  APSW provides blob context-manager support and notes that closing can release resources and report deferred I/O errors.

Copied code can retain a blob handle and associated database resources or delay close-time error handling until finalization.

-   Direction:

Wrap the blob operations in \`with connection.blob<sub>open</sub>(&hellip;) as blob:\`.

-   Test:

Raise an exception within the block, retain the blob object, verify it is closed afterward, and verify a subsequent operation requiring its resources succeeds.


<a id="orgfa8deeb"></a>

### PY129 [LOW] Trace callback puts required mutations inside an assertion

At lines 1357–1365, removal of \`sql\` and \`connection\` from the trace dictionary occurs inside \`assert\`.  Python removes the full assertion expression, including both \`pop()\` calls, under \`python -O\`.

Optimized execution prints fields the callback says it removes and teaches behavior that changes under a standard interpreter option.

-   Direction:

Perform the \`pop()\` operations unconditionally, then assert their values separately; use explicit checks if validation must remain enabled under optimization.

-   Test:

Run the callback normally and with \`python -O\`; in both modes verify \`sql\` and \`connection\` are removed before the event is printed.


<a id="orgf0a6ae7"></a>

## examples/session.py


<a id="org35fc1d4"></a>

### PY130 [MEDIUM] Bundled SQL is resolved from the process working directory

Lines 33 and 450 read \`pathlib.Path("session.sql")\`, which is relative to the caller’s current directory.  The only repository copy is \`doc/<sub>static</sub>/samples/session.sql\`; there is no root-level or \`examples/\` copy.  Internal documentation and fault-injection tooling rewrites the path before executing transformed code, while the published source retains the non-working path.

Direct invocation from the repository root, \`examples/\`, or an unrelated directory fails with \`FileNotFoundError\`.

-   Direction:

Resolve the bundled SQL from \`\_<sub>file</sub>\_\_\`, preferably by placing it adjacent to the example and using \`Path(<span class="underline"><span class="underline">file</span></span>).with<sub>name</sub>("session.sql")\`; otherwise construct the current sample location relative to \`\_<sub>file</sub>\_\_\`.  Read with an explicit encoding and remove hidden execution rewrites once self-contained.

-   Test:

Invoke the script by absolute path with repository-root, \`examples/\`, and unrelated temporary directories as the current directory.  Each invocation should locate the same SQL and complete.


<a id="org1ad8336"></a>

### PY131 [LOW] Session availability is reported but Session APIs are still used unconditionally

Lines 20–22 report whether SQLite and APSW Session support are available, but line 43 unconditionally creates \`apsw.Session\`, followed by further Session-only APIs.  Session support is optional in source builds, and the repository itself uses \`hasattr(apsw, "Session")\` as a guard elsewhere.

Users of an APSW build without Session support receive an avoidable traceback after the example has already diagnosed the missing prerequisite.

-   Direction:

After reporting availability, exit cleanly before creating resources when \`apsw.Session\` is absent, with a concise prerequisite message.

-   Test:

Run once with Session support and once without it.  Verify the enabled build completes and the disabled build exits without a traceback or database artifacts.


<a id="orgb69cd0c"></a>

### PY132 [LOW] The rebased changeset is computed but never demonstrated or applied

Lines 440–441 promise Alice-then-Bob ordering, while lines 478–481 compute `bob_rebased` but never display, apply, compare, or query it. Rebasing produces bytes; it does not apply them.

Readers cannot observe the change or verify the claimed result.

-   Direction:

Display the rebased changeset and preferably apply it to the documented post-Alice state and query the result.

-   Test:

Generate, apply, and assert the rebased result; require observable output in generated documentation.


<a id="orgd22f093"></a>

## setup.py


<a id="orge5a8f98"></a>

### PY133 [HIGH] Archive extraction escapes the extraction root

At `setup.py:258-268`, archive member names are normalized, the first path component is discarded, and the resulting path is written without validating resolved containment beneath the extraction directory. `os.pathsep` is tested instead of a filesystem root separator, and leading surviving `..` components are not rejected. For example, `top/../../../outside` normalizes to `../../outside` and ultimately writes through `sqlite3/../outside`. Archive-controlled names reach this code for the SQLite source ZIP, component ZIPs, and amalgamation TAR.

A crafted archive can write outside `sqlite3`, potentially overwriting source or build files that are subsequently compiled or executed.

-   Direction:

Parse archive names with portable archive-path semantics; reject absolute, drive, UNC, and any `..` components; require and remove only the expected archive root; and resolve/check the final destination is contained by the resolved extraction root before creating directories or writing files.

-   Test:

Exercise malicious names with surviving `..` components and both slash styles, asserting no file appears outside a temporary extraction root. Cover valid nested entries as well.


<a id="org504498f"></a>

### PY134 [MEDIUM] Download timeout absence and unpinned HTTP downgrade

At `setup.py:919-924`, both `urlopen(url).read()` calls lack explicit timeouts. After repeated HTTPS failures, an HTTPS URL is replaced with HTTP when `checksum` is truthy. At `setup.py:881-884`, `--missing-checksum-ok` permits missing checksum entries, so that boolean does not establish that downloaded bytes are pinned. Full-source and component fetches propagate that option.

Network fetches can block without an operation timeout. When an HTTPS failure is combined with explicit acceptance of a missing checksum, unauthenticated HTTP bytes can be accepted, extracted, and compiled.

-   Direction:

Remove the plaintext HTTP fallback; use explicit operation timeouts and bounded retries for expected transient failures. Do not permit a downgrade for content unless a matching pinned digest has already been located and will be verified.

-   Test:

Mock HTTPS timeout/certificate failures and an HTTP response. Verify timeouts are supplied, retries terminate, and HTTP is never attempted for unpinned content.


<a id="orgb0d19eb"></a>

### PY135 [LOW] Fetch removes the existing SQLite tree before successful replacement

At `setup.py:281-284`, `sqlite3/` is removed with `shutil.rmtree()` before download, checksum verification, archive parsing, or extraction. Documentation permits users to provide their own SQLite tree there. Repository automation generally treats the directory as reproducible input and the command announces its replacement behavior, so the confirmed issue is failed-refresh recovery rather than an unexpected destructive command.

A download, checksum, or extraction failure leaves no prior working or locally supplied SQLite tree.

-   Direction:

Download and verify into a temporary sibling location, extract there, and replace `sqlite3/` only after success. Preserve or restore the prior tree if final replacement fails.

-   Test:

Place a sentinel in an existing `sqlite3/` tree, inject download and extraction failures, and confirm it remains unchanged. Confirm successful fetch replaces it.


<a id="orgade0dab"></a>

### PY136 [MEDIUM] Extension-test markers are invisible to in-process tests

Build finalization uses `os.putenv("APSW_TEST_" + e.upper(), "1")` at `setup.py:615` and `os.putenv("APSW_TEST_ICU", "1")` at `setup.py:678`. Tests inspect `os.environ` at `apsw/tests/__main__.py:3843-3844`. Direct `os.putenv()` does not update Python’s existing `os.environ` mapping. The release validation and megatest paths chain build and test commands in the same setup process.

An explicitly enabled extension can be absent or misbuilt while in-process tests continue treating it as optional.

-   Direction:

Set the markers through `os.environ[...]` so the test process observes them; preserve and restore prior values if command-state isolation is needed.

-   Test:

Finalize `build_ext` with an enabled extension, verify the marker is present in `os.environ`, then make the feature probe fail and confirm the test fails.


<a id="org59e0b08"></a>

### PY137 [MEDIUM] Source-distribution generation overwrites and leaves `setup.apsw`

At `setup.py:710-713`, `sdist` copies a selected configuration to `setup.apsw` and calls its parent without backup or `finally` restoration. `setup.apsw` participates in setuptools configuration discovery at `setup.py:70-77` and is included in source distributions. A normal build leaves a new untracked file; `--for-pypi` leaves configuration that changes later builds; an existing packager or user configuration is overwritten.

User configuration can be lost, and later builds can unexpectedly use the release configuration, including after a failing `sdist`.

-   Direction:

Populate the release tree/archive without mutating source-root configuration. If temporary mutation is unavoidable, preserve the original bytes, metadata, and absence state and restore them in `finally`.

-   Test:

Run successful and failing `sdist` operations with both an existing custom file and no file. Verify source state is restored while the resulting archive contains the intended configuration.


<a id="orgcaa67f1"></a>

### PY138 [LOW] `--missing-checksum-ok` does not apply to the amalgamation

The source and component download calls at `setup.py:294` and `323` pass `missing_checksum_ok=self.missing_checksum_ok`. The amalgamation call at `setup.py:340` is `download(AURL, checksum=True)`, leaving the option at its default false value. Documentation describes the option without excluding the amalgamation.

For a version lacking an amalgamation checksum, the documented opt-in option fails; `--all` can download other content and then fail on the amalgamation.

-   Direction:

Pass `missing_checksum_ok=self.missing_checksum_ok` to the amalgamation download, together with the authenticated-download handling above.

-   Test:

Mock a version absent from `checksums`. Verify source and amalgamation fetch succeed with the option and fail without it.


<a id="orgedfde32"></a>

### PY139 [MEDIUM] No-old-names stub customization mutates the source tree

`update_type_stubs_old_names()` at `setup.py:432-442` reads and writes `apsw/__init__.pyi` directly, and `apsw_build_ext.finalize_options()` invokes it at `setup.py:544` for `--apsw-no-old-names`. The Makefile’s no-old-names path explicitly removes and regenerates the stub afterward, confirming the mutation. The affected mode is limited to builds using that option, but read-only and concurrent source builds are directly affected.

Such builds dirty the checkout, fail when the stub cannot be rewritten, and can contaminate sequential or concurrent builds using different options.

-   Direction:

Do not edit the source stub. Generate the option-specific package-data stub in the build directory. The canonical generated-stub source remains `src/apswtypes.py`; regenerate the baseline stub through `tools/gendocstrings.py` rather than manually maintaining `apsw/__init__.pyi`.

-   Test:

Build default and no-old-names wheels from clean read-only copies; compare source bytes before and after and inspect the wheel stub against runtime exports. Exercise sequential opposite-option and isolated concurrent builds.


<a id="org4b9948e"></a>

### PY140 [MEDIUM] ICU helper failures and quoted flags are mishandled

At `setup.py:833-843`, `subprocess.run()` is used without `check=True` inside a suppression of `CalledProcessError`, so nonzero helper exits are not caught. `icu-config` results are returned even if a flags or libraries query fails. At `setup.py:658-675`, helper output is parsed with `shlex.split(..., posix=False)`; conventional values such as `-I"/path with spaces"` are split into invalid tokens. Empty successful compiler flags are legitimate, so the confirmed failure is unsuccessful or partially successful helper invocation and incorrect quoted-path parsing.

ICU discovery can silently omit configuration or proceed to confusing compile/link failures. Include, library, and macro values containing quoted spaces are misparsed.

-   Direction:

Check helper return codes, apply timeouts, report failed queries clearly, permit valid empty compiler flags, require valid link settings, and parse `pkg-config=/=icu-config` output using the appropriate shell semantics.

-   Test:

Use fake helpers that separately fail flags, fail libraries, fail both, return empty flags with valid libraries, and emit quoted include/library paths and macro values. Verify clear failure/fallback behavior and correct single-token parsing.


<a id="org1880a64"></a>

## src/apswtypes.py


<a id="orgff92416"></a>

### PY141 [MEDIUM] `Self` and `Buffer` imports exceed supported typing targets

`src/apswtypes.py:3-4` imports `typing.Self` and `collections.abc.Buffer`, and the generated stub copies both imports. The package declares `python_requires`">=3.10"`, but =Self` is unavailable for the Python 3.10 typing target and `Buffer` is unavailable for Python 3.10 and 3.11. The declarations also use starred variadic typing syntax that is not compatible with all supported Python 3.10 checker targets.

Type checking on supported Python 3.10/3.11 targets can report unavailable imports or fail to resolve central aliases such as `SQLiteValue`.

-   Direction:

Use a declared `typing_extensions` dependency and import the required backports appropriately in `src/apswtypes.py`; also use Python-3.10-compatible `Unpack` spellings for starred variadic aliases, then regenerate the stub.

-   Test:

Run supported mypy and pyright versions targeting Python 3.10, 3.11, and 3.12+ against a program importing `apsw` and annotating `apsw.SQLiteValue`. Assert no diagnostic originates from unavailable APSW stub imports.


<a id="orgc3a4e4e"></a>

### PY142 [MEDIUM] Window `step` and `inverse` protocols require exactly one value

`src/apswtypes.py:69` and `81` declare one-argument methods. `create_window_function(..., numargs`-1)= supports arbitrary arity, and `src/connection.c` constructs and passes every SQL argument to both callbacks. Replacing these methods solely with variadic protocol methods is insufficient because fixed-arity concrete methods may not structurally satisfy a protocol promising arbitrary arity.

Working zero-argument and multi-argument window classes are rejected by static checking despite correct runtime invocation.

-   Direction:

Model `step` and `inverse` as callable attributes compatible with fixed and variadic arities, such as read-only attributes returning `Callable[..., None]`, while retaining precise no-argument contracts for `final` and `value`. Validate the spelling with the supported type checkers.

-   Test:

Statically check classes implementing zero-, one-, two-, and variadic-argument callbacks. Register and execute zero- and two-argument window functions and verify exact callback argument counts.


<a id="org62183a3"></a>

### PY143 [MEDIUM] `WindowFinal` incorrectly permits SQL arguments

`src/apswtypes.py:92-93` permits `WindowFinal` to receive SQL values. At runtime, `src/connection.c:3627-3629` calls tuple-form final callbacks only with their state object; class-form `final` receives no explicit values. The generated async alias inherits the incorrect signature.

A state-only callback can be rejected by this overly variadic contract, while a variadic implementation that assumes at least one SQL value can type-check but fail when runtime supplies none.

-   Direction:

Define `WindowFinal = Callable[[WindowT], SQLiteResult]`, or `SQLiteValue` if result-alias broadening is not applied concurrently; clarify that tuple form receives only state.

-   Test:

Statically require a tuple final callback to accept only the state object and reject one that requires any SQL value. At runtime, register multi-argument windows and verify final receives no SQL values.


<a id="org67d4f97"></a>

### PY144 [MEDIUM] SQL callback result aliases omit supported result values

Aggregate, scalar, window, FTS, and conversion declarations in `src/apswtypes.py` use `SQLiteValue` where C result dispatch also accepts `zeroblob` and `PyObjectBinding`. Binding conversion is recursively dispatched and can return general `Binding` alternatives, including `CArrayBinding` when enabled. Conversely, C SQL-result dispatch has no C-array result branch.

Valid scalar, aggregate, window, and FTS callbacks produce false-positive type errors, as do valid converted bindings; generated async aliases inherit the restriction.

-   Direction:

Introduce `SQLiteResult = SQLiteValue | zeroblob | PyObjectBinding` and use it for SQL-function result declarations. Define `ConvertBinding` to return `Binding`. Do not include `CArrayBinding` in `SQLiteResult`.

-   Test:

Statically accept `zeroblob` and `pyobject` results for scalar, aggregate, window, and FTS callbacks; accept each enabled binding alternative from `ConvertBinding`; and continue rejecting `CArrayBinding` as an SQL-function result.


<a id="org7e181a4"></a>

### PY145 [MEDIUM] `AsyncConnectionController.send` excludes regular methods returning awaitables

`src/apswtypes.py:203-209` declares `send` as `async def`. Runtime dispatch in `src/async.c:251-259` merely calls `send` and returns its result to the async call path; it does not require that result to originate from an `async def`. The test controller at `apsw/tests/async_meta.py:444-449` uses a regular `def send` that returns a custom awaitable.

Controllers matching the runtime and documented protocol can fail structural type checking.

-   Direction:

Declare `send` as `def send(self, call: Callable[[], Any]) -> Awaitable[Any]`. Async implementations remain compatible because calling them returns a coroutine awaitable.

-   Test:

Statically accept both an `async def send` implementation and a regular method returning `Awaitable[Any]`; reject a regular method returning a non-awaitable. Retain the existing custom-awaitable controller runtime case.


<a id="org5c25aa4"></a>

### PY146 [LOW] `JSONBTypes` models tuples as exactly one element

`src/apswtypes.py:176` uses `tuple["JSONBTypes"]`, which denotes a one-element tuple. `src/jsonb.c:815-840` accepts tuples of arbitrary length as JSON arrays.

Empty and multi-element tuples accepted by runtime JSONB encoding are rejected statically.

-   Direction:

Change the tuple branch to `tuple["JSONBTypes", ...]`.

-   Test:

Statically accept empty, single-element, and multi-element nested tuples. Encode each shape and assert decoding preserves its cardinality and content under the selected array hook.


<a id="orge3d8592"></a>

### PY147 [LOW] `TokenizerResult` includes malformed empty tuple forms

`src/apswtypes.py:137-141` permits `tuple[str, ...]`, including `()`, and offset tuples with only two integers. `src/fts.c:534-550,573-580` rejects empty tuples and requires offset tuples to contain start, end, and at least one token string. The precise non-empty tuple syntax must remain compatible with the project’s Python 3.10 target and supported checker matrix.

Malformed tokenizer output can pass static checking but raises `ValueError` during FTS tokenization.

-   Direction:

Use precise non-empty tuple aliases where every supported checker accepts them; otherwise use compatible helper aliases or document the unavoidable broader static contract rather than claiming invalid forms are excluded.

-   Test:

Cover valid `("token",)`, colocated-token, and offset-token forms. Where expressible, statically reject `()` and `(0, 5)`; at runtime assert those forms fail and valid forms succeed.


<a id="org36ed39c"></a>

### PY148 [LOW] `WindowFactory` excludes runtime-supported list results

`src/apswtypes.py:101-103` permits only the tuple factory form. `src/connection.c:3473-3499` explicitly accepts either a tuple or list, and `src/connection.c:3746-3750` documents a sequence. Existing tests exercise list-returning factories on negative paths for bad lengths and callback elements.

A list-returning factory accepted by runtime and documentation is rejected by static checking.

-   Direction:

Add a list alternative while documenting the necessary broader element type: typing cannot precisely represent the heterogeneous fixed-length list. Change the C documentation's broader “sequence” wording to “tuple or list”; do not broaden typing to arbitrary `Sequence` because runtime accepts tuple and list specifically.

-   Test:

Statically accept tuple- and list-returning factories. Execute both successfully, retain runtime-negative coverage at `apsw/tests/__main__.py:2810-2830,2832-2845` for bad lists, and assert a custom `Sequence` remains rejected.


<a id="orga83ea91"></a>

## tools/aio<sub>bench.py</sub>


<a id="orgded7de8"></a>

### PY149 [MEDIUM] APSW and aiosqlite use different transaction regimes

Evidence: \`apsw<sub>bench</sub>()\` uses \`contextlib.aclosing(await apsw.Connection.as<sub>async</sub>(":memory:"))\` and performs setup and repeated \`executemany()\` calls without an explicit \`BEGIN\`; APSW therefore uses autocommit. \`aiosqlite<sub>bench</sub>()\` uses the default \`aiosqlite.connect()\` transaction configuration, whose first loop \`executemany()\` implicitly opens a transaction that is never committed. \`aclosing()\` does not enter APSW's async transaction context manager.

Impact: The benchmark compares different SQLite transaction costs rather than isolating async iteration, prefetching, and worker-thread overhead. This affects benchmark validity, not durability, because both databases are in-memory and discarded.

-   Direction: Explicitly configure the same transaction policy on both paths. For example, issue \`BEGIN\` on both connections immediately before the timed loop, or explicitly configure aiosqlite for autocommit if per-statement transactions are intentional.

-   Test: Run a reduced workload and verify that `not await apsw_connection.get_autocommit()` equals aiosqlite's underlying `in_transaction` before and after the first insert batch; use SQL tracing to confirm equivalent `BEGIN=/=COMMIT` profiles and verify equal final row counts.


<a id="org64fa448"></a>

### PY150 [MEDIUM] CPU component columns combine incompatible clocks

Evidence: \`get<sub>times</sub>()\` combines \`time.process<sub>time</sub>()\` with \`resource.getrusage(resource.RUSAGE<sub>THREAD</sub>).ru<sub>utime</sub>\`. \`show()\` labels the latter-derived value \`CpuEvtLoop\` and computes \`CpuDbWorker\` as whole-process CPU minus event-loop-thread user CPU. Process CPU includes user and system time for all threads, while \`ru<sub>utime</sub>\` includes only user time for the current thread. The unconditional \`resource\` import also prevents importing the script on Windows.

Impact: \`CpuEvtLoop\` omits event-loop system CPU, while \`CpuDbWorker\` includes that omitted system CPU and any CPU consumed by unrelated threads. The total remains valid process CPU, but the advertised split is inaccurate and unnecessarily non-portable.

-   Direction: Replace the \`resource\` measurement with \`time.thread<sub>time</sub>()\`, which is compatible with `process_time()`'s user-plus-system accounting and removes the Windows-incompatible dependency. Rename the residual column to \`CpuOtherThreads\` unless the benchmark directly instruments and guarantees a single database worker thread.

-   Test: Inject deterministic process and thread clock values and assert the subtraction. Add an import smoke test with `resource` unavailable, preferably on native Windows. In a smoke benchmark, verify \`CpuTotal\` approximately equals \`CpuEvtLoop + CpuOtherThreads\`; add unrelated CPU work in another thread and ensure it is not represented as database-worker-only.


<a id="org77fc686"></a>

### PY151 [MEDIUM] Direct uvloop modes fail on Python 3.10 and 3.11

Evidence: When \`uvloop\` imports, the script enables direct \`AsyncIO uvloop\` and \`aiosqlite uvloop\` modes, then calls \`asyncio.run(&hellip;, loop<sub>factory</sub>=uvloop.new<sub>event</sub><sub>loop</sub>)\`. \`loop<sub>factory</sub>\` was added to \`asyncio.run()\` in Python 3.12, while the repository supports Python 3.10 onward. The AnyIO uvloop path uses AnyIO backend options and is not affected.

Impact: On Python 3.10 or 3.11 with uvloop installed, the first direct uvloop mode raises \`TypeError\`, terminating the benchmark before remaining modes and prefetch values run; constructing the coroutine before the failed call can also emit an unawaited-coroutine warning.

-   Direction: Use a uvloop runner compatible with the supported Python range, implement a correctly cleaned-up explicit event-loop helper for older Python versions, or version-gate direct uvloop modes with a clear skip message.

-   Test: With uvloop installed, run a reduced benchmark on Python 3.10, 3.11, and 3.12+. Require rows when a compatibility runner is implemented, or a clear skip for version-gated direct modes; neither path may produce `TypeError` or unawaited-coroutine warnings, and the AnyIO uvloop mode must remain functional.


<a id="org8a69818"></a>

## tools/checksums.py


<a id="org5288365"></a>

### PY152 [LOW] Reported checksum mismatches exit successfully

Evidence: In \`check()\`, an existing checksum entry that differs from the downloaded length, SHA-256, or SHA3-256 only produces \`print()\` diagnostics. Neither \`check()\` nor the top-level download loops return failure state or call \`sys.exit()\`, so completed downloads with one or more mismatches end with status zero. Audited build and release paths use \`setup.py\`'s separate downloader, which raises on checksum mismatch; this tool is not demonstrated as a release-publication gate.

Impact: Maintainers and ad hoc automation using this standalone helper can treat a reported mismatch as success, for example by continuing after \`tools/checksums.py && next-command\`. This is a maintenance-tool status defect, not a demonstrated release bypass.

-   Direction: Refactor execution into \`main()\`, aggregate mismatch results, and exit nonzero after reporting all mismatches. Keep missing checksum entries distinct from failures, preferably behind an explicit generation mode; send failure diagnostics to stderr.

-   Test: Mock downloading and verify: matching data exits zero; altered data for an existing entry exits nonzero and identifies the URL; all mismatches are reported before the final nonzero exit; missing entries follow the explicit generation policy; malformed checksum data and network failures remain nonzero.


<a id="orgdb111e5"></a>

### PY153 [LOW] Downloads have no tool-controlled time or resource bounds

Evidence: Both download loops call \`urllib.request.urlopen(url).read()\` without a timeout or read-size limit. Each complete response is retained in memory while its length, SHA-256, and SHA3-256 are calculated.

Impact: A stalled or continuously trickling endpoint can block a checksum refresh for an uncontrolled duration, and an unexpectedly large response can consume excessive memory. The URLs are fixed release archives and the tool is a standalone maintainer helper, so the practical impact is limited; this is not evidence that invalid content can pass verification.

-   Direction: Add configurable finite per-operation network timeouts plus an overall elapsed-time deadline, use response context managers, and hash incrementally in bounded chunks while counting bytes. If a maximum size is enforced, make it configurable and do not rely solely on `Content-Length`.

-   Test: With an injected opener or controlled local server, verify stalled and continuously trickling responses both exceed their respective bounds; verify chunked hashing produces the same byte count and digests as one-shot data; if a maximum is part of the chosen policy, verify an over-limit response fails without retaining the whole body; and verify response closure and useful URL diagnostics.


<a id="org66e3519"></a>

## tools/checkversion.py


<a id="org0c57aa8"></a>

### PY154 [LOW] Minimum SQLite version is selected lexicographically

Evidence: \`vers["megatest SQLITEVERS"] = convert<sub>to</sub><sub>number</sub>(min(megatest.SQLITEVERS))\` applies \`min()\` to version strings. For example, lexical ordering selects \`"3.53.10"\` before \`"3.53.9"\`, although \`3.53.9\` is the lower SQLite version. Current matrix values `3.53.0` through `3.53.4` do not trigger the defect.

Impact: A future release consistency check can choose the wrong matrix minimum, either failing a consistent declaration or passing a declaration that excludes an actually tested older version.

-   Direction: Parse versions before finding the minimum, such as \`min(convert<sub>to</sub><sub>number</sub>(version) for version in megatest.SQLITEVERS)\`, preferably using a shared strict version parser.

-   Test: Use a fixture with \`SQLITEVERS = ("3.53.9", "3.53.10")\`. Verify \`3.53.9\` is selected, matching C and documentation declarations pass, and declarations of \`3.53.10\` fail while \`3.53.9\` remains in the matrix.


<a id="org35b7a2f"></a>

### PY155 [LOW] Version normalization permits collisions and relies on removable assertions

Evidence: \`convert<sub>to</sub><sub>number</sub>()\` encodes components as \`major \* 1000000 + minor \* 1000 + patch\`, but does not require minor and patch values below 1000. Thus \`"3.52.1000"\` and \`"3.53.0"\` both normalize to \`3053000\`. Its major-version and component-count checks are \`assert\` statements, which disappear under \`python -O\`; optimized execution can silently accept a fourth component such as \`"3.53.0.1"\` and ignore it.

Impact: A malformed textual declaration can normalize to the same value as a different valid declaration and let the consistency gate pass. Inputs are committed maintainer-controlled text and current declarations are valid, so this is a low-severity release-check weakness.

-   Direction: Replace assertions with explicit validation. Require exactly two or three decimal components, the intended SQLite major version, and minor and patch values below 1000; produce a diagnostic that identifies the malformed source declaration.

-   Test: Verify \`"3.52.1000"\` is rejected rather than equated with \`"3.53.0"\`; verify \`"3.53.0.1"\` and an unintended major version are rejected under both normal Python and \`python -O\`; verify \`"3.53"\` and \`"3.53.0"\` remain equivalent; and test valid boundary components against adjacent minor versions.


<a id="orgec75114"></a>

## tools/coverageanalyser.py


<a id="orgb1bf955"></a>

### PY156 [LOW] Fault-injection exclusion misses current conditional form

Evidence: The parser starts exclusion only for the exact line \`#ifdef APSW<sub>FAULT</sub><sub>INJECT</sub>\` and ends it at the first \`#else\` or \`#endif\`, without nested-preprocessor tracking. \`src/unicode.c\` uses \`#if defined(APSW<sub>FAULT</sub><sub>INJECT</sub>) && PY<sub>VERSION</sub><sub>HEX</sub> >= 0x030c0000\` around fault-injection support, including the executable \`OBJ()\` helper. The coverage build defines \`APSW<sub>FAULT</sub><sub>INJECT</sub>\` and compiles \`unicode.c\`.

Impact: On Python 3.12 and later, fault-injection-only executable source can be included in \`<sub>unicode</sub>\` coverage totals. The presently direct distortion is small—principally the helper rather than the separately filtered generated fault-injection source—but the metric is technically inaccurate.

-   Direction: Recognize both \`#ifdef APSW<sub>FAULT</sub><sub>INJECT</sub>\` and \`#if defined(APSW<sub>FAULT</sub><sub>INJECT</sub>) &hellip;\` forms, and track preprocessor nesting so only the intended fault-injection branch is excluded and an outer production \`#else\` resumes inclusion.

-   Test: Use synthetic \`.gcov\` fixtures covering exact \`#ifdef\`, \`#if defined(&hellip;)\`, nested conditionals, and an outer \`#else\`. Assert fault-injection executable lines are omitted, nested directives do not terminate exclusion, and production-branch lines remain counted. Check a Python 3.12+ artifact to ensure the \`OBJ()\` region no longer affects \`<sub>unicode</sub>\` totals.


<a id="org83fc80b"></a>

### PY157 [LOW] Gcov files are decoded using the ambient locale

Evidence: \`.gcov\` files are opened with \`open(f, "rt")\` without an explicit encoding. Gcov textual output includes copied source text, and current included source contains the UTF-8 snowman character in \`src/jsonb.c\`; \`coverage.sh\` generates and analyzes these text artifacts without setting Python text encoding.

Impact: In a strict non-UTF-8 Python locale, valid current coverage artifacts can raise \`UnicodeDecodeError\` and abort analysis. Normal modern UTF-8 Unix environments are unaffected; the concrete risk is decoding failure, not demonstrated misrecognition of directives.

-   Direction: Open \`.gcov\` files explicitly as UTF-8 using \`open(f, "rt", encoding="utf-8")\`. Preserve strict decoding so malformed artifacts are not silently concealed.

-   Test: Create a \`.gcov\` fixture containing the UTF-8 snowman and run the analyzer in a subprocess with Python UTF-8 mode and locale coercion disabled under a strict non-UTF-8 default encoding. Assert successful parsing and correct totals; separately assert that invalid UTF-8 fails.


<a id="org760d0b7"></a>

### PY158 [LOW] Gcov exceptional zero-count marker is counted as executed

Evidence: The analyzer ignores only \`-\` and considers every count other than \`#####\` executed. GNU gcov's \`=====\` marker is a valid unexecuted exceptional-block marker, so it is added to both \`file<sub>exec</sub>\` and \`file<sub>total</sub>\`. The coverage wrapper invokes GNU gcov, or \`llvm-cov gcov\`, with default textual output options.

Impact: Whenever a supported coverage artifact contains \`=====\`, each such line is falsely reported as covered and inflates the percentage. No committed artifact establishes that the current C-only workflow emits this marker, so present numerical inflation is unproven.

-   Direction: Treat numeric zero, \`=====\`, and \`#####\` as total-only; treat positive numeric counts as executed. Prefer positive parsing of supported count tokens, including deliberately supported decorated numeric forms such as \`1\*\`, rather than treating every non-\`#####\` token as executed.

-   Test: Run a textual \`.gcov\` fixture containing \`-\`, \`#####\`, \`=====\`, zero/positive numeric counts, and \`1\*\`. Assert exact executed and total line sets, including that zero and both unexecuted markers are total-only; cover supported GNU gcov and \`llvm-cov gcov\` output dialects where their conventions differ.


<a id="orge70c1b7"></a>

## tools/code2rst.py, tools/genconstants.py, tools/gendocstrings.py


<a id="org99e88fb"></a>

### PY159 [LOW] Shared TOC URL handling requires a trailing slash and lacks a finite timeout

All three TOC consumers concatenate `SQLITE_URL` with `"toc.db"` and use
`urlopen(...).read()` without an explicit timeout.  Natural no-slash roots form incorrect URLs, and a stalled server can hang generation.  This is a reliability
issue, not a hostile-endpoint security finding.

-   Direction: use one helper that normalizes directory URL semantics, preserves
    nested paths, and applies a documented finite timeout.
-   Test: with a local HTTP fixture, verify slash/no-slash roots request the same TOC resource and generate identical function and constant links; require a nonresponding server to fail within the configured timeout.


<a id="orgc6d0b6e"></a>

### PY160 [LOW] Open temporary TOC databases may not reopen on native Windows

Each tool keeps a default `NamedTemporaryFile` open while APSW reopens its
pathname as a SQLite database.  Native Windows sharing and delete-on-close
semantics may prevent that reopen or cleanup.  This concerns maintainer
portability; native Windows execution was unavailable during the audit.

-   Direction: write to an ordinary temporary path, close it before APSW opens
    it, close APSW explicitly, and unlink in `finally`.
-   Test: on native Windows CI, run all three generators against a local valid
    `toc.db` and verify open and cleanup on success and injected failure.


<a id="org98614d8"></a>

## tools/code2rst.py


<a id="orgbda7fd7"></a>

### PY161 [MEDIUM] Shared docdb updates race and broad exception silently resets data

Every class/module flush reads shared `doc/docdb.json`, treats any load failure
as an empty object, mutates it, and writes it directly.  Ten Make targets can do
this concurrently, losing entries; malformed or partially rewritten JSON is
also discarded and overwritten.  Downstream docstring and stub generation can
therefore omit APIs.

-   Direction: write per-source fragments and merge once, or lock the entire
    read/merge/atomic-replace transaction.  Treat only a missing initial file as
    empty and preserve malformed bytes on failure.
-   Test: coordinate concurrent starts before lock acquisition and require serialized completion plus the final union; test fragment merging separately if that design is selected. Seed malformed JSON and require unchanged bytes plus failure.


<a id="org404e26d"></a>

### PY162 [LOW] Output publication is non-atomic and path-collision unsafe

The script removes its output before download, parsing, and validation, then
uses direct `Path.write_text()`.  Normal Make outputs are regenerable, but
direct invocation can delete its input when paths coincide, overwrite the
docdb when that path collides, or leave a partial destination on write failure.

-   Direction: resolve and reject all input/output/docdb collisions before
    mutation, then publish complete RST through a sibling temporary and
    `os.replace()`.
-   Test: exercise all three pairwise input/output/docdb collision cases and an injected final-write failure, requiring controlled failure and unchanged original destinations.


<a id="org2af0989"></a>

### PY163 [LOW] Consecutive SQLite-call markers retain only the final RST marker

Each `-* ...` line overwrites the prior `indexop, saop`.  Current
`ChangesetBuilder.add_insert`, `add_delete`, and `add_update` consequently retain
only `sqlite3changegroup_change_finish` while omitting
`sqlite3changegroup_change_begin` from generated RST.  The raw markers enter
the docdb before this conversion, so runtime docstrings/stubs are not affected
by this particular defect.

-   Direction: accumulate calls across all marker lines and render one combined
    index/call block, or reject multiple markers explicitly.
-   Test: use two markers in method and constructor fixtures; both calls must
    occur exactly once in the RST index and `Calls:` block.


<a id="org3243f31"></a>

### PY164 [LOW] Bad CLI arity handling continues and the SQLite-version argument is dead

Bad argument counts only print a message.  Missing arguments then raise
`IndexError`, while extra arguments allow generation to continue.  The required
SQLite-version positional value is assigned but unused; that is a vestigial
interface, not proof of wrong-version output.

-   Direction: use `argparse` or exit status 2 immediately; remove the dead
    positional argument from the script and Make recipe unless given a contract.
-   Test: missing and extra arguments must return 2 without network or file
    mutation; valid invocation must still work.


<a id="org512d809"></a>

### PY165 [LOW] Malformed parser state enters interactive pdb

Pending methods followed by an unexpected non-method documentation block
execute `pdb.set_trace()`.  Make or CI can hang or report a debugger-specific
failure instead of the source structure error.

-   Direction: raise a deterministic parser exception naming input, line, class,
    pending methods, and unexpected directive.
-   Test: process a malformed fixture noninteractively with a timeout; require
    prompt-free, prompt failure with the full parser-state diagnostic.


<a id="orgcecf2aa"></a>

## tools/genconstants.py


<a id="orgef91e85"></a>

### PY166 [LOW] Reverse mappings select incidental sentinel aliases

Bidirectional entries overwrite duplicate integer keys in lexical name order.
For SQLite 3.53.4, `SQLITE_DBCONFIG_FP_DIGITS` and sentinel
`SQLITE_DBCONFIG_MAX` both equal 1023, so reverse lookup prefers the sentinel.
Forward mappings remain correct; reverse alias selection is accidental.

-   Direction: retain every name-to-value entry but define one explicit preferred
    reverse name per value, avoiding `*_MAX` where an operational alias exists.
-   Test: after regeneration, require reverse lookups to prefer
    `SQLITE_DBCONFIG_FP_DIGITS` and `SQLITE_DBSTATUS_TEMPBUF_SPILL` while all
    forward aliases remain.


<a id="org67f1b8c"></a>

### PY167 [MEDIUM] Live TOC metadata is not tied to the pinned SQLite version

The project pins SQLite 3.53.4, but the generator downloads mutable unversioned
`toc.db` without checking it against the pinned headers.  Current generated C
agrees with 3.53.4, but future live metadata can change output at the same commit
or reference newer macros.  Make also removes/truncates `src/constants.c`
before successful generation.

-   Direction: use frozen or digest-verified metadata tied to `SQLITEVERSION` and
    atomically replace generated C only after validation.
-   Test: require byte-identical output from a frozen fixture, reject metadata
    containing a newer macro before publication, and preserve the old target on
    download/validation failure.


<a id="orga5ceddb"></a>

## tools/gendocstrings.py


<a id="org7e56e0b"></a>

### PY168 [MEDIUM] Multi-call expansion deletes following documentation

A multi-call marker expands to several lines, but `classify()` replaces
`doc[n:n + len(lines)]` rather than the one marker line.  Current
`Cursor.execute` loses its following `seealso`, example, and execution-model
links in generated `apsw/__init__.pyi` and runtime docstrings.

-   Direction: replace exactly `doc[n:n + 1]` and advance over inserted lines.
-   Test: put a multi-call marker before `seealso` and prose, require all following
    text to survive, and verify regenerated `Cursor.execute` links.


<a id="org5fc00fa"></a>

### PY169 [LOW] Generated constant docstrings contain malformed RST links

Constant docstrings are emitted as `` `title <URL>'__ ``, with an apostrophe instead
of the closing backtick.  The generated stub contains 423 instances, impairing
stub-aware documentation/IDE rendering without affecting constant values.

-   Direction: emit `` `title <URL>`__ `` at the generator.
-   Test: assert exact well-formed output for a representative constant and parse
    it with an RST parser.


<a id="org1ae9df2"></a>

### PY170 [MEDIUM] Validation failures can leave source and artifacts partly updated

Argument-parser blocks in primary C sources are rewritten while processing is
ongoing; `src/apsw.docstrings` is written before unreferenced-symbol validation;
and the stub is generated afterward.  A late failure can leave these outputs
inconsistent, while direct writes can truncate on I/O failure.

-   Direction: compute and validate every update first, then stage all destinations through sibling temporaries. If only pre-publication failures are covered, document that guarantee; otherwise publish with rollback/recovery across replacement boundaries because sequential `os.replace()` calls are atomic only per file.
-   Test: inject a late validation/signature failure after an earlier rewrite candidate and require all source/output bytes unchanged; inject staging and each replacement-boundary failure, verifying the documented rollback/recovery guarantee.


<a id="org54bf3f8"></a>

### PY171 [LOW] Unsupported signatures can invoke an interactive debugger

Unsupported argument type/default combinations and an attribute-docstring error
path call `breakpoint()`.  Current inputs avoid these branches, but future
maintenance edits can make noninteractive generation stop in a debugger.

-   Direction: raise descriptive generator exceptions including item, signature,
    parameter, type, and default.
-   Test: route unsupported parameter and empty attribute-doc fixtures through the
    branches with default and disabled `PYTHONBREAKPOINT`; require identical,
    prompt-free failure and no file changes.


<a id="org9fff8a2"></a>

### PY172 [MEDIUM] C block replacement uses ambiguous substring and brace heuristics

Replacement locates a symbol by substring, remembers the latest standalone
opening brace, and stops at a standalone closing brace.  Current generated
blocks fit the implicit shape, but ordinary future comments, duplicate symbol
mentions, or brace-layout edits can select the wrong region of a primary C
source file.

-   Direction: use exact symbol-specific generated-region sentinels and require
    exactly one complete region before writing.
-   Test: cover duplicate mentions, comments/strings containing the symbol, and
    nearby/nested brace blocks; replace only the sentinel region or fail unchanged.


<a id="orgaa0caca"></a>

## tools/docmissing.py


<a id="org741a74c"></a>

### PY173 [MEDIUM] Public APSW types are excluded without member audit

A handwritten set excludes documented public extension types such as
`URIFilename`, `Backup`, `IndexInfo`, `FTS5Tokenizer`, `ChangesetBuilder`, and
`Rebaser` from member comparison.  `stubtest` can catch some drift, but the
documentation target does not enforce it and stubs derive from documentation.
The exact current omissions that this gap conceals remain runtime-qualified.

-   Direction: maintain one authoritative table covering every inspectable public
    type, with explicit handling for non-runtime protocols.
-   Test: inject documented-missing and present-undocumented members and require
    every excluded public type to be audited or explicitly exempted.


<a id="org761be96"></a>

### PY174 [LOW] Missing documented members abort before the intended diagnostic

`inspect.getattr_static(obj, c)` has no default, so an absent documented member
raises `AttributeError` before the intended diagnostic and status aggregation.
The checker still fails closed, but hides later discrepancies behind a traceback.

-   Direction: use a unique sentinel or catch only `AttributeError`, then emit and
    aggregate the intended diagnostic.
-   Test: include one absent documented member and another discrepancy; require
    both messages, no traceback, and nonzero status.


<a id="org9ebd323"></a>

### PY175 [LOW] Virtual-table protocols are validated only by member count

`VTModule`, `VTTable`, and `VTCursor` are checked only against counts 3, 17,
and 7.  Same-count substitution or duplicate-plus-omission passes.  This also
masks a current mismatch: `VTTable.RollbackTo` is installed in
`src/vtable.c:2179-2214,2821-2824` but its comment is not a documentation
comment, while `tools/code2rst.py:224-232` mis-maps ordinary `Rollback` to the
`xRollbackTo` reference.

-   Direction: restore and generate `RollbackTo` documentation, correct the `Rollback` link mapping, and compare exact unique names against the independently maintained 18-member `VTTable` callback contract and corresponding exact contracts for the other protocols.
-   Test: cover same-count substitution, duplicate-plus-omission, missing protocol, and the exact current valid sets, including distinct `Rollback` and `RollbackTo` entries and links.


<a id="org453f427"></a>

### PY176 [LOW] Pragma consistency checks disappear under optimized Python

Bare assertions perform the shell/SQLite pragma consistency checks.  Under
`-O` they vanish, allowing drift to pass.  Normal Make invocation is not
optimized, and no current mismatch is claimed.

-   Direction: replace assertions with explicit checks that aggregate diagnostics
    and return nonzero.
-   Test: run mismatch fixtures normally and with `-O`; both must diagnose and
    fail.


<a id="org010fb49"></a>

## tools/docupdate.py


<a id="org5800369"></a>

### PY177 [LOW] Marker replacement can truncate documents

Replacement loops suppress lines while marker-state flags remain active.  A
missing close suppresses subsequent content, potentially to EOF, although a
later unrelated end marker can also clear the shared state incorrectly;
missing, duplicate, and reversed markers are not diagnosed.  Current pairs are
valid, so this is a future template-regression hazard.

-   Direction: validate every expected marker exactly once and in order before
    writing any file.
-   Test: cover missing open/close, duplicates, and reversal, requiring a
    marker-specific error and unchanged input.


<a id="org6508bdd"></a>

### PY178 [LOW] Existing literal-block markers become triple-colon output

The help converter appends a colon to every colon-ending paragraph before an
indented block.  Existing `Example::` therefore becomes `Example:::` in current
shell/CLI RST.  This is noncanonical and may visibly double punctuation, though
it is not necessarily invalid RST.

-   Direction: convert only a single trailing colon.
-   Test: require `Example:` to become `Example::`, existing `Example::` to remain
    unchanged, and regenerated docs to contain no unintended `:::`.


<a id="org3a73cca"></a>

### PY179 [LOW] Colon lookahead can raise IndexError

`long_help_to_rst()` skips blanks after a colon-ending paragraph without bounds.
`["Options:"]` and `["Options:", ""]` raise `IndexError`.  Current help does
not hit the boundary, but valid future help text can abort generation.

-   Direction: bound the initial lookahead and blank-skipping loop.
-   Test: cover terminal colons, trailing blanks, following indented content, and
    a non-colon terminal paragraph.


<a id="org78d9bf7"></a>

## tools/example2rst.py


<a id="orgb74021b"></a>

### PY180 [MEDIUM] Final example-section output is never emitted

Captured output is rendered only when a later section marker appears; there is
no final flush.  Current `examples/async.py` ends with `async_session` and
prints size output, which is therefore omitted from generated RST.

-   Direction: render output before each section switch and once after the loop.
-   Test: use a final printing section and require one output block; require no
    empty block for a silent final section.


<a id="orge2e2892"></a>

### PY181 [MEDIUM] Temporary publication is non-atomic, mode-changing, and Windows-sensitive

The tool copies from an open `NamedTemporaryFile` to the destination.  A copy
failure can truncate the destination, POSIX output inherits restrictive 0600
mode, and native Windows may reject reopening the delete-on-close temp path.
Generation failures happen before copy and do preserve old output.

-   Direction: write a sibling temporary, close it, set the intended mode, and
    publish with `os.replace()`.
-   Test: inject publication failure and preserve the old output; verify POSIX
    mode and execute on native Windows.


<a id="org7bac297"></a>

### PY182 [MEDIUM] Locale-dependent encoding breaks current example generation

Source reads and RST writes omit encoding.  Current examples include Vietnamese
and extensive other Unicode; non-UTF-8 locales can fail decoding/encoding or
produce non-UTF-8 documentation.

-   Direction: read Python with `tokenize.open()` or explicit UTF-8 and write RST
    explicitly as UTF-8.
-   Test: generate a Unicode fixture with UTF-8 mode disabled under a non-UTF-8
    locale and require exact UTF-8 output.


<a id="org7a35078"></a>

### PY183 [LOW] gen<sub>rst</sub> flushes an accidental global instead of its output stream

`gen_rst(filename, outfile, output)` writes to `outfile` but flushes global `f`.
The CLI happens to bind that same object; independent use raises `NameError` or
flushes the wrong stream.

-   Direction: flush `outfile` and close it before publication.
-   Test: invoke the helper with an independently named stream and no global `f`.


<a id="org5073201"></a>

## tools/fi.py


<a id="org1403f21"></a>

### PY184 [HIGH] Python 3.12+ suppresses every named APSW<sub>FAULT</sub> injection

`should_fault()` requires `pending_exception =` (None, None, None)=, but on
Python 3.12+ C passes `(None,)` for no pending exception.  Named faults always
return false before control dispatch, while generic faults can still make the
campaign appear active.

-   Direction: normalize version-specific representations or pass a stable
    `has_pending_exception` boolean.
-   Test: on every supported minor, trigger a named fault with/without a pending
    exception and require the no-pending C bad branch to execute.


<a id="orga234e51"></a>

### PY185 [MEDIUM] Multi-fault exception verification is under-constrained

Observed classes are checked globally and only their count is generally matched
to tested faults.  One matching outcome plus unrelated/context exceptions can
cover another key.  Context chains can be legitimate; the defect is missing
per-key association.  A current full-campaign false pass was not reproduced.

-   Direction: associate allowable exception or non-exception outcomes with each
    injected key and match nested contexts explicitly.
-   Test: synthetic two-key plans must reject one match plus one unrelated result,
    accept two key-matched outcomes, and validate contexts per key.


<a id="orga6cbe69"></a>

### PY186 [MEDIUM] Fixed /tmp/fitesting database escapes managed run isolation

The harness creates a managed temporary directory but opens the real-file test
database at fixed `/tmp/fitesting`.  It survives cleanup, affects later runs,
collides across concurrent campaigns, and is nonportable.

-   Direction: put the database beneath the managed directory and close it before
    directory cleanup.
-   Test: preserve an unrelated fixed-path sentinel, run campaigns concurrently
    with distinct paths, and require no database sidecars after completion.


<a id="orge45cbd2"></a>

### PY187 [LOW] Emergency traceback printing raises a secondary TypeError

The custom `print()` always supplies `file=real_stdout`, while the emergency
path also passes `file=sys.stderr`.  The duplicate keyword raises `TypeError`
and obscures the original exception.

-   Direction: preserve real stderr and use the original print directly for
    emergency output.
-   Test: trigger the emergency path under redirection and require the original
    traceback with no secondary exception.


<a id="org8c5f634"></a>

### PY188 [MEDIUM] Leak detection can block or continue instead of failing

A nonempty leak result calls `input()` inside an exception-suppressing context.
CI can hang, EOF may be suppressed, and supplied input allows continuation
despite the leak.

-   Direction: fail outside the suppressing context; make interactive inspection
    explicit opt-in.
-   Test: with closed stdin and a never-writing pipe, a mocked leak must terminate
    promptly and nonzero without a prompt.


<a id="orgc4cb7e7"></a>

### PY189 [LOW] Unknown faults enter an interactive debugger

An unclassified fault reaches `breakpoint()` from the C callback path, causing
terminal- and environment-dependent prompts or secondary errors.

-   Direction: record a fatal harness error and report it outside callback and
    suppression boundaries; permit debugging only by explicit option.
-   Test: pass an unknown key through the real callback with no TTY and differing
    `PYTHONBREAKPOINT` settings; require prompt-free nonzero failure naming it.


<a id="org573361b"></a>

### PY190 [LOW] Missing example name raises NameError while building its diagnostic

For an unmatched example, an f-string references undefined `pattern` before
`sys.exit()` executes, yielding a traceback instead of a controlled error.

-   Direction: retain the glob pattern and report through `argparse`.
-   Test: select a nonexistent example and require status 2, a useful name/glob,
    no traceback, and no campaign side effects.


<a id="orgd481463"></a>

## tools/gencompilecommands.py


<a id="org986dd69"></a>

### PY191 [MEDIUM] Compilation entries pair file values with another translation unit

Entries are emitted for every `src/*.c`, but commands compile only `src/apsw.c`
or `src/unicode.c`.  Most mismatches are included implementation fragments;
`testextension.c` and `fileio_win32.c` are separately built helpers.  Most entries therefore have a JSON `file` field that disagrees with the
command's translation unit, misleading indexers and diagnostics; the entries
for `src/apsw.c` and `src/unicode.c` themselves agree.

-   Direction: emit only real extension translation units, plus accurate helper
    commands if whole-project scope is intended.
-   Test: parse JSON and require each `file` to equal the command source operand;
    verify helper entries have their actual configuration.


<a id="orgb037a52"></a>

### PY192 [MEDIUM] Compound compiler commands are emitted as one executable argument

`CC` is inserted as one element while flags are split.  A value such as
`ccache gcc -pthread` becomes one nonexistent executable name.

-   Direction: tokenize `CC` correctly per platform, preferably deriving argv
    from the compiler object used by the real build.
-   Test: require compound/quoted compiler configurations to produce correct argv.


<a id="orge1a8415"></a>

### PY193 [MEDIUM] Standard Windows Python configurations are unsupported

The generator assumes Unix `CC`, `CFLAGS`, and `CCSHARED` variables. Standard Windows CPython/MSVC configurations can leave them absent; on Python 3.12+ `shlex.split(None)` raises `ValueError`, while older behavior can read standard input and still leaves no compiler executable.

-   Direction: derive commands from the actual platform compiler, including MSVC
    syntax, or fail with an explicit unsupported-platform diagnostic.
-   Test: run on native Windows and unit-test absent sysconfig variables.


<a id="org54351de"></a>

## tools/genfaultinject.py


<a id="org0177fdf"></a>

### PY194 [MEDIUM] Fault inventory omits actively used faultable Python APIs

Generated wrappers omit active `PyDict_Copy`, `PyUnicode_Substring`,
`_PyObject_NewVar`, `PyLong_AsUnsignedLongLong`, `PyLong_AsSsize_t`, and
`Py_AddPendingCall` paths.  The campaign can report completion without testing
their failure handling.  `PyLong_AsUnsignedLong` has no active audited call and
is not part of this finding.

-   Direction: add active APIs to the correct categories and update `fi.py`
    expectations for the caller-translated `Py_AddPendingCall` failure.
-   Test: require wrappers only for active APIs, build the fault variant, and
    verify every new site is reached.


<a id="orgde83027"></a>

### PY195 [LOW] Regeneration bypasses the configured Python

The shebang is `/usr/bin/python` and Make invokes the script directly rather
than via `$(PYTHON)`.  Regeneration can fail or use a different environment;
the generated header is checked in, limiting impact.

-   Direction: invoke `$(PYTHON) tools/genfaultinject.py ...` and optionally use an
    `env python3` shebang for direct use.
-   Test: regenerate with explicit `PYTHON` where `/usr/bin/python` is absent and
    require the committed output.


<a id="org8cfde4c"></a>

### PY196 [LOW] nm can interpret the inspected filename as an option

The command is `["nm", "-u", fname]`, so an option-like pathname can be
interpreted by `nm`.  This is operand mishandling, not shell injection.

-   Direction: pass an absolute pathname or a supported `--` terminator.
-   Test: inspect a real object whose basename begins with `-` and require its
    symbols to be processed; missing paths must fail clearly.


<a id="org3fcb126"></a>

### PY197 [LOW] Generated-header publication is non-atomic

The generator writes directly while Make removes the previous
`src/faultinject.h`.  Interruption or I/O failure can leave a partial target
newer than prerequisites.

-   Direction: render, write/close a sibling temporary, then `os.replace()`; stop
    pre-deleting the valid header.
-   Test: inject short-write, ENOSPC, and close failures and preserve the old file.


<a id="org1491b3a"></a>

## tools/gensqlitedebug.py


<a id="org986a6b6"></a>

### PY198 [HIGH] Broad session patterns suppress required mutex wrappers

A broad `sqlite3change(group|set)_.*` rule suppresses wrapper generation for all four apply APIs and shadows the specific `sqlite3changegroup_schema` rule; the sole intended specific v3 apply rule is also misspelled. Generated debug C therefore lacks all five wrappers. The apply call sites hold `dbmutex` but lack generated enforcement. `ChangesetBuilder_schema()` and later schema-backed builder operations do not acquire or route through the mutex/async worker; close and deallocation acquire the mutex independently.

-   Direction: narrow/reorder rules, fix spelling, generate the correct argument checks, and validate classification overlap. Separately serialize or reject schema-backed builder work through the connection's mutex/async-affinity policy with coherent cleanup.
-   Test: unit-test all five classifications and run debug threaded builder
    schema/add/close coverage that detects missing acquisition.


<a id="org555695b"></a>

### PY199 [LOW] Generated macros evaluate the checked argument twice

A protected argument appears in the mutex assertion and SQLite call.  No
current side-effecting audited operand was found, but a future expression can
be evaluated twice and check a different object from the call.

-   Direction: capture the operand once in a temporary and reuse it.
-   Test: compile a side-effecting operand fixture and require exactly one
    evaluation with identical checked/called pointers.


<a id="orgfa0b56f"></a>

### PY200 [LOW] git grep errors are treated as ordinary absence

Any nonnegative return code is accepted, although Git codes above 1 are errors.
Repository/pathspec failures can therefore masquerade as absent API usage.

-   Direction: accept only 0 and 1; report all others with command, API, code, and
    stderr using explicit checks rather than `assert`.
-   Test: inject 0, 1, 2, and 128 and require only 0/1 to follow found/absent paths.


<a id="orgfbf29cd"></a>

## tools/genstrings.py


<a id="orgd144dd8"></a>

### PY201 [LOW] Failed generation can leave a partial target considered current

Make removes `src/stringconstants.c` and redirects stdout directly to the final
target before the generator completes.  Startup/write/disk failure can lose the
last valid source and leave an empty/partial file that Make may consider current.
Compilation usually fails loudly, but release inputs or the working tree remain
contaminated.

-   Direction: preserve the old target, generate to a sibling temporary, rename
    only after success, and clean temporary files on failure.
-   Test: force partial output failure and require the old file unchanged; verify
    successful atomic replacement matches golden output.


<a id="org217770c"></a>

## tools/import<sub>enron.py</sub>


<a id="orgd35cadb"></a>

### PY202 [MEDIUM] Multipart message headers are taken from the selected MIME leaf

The importer selects the first `text/plain` object from `msg.walk()` and builds
`headers` from `part.keys()` and `part.get_all()`.  For multipart mail, that
part is normally a MIME leaf containing MIME metadata rather than the root
message's `From`, `To`, `Subject`, `Date`, and `Received` fields.  In addition,
`keys()` repeats duplicate field names while `get_all()` returns all values for
each occurrence, causing duplicate headers such as `Received` to be appended
more than once.

Imported multipart mail loses the principal searchable message headers and can
inflate indexed duplicate header content.

-   Direction:

Build headers from `msg`, not the selected body part.  Iterate header
occurrences directly with `msg.items()` or `msg.raw_items()` rather than
combining `keys()` and `get_all()`.

-   Test:

Import a multipart fixture whose root has `From`, `Subject`, and two `Received`
fields while its plain-text leaf has only MIME headers.  Assert root headers are
stored, each `Received` occurs once, and leaf metadata is not substituted for
the message header set.


<a id="org9ed2b86"></a>

### PY203 [MEDIUM] MIME bodies are not transfer-decoded and body selection can choose an attachment

`part.get_payload()` returns the undecoded content-transfer payload.  Thus
quoted-printable text such as `Ol=C3=A1` and base64 text such as `T2zDoQ==`
are stored as encoded source instead of readable Unicode.  The first
`text/plain` part from `msg.walk()` is also selected without considering
disposition, so a preceding plain-text attachment can be chosen instead of the
message body.

Searches for decoded words can fail, and an attachment can be imported in place
of the actual message body.

-   Direction:

Use `msg.get_body(preferencelist=("plain",))` to select the body and
`part.get_content()` to obtain decoded text.  Define a controlled fallback for
malformed payloads or unknown character sets.

-   Test:

Cover quoted-printable UTF-8, base64 UTF-8, a declared non-UTF-8 charset, and
a multipart message whose plain-text attachment precedes its body.  Assert the
stored body is decoded Unicode from the non-attachment body.


<a id="org0b5934b"></a>

### PY204 [MEDIUM] Base64 cleanup regex deletes legitimate text and leaves quoted blocks incomplete

The expression `r"\s[A-Za-z0-9+/]{76}\s"` deletes any whitespace-delimited
76-character base64-alphabet line, including legitimate text.  It consumes the
surrounding separators, joining adjacent lines.  Consuming one separator also
prevents an adjacent matching line from being removed.  Padded lines containing
`=` and short terminal base64 lines do not match.

The importer can irreversibly delete ordinary long text and join surrounding
prose while still retaining fragments of the quoted-base64 content it intended
to remove.

-   Direction:

Keep MIME transfer decoding separate from quoted-reply cleanup.  Use a
line/block-oriented detector with contextual evidence for quoted base64,
preserve surrounding separators, handle padded and terminal lines, and make
lossy cleanup optional where practical.

-   Test:

Use LF and CRLF fixtures containing a legitimate 76-character line, adjacent candidate lines, and an inline whitespace-delimited candidate. Verify surrounding prose and separators remain unchanged and a representative multiline quoted-base64 block, including its padded or short final line, is handled only by the documented optional policy.


<a id="org68228ce"></a>

### PY205 [MEDIUM] Raw SQL table-name interpolation affects both importers

Both importers interpolate the user-supplied table name into `DROP TABLE`,
`CREATE TABLE`, and `INSERT INTO` SQL.  Valid SQLite identifiers such as
`select`, `my table`, and `a"b` consequently fail or are parsed as SQL syntax
rather than one identifier.  APSW accepts multiple semicolon-separated
statements, so metacharacters can execute unintended SQL against the
operator-selected database.

Legitimate table names cannot reliably be used, and a mistaken or crafted name
can affect objects beyond the replacement table.  This is primarily a
destructive correctness and automation-safety problem within the selected
database, rather than a privilege-boundary claim.

-   Direction:

Reject NULs and quote each table name as one SQLite identifier by surrounding
it with double quotes and doubling embedded double quotes.  If schema-qualified
targets are supported, accept schema and table separately and quote each
component; do not parse arbitrary SQL or dot-separated input.  Parameters
cannot bind identifiers.

-   Test:

For `select`, `my table`, `a"b`, and `x; DROP TABLE sentinel; --`, run each
importer against a temporary database with a sentinel table.  Assert the name
is either rejected before DDL or treated as one literal identifier, the import
succeeds for valid literal names, and the sentinel survives.


<a id="orgeeecb15"></a>

### PY206 [LOW] Progress output has a duplicate terminal percentage and unsupported fixed denominator

When yielded rows reach `total`, the progress loop can print `100%`, after
which the unconditional final print emits `100%` again.  With one yielded row
and `total=1`, output is `0%100%100%`.  The CLI accepts an arbitrary archive
but always supplies `517402` as its denominator, and skipped members do not
advance the yielded-row count.

Progress output is cosmetically duplicated and may be misleading for
noncanonical or skip-heavy archives.  Import correctness is unaffected.

-   Direction:

Report explicit processed, imported, and skipped counts.  If retaining a
percentage, derive its denominator from known eligible members or label it as
an estimate, and prevent a second terminal `100%`.

-   Test:

For a one-row archive with `total=1`, assert one terminal `100%`.  Include a
skipped member and a custom-sized archive, asserting counters distinguish
processed, imported, and skipped records and do not depend on `517402`.


<a id="orgdcbe408"></a>

### PY207 [LOW] Automatic VACUUM can obscure completed import work

At top-level CLI use, the import transaction completes at the end of the connection context, then `con.execute("vacuum")` rebuilds the entire selected database. If `do_import()` runs inside an existing outer transaction, however, the context releases only its nested savepoint and `VACUUM` fails while the import remains uncommitted. Thus a maintenance failure can follow completed insertion work under either transaction state.

The command can add substantial full-database I/O and temporary-space needs,
especially when the database contains unrelated large tables.  A maintenance failure can appear to the operator as though insertion failed; at top level the import is already committed, while under an existing outer transaction it remains uncommitted.

-   Direction:

Make vacuuming explicit with a documented option or policy. Report insertion completion and transaction state before maintenance starts, and state clearly whether an optional vacuum failed after a committed top-level import or within an uncommitted outer transaction.

-   Test:

Trace the selected vacuum behavior in top-level and existing-outer-transaction cases. Inject a vacuum failure after successful insertion and assert diagnostics accurately distinguish committed import success from uncommitted nested work and maintenance failure.


<a id="org68a7d0b"></a>

## tools/import<sub>html.py</sub>


<a id="orgbab504a"></a>

### PY208 [MEDIUM] Strict UTF-8 decoding rejects valid HTML in other encodings

Every selected ZIP member is decoded with `.decode("utf8")`.  Valid HTML can
declare encodings such as Windows-1252 or UTF-16, including via BOM or
`meta charset`.  Such members raise `UnicodeDecodeError` and abort the import
transaction.

A single valid non-UTF-8 document prevents importing the archive.  The
transaction preserves the previous table, but the raw decoding exception does
not identify the offending member clearly.

-   Direction:

Adopt an explicit byte-to-text policy, such as HTML BOM/meta charset sniffing,
an encoding option with automatic mode, or BLOB storage when exact bytes are
required.  Include the ZIP member name and selected encoding in decode errors;
do not silently discard invalid bytes.

-   Test:

Create an archive with UTF-8, Windows-1252 declared by `meta charset`, and
UTF-16-with-BOM pages.  Assert automatic decoding stores the intended Unicode.
For invalid bytes or an unsupported declaration, assert the error names the ZIP
member and the previous destination table remains unchanged.


<a id="orga94029e"></a>

### PY209 [LOW] HTML-suffixed ZIP directory entries are imported as documents

Suffix filtering occurs without checking `member.is_dir()`.  A directory entry named `section.html/` has the `.html` suffix and is inserted as a document. Conventional directory entries produce empty bytes, while a directory entry carrying bytes can produce nonempty bogus content.

Archives with HTML-suffixed directories gain bogus documents, skewing document counts and downstream indexing.

-   Direction:

Exclude directory entries before applying the suffix filter.

-   Test:

Create a ZIP containing empty and nonempty `section.html/` directory entries, `section.html/page.htm`, and `page.html`. Assert only the two regular files are imported.


<a id="org041c028"></a>

## tools/json<sub>bench.py</sub>


<a id="org4441b5d"></a>

### PY210 [HIGH] Generated JSON is decoded only for a discarded validation result

The script evaluates `[json.loads(i) for i in items]` but discards the parsed
objects, then extends `data` with the original JSON strings.  Consequently
`big_data` contains JSON documents as string values rather than the generated
nested dictionaries, lists, numbers, booleans, and nulls.  The surrogate
exception handling also does not reject lone surrogates returned by
`json.loads()`.

The benchmark largely measures encoding and escaping embedded JSON text as
ordinary strings.  Nested-object traversal, JSON/JSONB sizes, and relative
timings do not represent the intended nested Python-object workload.

-   Direction:

Retain the decoded objects, recursively validate contained strings for UTF-8 encodability, and extend `data` with those parsed objects. Define the size option's unit explicitly: use encoded byte lengths if it means source JSON bytes, or document and count source-text characters.

-   Test:

Use deterministic multibyte JSON containing nested values. Assert retained values are structured objects rather than strings, JSON and JSONB round trips preserve them, lone-surrogate input is rejected by recursive validation, the configured size unit is honored, and the final benchmark object contains nested non-string values.


<a id="orgfdc83af"></a>

### PY211 [HIGH] The row labelled stdlib json.loads measures json.dumps

After correctly producing `big_data_json = json.dumps(big_data)`, the script
times `json.dumps(big_data_json)` but labels the result `stdlib json.loads`.
This serializes an already serialized JSON string, adding escaping and outer
quotes; it does not decode JSON.

The reported stdlib decode timing and comparisons with JSONB decoding are
invalid.

-   Direction:

Time `json.loads(big_data_json)` and verify its result equals `big_data` outside
the timed interval.

-   Test:

With a known nested document, assert the callable for the
`stdlib json.loads` row is `json.loads`, its result is a dictionary equal to
the original object, and it is not a newly escaped JSON string.


<a id="org4d90c88"></a>

### PY212 [LOW] Claimed JSONB warm-up does not call either function

After assigning `decode` and `encode`, the expression `decode, encode` merely
constructs and discards a tuple.  It invokes neither function.  Encoding occurs
before its timed row while obtaining the JSONB size, but decoding is first
invoked in its timed row.

The decode measurement can include one-time call-path initialization not shared
by the encode measurement.  The material effect on the default large workload
is unproven, so this is benchmark hygiene rather than a demonstrated major
timing distortion.

-   Direction:

Perform an explicit small encode/decode warm-up before timed measurements, or
remove the claim that loading has occurred.  Prefer repeated measurements with
a reported median for benchmark stability.

-   Test:

Use instrumented encode/decode stand-ins and assert both are called before the
timed region.  Compare first and subsequent decode calls before making any
claim about material initialization cost.


<a id="org6cac451"></a>

### PY213 [LOW] Help and argparse diagnostics require benchmark dependencies first

The script imports APSW, creates a connection, and loads `randomjson` before
constructing and parsing the argument parser.  Therefore `--help` and ordinary
invalid-option diagnostics are unavailable when APSW or the optional extension
is not built, packaged, or otherwise loadable.

Users cannot inspect usage or receive argparse's normal invalid-argument
diagnostic unless all benchmark runtime dependencies are already working.

-   Direction:

Move parser construction and argument parsing ahead of APSW import, connection
creation, extension loading, and workload setup, preferably through a `main()`
function.

-   Test:

In a subprocess with a deliberately unavailable dependency, assert `--help`
exits successfully with usage and an unknown option produces argparse's normal
status and diagnostic.  Assert a real benchmark invocation still reports its
unavailable dependency clearly.


<a id="org8378334"></a>

### PY214 [LOW] Source vocabulary is read using the locale default encoding

`Path(__file__).read_text()` is called without an encoding even though the
source deliberately contains multilingual text and emoji.  Under a non-UTF-8
default locale, reading the source fails before argument parsing.

The benchmark, including its help path, can fail solely because the platform
default text codec cannot decode the repository's UTF-8 source.

-   Direction:

Read the file with `encoding="utf-8"`.

-   Test:

Run vocabulary loading in a subprocess with UTF-8 mode disabled and an
ASCII/non-UTF-8 locale.  Assert it succeeds and retains representative
multilingual and emoji tokens.


<a id="orgb1c8bef"></a>

## tools/jsonb<sub>proportion.py</sub>


<a id="org833fb12"></a>

### PY215 [LOW] Monte Carlo estimates omit uncertainty information

The utility accumulates Bernoulli outcomes and prints only the point estimate
`yes / tot`.  It shows the common probe denominator, but not hit counts or a
confidence interval.  Early rounds can display either `0.0000%` or `100.0000%`
from one sample, and a zero-hit result at a finite denominator does not
establish a zero population proportion.

An operator can copy a transient rare-event estimate before its uncertainty is
small enough to support the displayed precision.  Sampling and the point
estimator themselves remain valid.

-   Direction:

Print hits and probes together with a binomial confidence interval, including a
nonzero upper bound for zero-hit samples.  Optionally support a target probe
count or confidence-interval width while retaining continuous exploratory mode.

-   Test:

Inject a deterministic detector and run a bounded probe count.  Assert hit and
denominator counts are reported, zero hits have a nonzero upper confidence
bound, and any target-precision indication is emitted only after the calculated
interval width meets that target.


<a id="org95c1e18"></a>

### PY216 [LOW] Configuring a lower bound above one raises KeyError

`passed` is initialized only for lengths from `low` through `high`, but the
heading unconditionally accesses `passed[1]`.  Changing `low` to `2` therefore
raises `KeyError` before sampling.  The script explicitly presents `low` as a
configurable range boundary and discusses advancing it after short lengths have
received sufficient work.

The utility cannot safely run a range beginning above one or implement its
suggested optimization of dropping completed short lengths.

-   Direction:

Track cumulative probes independently of a per-length entry.  A minimal static
range correction can use `passed[low]`, but dynamic range adjustment should use
a separate total-probe counter.

-   Test:

Run a bounded iteration with `low = 2` and `high = 3` using a deterministic
detector.  Assert no key `1` is accessed, both lengths receive the configured
number of probes, the heading reports the correct denominator, and advancing
`low` does not reset or misreport it.


<a id="org69b3584"></a>

## tools/megatest.py


<a id="orgee032ab"></a>

### PY217 [MEDIUM] System-Python jobs misrepresent configuration and GIL coverage

All `pyver =` "system"= jobs receive `--use-system-sqlite-config`, so `config=none` and `config=full` do not exercise their requested SQLite configurations. The planner also schedules `gil=False` for system Python because `natural_compare("system", "3.14")` treats the non-version sentinel as newer, but `buildpython()` immediately returns `/usr/bin/python3` without configuring a free-threaded interpreter. Jobs are not necessarily byte-identical because warning flags and randomized extension flags may differ, but their labels claim coverage that is not being exercised.

-   Direction: Detect the installed system interpreter's GIL mode, schedule only that effective mode, and either limit system Python to `config=system` or honor the requested `none` and `full` configuration flags.

-   Test: Generate the matrix without running builds and assert that normal system Python is never labelled `gil=False`, `config=full` includes `--enable-all-extensions`, `config=none` includes neither SQLite configuration override, and displayed labels match effective settings.


<a id="orgcff315f"></a>

### PY218 [HIGH] Installed-wheel validation can import the in-tree extension

The job first builds APSW in place with `build_ext --inplace`, then builds and installs a wheel, but runs `python -m apsw.tests` while the copied source checkout remains the current directory. Python can resolve the top-level in-tree APSW extension before the venv's installed wheel. Broken wheel contents, metadata, extension placement, or packaged support files can therefore pass the purported post-install test.

-   Direction: Run installed-wheel tests from a newly created directory outside the source checkout and verify that `apsw.__file__` resolves under the venv's installed `site-packages` directory.

-   Test: Build distinguishable in-tree and wheel artifacts, install the wheel, run tests from an unrelated directory, and assert that the imported module path and marker belong to the installed wheel.


<a id="orgca921ad"></a>

### PY219 [HIGH] Worker failures are reported but do not fail megatest

Exceptions from completed futures are caught and printed as `FAIL`, but no failure state is retained or returned. `main()` returns normally after all jobs finish, so one or all matrix jobs can fail while the megatest process exits with status zero. CI and wrappers relying on process status can accept failed compatibility coverage.

-   Direction: Accumulate failed jobs while continuing to await all work, print a final failure summary with job/log information, and return a nonzero status when any worker failed.

-   Test: Inject one successful and one failing future; assert both are awaited and reported, failure metadata is emitted, and the process exits nonzero. Assert an all-success run exits zero.


<a id="org0129574"></a>

### PY220 [MEDIUM] Startup cleanup can delete host user-site packages and sibling-tree data

Startup unconditionally removes matching APSW packages from `$HOME/.local/lib/python*/site-packages/` and clears `../apsw-test/*` without an ownership marker. The documented Podman flow normally makes these disposable container/mounted work locations, but direct host execution can delete real user installations and unrelated files in a pre-existing sibling directory.

-   Direction: Do not delete user-site packages; use `PYTHONNOUSERSITE` and import-origin checks instead. Use a unique or explicitly configured work root, and require a tool-created ownership marker before recursive cleanup.

-   Test: In a temporary fake home and repository layout, create user-site and marked/unmarked work-directory sentinels. Assert user-site and unmarked data survive and only a marked tool-owned work directory is cleaned.


<a id="orgbb17a66"></a>

### PY221 [MEDIUM] Shell interpolation permits command injection and unsafe path handling

`run()` executes interpolated command strings with `shell=True`. Unrestricted CLI values such as Python, SQLite, and configuration versions flow into URLs, shell commands, job paths, and cleanup commands without quoting or validation. Metacharacters can execute commands under the invoking user; spaces can break normal paths; traversal-containing values can escape intended output roots. In Podman, mounted source, work, and ccache paths remain writable.

-   Direction: Replace shell programs with checked argument-vector subprocess calls using `cwd=` and `env=`; validate version/configuration inputs; and resolve generated paths before verifying they remain beneath configured output roots.

-   Test: Exercise values containing spaces, shell metacharacters, quotes, newlines, and traversal components. Assert invalid input is rejected before filesystem changes, no injected command runs, and generated paths remain within their roots.


<a id="orgebcd386"></a>

### PY222 [MEDIUM] Free-threaded jobs suppress broad warning and bytes strictness

Only GIL-enabled jobs receive `-bb -Werror`. The expected free-threaded APSW GIL-enabling warning is handled by dropping strictness for all warnings and bytes/str diagnostics, allowing unrelated regressions to pass in free-threaded jobs. The installed-wheel test omits this policy for both modes.

-   Direction: Retain `-bb -Werror` in both modes and narrowly ignore only the known expected RuntimeWarning by category and constrained message/module. Apply the selected policy to wheel validation as well.

-   Test: Under a free-threaded job, verify the known APSW warning is ignored, unrelated RuntimeWarning and DeprecationWarning instances fail, BytesWarning-producing comparisons fail under `-bb`, and the installed-wheel phase applies the same policy.


<a id="org7d31076"></a>

## tools/names.py


<a id="orgb71f4b5"></a>

### PY223 [HIGH] Legacy-name test failures do not affect process status

`TextTestRunner.run()` returns a `TestResult` rather than raising on ordinary failures or errors. The result is discarded, so `run-tests` returns successfully even when transformed old-name tests fail. The `make test` recipe invokes this command and therefore can continue and succeed despite a failed compatibility test pass.

-   Direction: Capture the result, return or raise a nonzero status when `result.wasSuccessful()` is false, and have the CLI entry point propagate that status.

-   Test: Run a failing/erroring transformed suite in a subprocess and assert nonzero exit status; run a successful suite and assert zero status.


<a id="org4542838"></a>

### PY224 [LOW] Subcommands depend inconsistently on the caller's working directory

Rename data is resolved relative to the script, but `run-tests` reads `apsw/tests/__main__.py` relative to the process CWD and `check-old` invokes Git with root-relative-looking pathspec exclusions without `git -C`. From `tools/`, `run-tests` cannot find the test file and `check-old` silently searches a different scope. Current Make callers run from repository root, so this is conditional on arbitrary-CWD direct invocation being supported.

-   Direction: Derive the repository root from `__file__`, read the test source from that root, and invoke Git with `-C` pointing to it. Alternatively, explicitly reject non-root invocation with a clear diagnostic.

-   Test: Invoke every subcommand from repository root, `tools/`, and an unrelated directory through an absolute script path. Assert equivalent results or a deliberate clear nonzero root-only error.


<a id="orgaa9185e"></a>

### PY225 [LOW] Locale-dependent decoding can prevent legacy-name test execution

`Path.read_text()` uses the process locale when no encoding is supplied. The transformed test source contains non-ASCII text, so under a non-UTF-8 locale such as `LC_ALL=C PYTHONUTF8=0` it can raise `UnicodeDecodeError` before transformation or testing. This affects the `make test` invocation because it does not establish UTF-8 mode.

-   Direction: Read repository text explicitly as UTF-8, including both `renames.json` and the test source.

-   Test: Run source loading or `run-tests` in a subprocess with locale coercion and UTF-8 mode disabled. Assert Unicode-bearing source loads and transforms correctly; include a non-ASCII rename-data fixture where practical.


<a id="org1ccb4ea"></a>

## tools/recipe.py


<a id="org7008972"></a>

### PY226 [MEDIUM] Recipe archive decoding depends on the process locale

`gzip.open(..., "rt")` defaults to the process locale encoding. The referenced OpenRecipes JSON-lines archive is UTF-8 and extensively contains non-ASCII content. On legacy non-UTF-8 locales it can fail decoding; permissive single-byte decoders can instead import mojibake into recipe fields and generated FTS indexes.

-   Direction: Open the archive through a context manager with explicit UTF-8 and strict error handling, for example `gzip.open(path, "rt", encoding`"utf-8", errors="strict")=.

-   Test: Import a UTF-8 gzip JSON-lines fixture containing curly quotes, fractions, and other non-ASCII text under an ASCII/non-UTF-8 locale. Assert import succeeds and stored recipe values match exactly; assert invalid UTF-8 still fails.


<a id="orgdf7150c"></a>

## tools/sqlite<sub>compile</sub><sub>info.py</sub>


<a id="org6968eac"></a>

### PY227 [MEDIUM] Compile-option values containing = are parsed incorrectly

\`transmogrify()\` uses \`co.split("=")\` and asserts that exactly one or two pieces result. Valid compile-option values may themselves contain \`=\`, such as \`COMPILER=vendor=release\` or \`DEFAULT<sub>CACHE</sub><sub>SIZE</sub>=(1==1)\`. Normal execution raises \`AssertionError\`; optimized execution retains a malformed three-element tuple and comparison discards trailing value content.

A custom SQLite build can make this comparison tool abort or silently omit meaningful compile-option differences.

-   Direction:

Split only at the first delimiter, preserving the entire remaining value, for example with \`partition("=")\`. Use explicit validation rather than \`assert\` if an empty option name must be rejected.

-   Test:

Cover valueless options and values containing additional equals signs. Compare \`COMPILER=foo=bar\` with \`COMPILER=foo=baz\` under normal and optimized Python, requiring the complete values to be reported as different.


<a id="orgdfd6c33"></a>

### PY228 [MEDIUM] Decimal coercion hides distinct C compile-time values

\`transmogrify()\` converts any value for which \`isdigit()\` is true to a Python decimal integer. Consequently, \`X=010\` and \`X=10\` both become \`("X", 10)\`. SQLite exposes stringified C preprocessor values; in C, \`010\` is octal 8 while \`10\` is decimal 10.

The tool can falsely report behaviorally different SQLite configurations as equal.

-   Direction:

Retain compile-option values as exact text unless a complete and intentional C-literal semantic parser is implemented.

-   Test:

Verify that \`X=010\` differs from \`X=10\`, along with textual comparisons involving hexadecimal, signed, and suffixed values. Identical text must remain equal.


<a id="org636a0b4"></a>

### PY229 [LOW] Dynamically loaded SQLite reporting is Linux-specific and can be misleading

\`figure<sub>out</sub><sub>libsqlite3</sub>()\` reads only \`/proc/<pid>/maps\`, returns the first matching \`/libsqlite3.so\` mapping, and obtains its pathname with \`line.split()[-1]\`. This is unavailable on typical macOS and Windows systems, truncates Linux pathnames containing spaces, and cannot establish which of multiple mapped SQLite libraries belongs to either compared module.

Diagnostics can report a truncated or arbitrary process mapping on unusual Linux linkage, while unsupported platforms produce no discovery result.

-   Direction:

Parse maps using a bounded split, collect and deduplicate plausible SQLite paths, label them as process mappings rather than module-owned libraries, and explicitly report unsupported or unavailable discovery.

-   Test:

Test supplied maps text with repeated mappings, multiple SQLite paths, pathnames containing spaces, and mapping lines without pathnames. Test unavailable maps and a mocked non-Linux platform.


<a id="orgeb1791c"></a>

### PY230 [LOW] Standard-library sqlite3 is labeled pysqlite3

The script imports and queries the standard-library \`sqlite3\` module but passes \`"pysqlite3"\` as its label to \`get<sub>differences</sub>()\`. \`pysqlite3\` is also the identity of a separate package which this script does not import.

Comparison output can incorrectly imply that options came from the separately installed \`pysqlite3\` package.

-   Direction:

Use a consistent unambiguous label such as \`stdlib sqlite3\` or \`Python sqlite3\`.

-   Test:

Capture output for distinct option sets and require the left side to use the selected standard-library label, with no \`pysqlite3\` text present.


<a id="orgab9216e"></a>

## tools/sqliteliboptions.py


<a id="org89eaf27"></a>

### PY231 [MEDIUM] Valueless SQLite options become empty macro definitions

For an option without \`=\`, the script appends an empty replacement value and emits, for example, \`#define SQLITE<sub>ENABLE</sub><sub>FTS5</sub>\`. Unlike the equivalent normalization in \`setup.py\`, it does not emit \`1\`. A subsequent \`#if SQLITE<sub>ENABLE</sub><sub>FTS5</sub>\` has no expression and fails preprocessing.

The generated output cannot safely serve as a configuration header for consumers that evaluate enabled options numerically.

-   Direction:

Normalize valueless options to \`1\`, preferably through shared normalization logic with \`setup.py\`.

-   Test:

Use a fixture returning \`ENABLE<sub>FTS5</sub>\`; require \`#define SQLITE<sub>ENABLE</sub><sub>FTS5</sub> 1\`, then preprocess source using both \`#ifdef SQLITE<sub>ENABLE</sub><sub>FTS5</sub>\` and \`#if SQLITE<sub>ENABLE</sub><sub>FTS5</sub>\`.


<a id="orgce86d8e"></a>

### PY232 [LOW] Diagnostic options are serialized as source definitions

The utility converts every reported diagnostic string into a \`#define\`. SQLite can report \`COMPILER=gcc-&hellip;\`, producing an unquoted \`SQLITE<sub>COMPILER</sub>\` value that is unreliable and generally unusable as a source-level configuration value. The maintained \`setup.py\` path removes \`SQLITE<sub>COMPILER</sub>\` and cleans up derived mutex-selection macros.

A consumer using the generated output as a configuration header can receive invalid or behavior-altering macro definitions.

-   Direction:

Follow \`setup.py\` collection and cleanup policy: normalize valueless options to \`1\`, preserve valid source-compatible values, remove \`SQLITE<sub>COMPILER</sub>\`, and apply equivalent mutex macro cleanup. Alternatively, emit raw diagnostics rather than C definitions when inspection is the intent.

-   Test:

With fixture values for \`COMPILER\`, \`THREADSAFE\`, \`MUTEX<sub>PTHREADS</sub>\`, and \`ENABLE<sub>FTS5</sub>\`, require omission of \`SQLITE<sub>COMPILER</sub>\`, matching mutex cleanup, a numeric boolean definition, and successful preprocessing of the remaining header.


<a id="orgc5ee837"></a>

### PY233 [LOW] The checked path can differ from the library loaded

The script validates \`os.path.isfile(sys.argv[1])\` but passes the original argument to \`ctypes.cdll.LoadLibrary()\`. On POSIX, a bare filename can be subject to dynamic-loader search rather than referring to the checked current-directory file; a valid local basename can therefore fail or resolve ambiguously.

Valid basename input may fail unexpectedly, and a same-named library in loader search paths may be loaded instead of the file that was checked.

-   Direction:

Resolve the supplied path once, verify it is a file, and load that resolved path.

-   Test:

Invoke the utility with a fixture library basename from the current directory without changing loader paths. Require successful loading of that fixture and verify a same-named loader-search-path library is not selected.


<a id="org8d81db9"></a>

## tools/types2rst.py


<a id="org7fbb645"></a>

### PY234 [LOW] Blind identifier substitution can corrupt existing RST markup

\`nomunge()\` applies identifier substitution over whole values and descriptions, including existing roles and inline literals. A matched name in \`:class:\\\`Foo\\\`\`, \`:meth:\\\`Foo.bar\\\`\`, or \`\`Foo\`\` becomes nested or altered markup. Current source already contains `` :attr:`Connection.connection_hooks` `` in `src/apswtypes.py:199`, so substitution nests generated class markup inside an existing role. Other future descriptions using known generated names inside RST constructs can likewise produce malformed documentation.

-   Direction:

Protect or tokenize existing RST inline constructs before linking identifiers, rather than applying global substitution and special-case repairs.

-   Test:

Render the current `Connection.connection_hooks` reference plus content containing plain \`Foo\`, \`:class:\\\`Foo\\\`\`, \`:meth:\\\`Foo.bar\\\`\`, and \`\`Foo\`\`. Require existing constructs to remain byte-for-byte valid and only plain text to be linked, then validate with docutils or Sphinx.


<a id="org1e19d83"></a>

### PY235 [LOW] Global asterisk replacement can change literal values

For every non-protocol alias, rendering applies \`value.replace("**", "** ")\`. This changes a valid alias such as \`Literal["\*"] | Callable[[\*Ts], R]\`, altering both literal content and unpacking formatting.

A future type alias containing an asterisk literal can be documented with a different semantic value.

-   Direction:

Limit spacing changes to the specific RST boundary requiring it, or render unpacking syntax structurally instead of globally replacing every asterisk.

-   Test:

Cover both \`Callable[[\*Ts], R]\` and \`Literal["\*"]\`; require required unpacking spacing while preserving the literal byte-for-byte.


<a id="org103948d"></a>

### PY236 [LOW] Output encoding depends on the process locale

The renderer emits U+200B zero-width spaces in generated links, while source and destination files are opened without explicit encodings. The current generated output necessarily contains non-ASCII U+200B characters.

Documentation generation fails with \`UnicodeEncodeError\` under an ASCII or other non-UTF-8 default text encoding.

-   Direction:

Read and write files explicitly as UTF-8.

-   Test:

Run generation in a subprocess with locale coercion and UTF-8 mode disabled under a non-UTF-8 locale. Require successful UTF-8 output containing U+200B.


<a id="org874bdc6"></a>

### PY237 [LOW] Destination is truncated before generation succeeds

The script opens \`doc/typing.rstgen\` for writing before calling \`process()\` and \`output()\`. A later exception leaves an empty or partial generated target. The normal Make rule already removes the target first, so preserving an older target is not its supported behavior, but a failed direct generation can still publish an incomplete target.

A failed generation can leave an empty or partial target that may interfere with later incremental builds, as well as destroying direct-invocation output.

-   Direction:

Complete processing and rendering before publishing the destination. Write a UTF-8 temporary sibling and atomically replace the destination only after successful completion.

-   Test:

Seed the destination with sentinel content, force processing or rendering to fail, and require that no empty or partial target is published. Then verify a subsequent Make invocation regenerates successfully rather than treating a failed target as current.


<a id="orgc4e1809"></a>

### PY238 [LOW] Python data objects use class roles

The renderer emits \`:class:\` for all configured standard names, including \`None\`, \`typing.Any\`, and \`typing.Tuple\`. Python intersphinx inventories classify these as \`py:data\`, while \`:class:\` resolves only class and exception targets. \`Tuple\` is configured but is not currently used by \`src/apswtypes.py\`; current generated content includes \`:class:\\\`None\\\`\` and \`:class:\\\`~typing.Any\\\`\`.

Generated documentation can contain unresolved links and nitpicky Sphinx warnings. The normal documentation build does not treat warnings as errors.

-   Direction:

Use an explicit symbol-to-role mapping: emit \`:data:\` for \`None\`, \`typing.Any\`, and \`typing.Tuple\`; retain \`:class:\` only for actual class objects.

-   Test:

Generate a minimal page covering all mapped standard symbols and build it with project configuration, intersphinx, nitpicky mode, and warnings as errors. Require every reference to resolve to the intended inventory target.


<a id="org267fb32"></a>

## tools/ucdnames.py


<a id="orge6d87f5"></a>

### PY239 [MEDIUM] Numbered Unicode-name suffixes are not validated

Evidence: `check_numbered()` compares only the text before the final hyphen.
Thus `18800 ; TANGUT COMPONENT-999` and `FE00 ; VARIATION SELECTOR-17` pass
validation even though their numeric suffixes disagree with the code point.
The source record is then discarded and generated C calculates a different
name from the hard-coded range formula.

Impact: malformed local UCD input or a numbering-rule change can be silently
accepted while the generated `NAME_RANGES` table emits names inconsistent with
its source.

-   Direction: associate numbered ranges with exact name-formatting functions and
    compare the complete expected name, including offsets and padding, before
    discarding a source record.
-   Test: accept correct Tangut and variation-selector names; reject incorrect
    suffixes such as `TANGUT COMPONENT-999` at U+18800 and `VARIATION
      SELECTOR-17` at U+FE00; verify generation stops before emitting C.


<a id="org62ac65c"></a>

### PY240 [MEDIUM] A Unicode-17 Tangut range is emitted for older source data

Evidence: the hard-coded `18D80..18DF2` Tangut-component range is included in
`numbered` unconditionally.  Every numbered range is emitted into
`NAME_RANGES`, even if the supplied `DerivedName.txt` does not contain it.
Unicode 16 data has no assignments in that range, while Unicode 17 assigns
components 769 through 883.

Impact: generating with Unicode 16 input can produce a database labelled as
Unicode 16 that returns Unicode-17 names for 115 unassigned code points.  The
current checked-in Unicode-17 output is unaffected.

-   Direction: retain numbering formulas separately from observed coverage, and
    emit exceptional ranges only where the supplied `DerivedName.txt` contains
    contiguous validated entries.  Source-derived coverage is preferable to
    version-specific hard-coding.
-   Test: generate from Unicode 16 and require no names for U+18D80 and U+18DF2
    while retaining U+18800..U+18AFF; generate from Unicode 17 and require names
    for U+18D80..U+18DF2.


<a id="org2d0a6e8"></a>

### PY241 [LOW] Standalone generation decodes official UCD input using the process locale

Evidence: the standalone entry point uses `open(sys.argv[1], "rt").read()`
without an encoding.  Official `DerivedName.txt` files contain non-ASCII text
in their copyright header.  With Python UTF-8 mode and locale coercion disabled under
an ASCII locale, decoding fails before parsing.  The integrated
`ucdprops2code.py` path already reads UCD files as UTF-8.

Impact: direct use of the standalone generator fails on valid official input
in non-UTF-8 environments.  The ordinary integrated generation path is not
affected.

-   Direction: read the source explicitly as UTF-8 and write the currently
    ASCII-only generated C with an explicit ASCII encoding and newline policy.
-   Test: run the standalone command under an ASCII locale with UTF-8 mode and
    locale coercion disabled against an official-format file containing its
    non-ASCII header; require successful ASCII `dbnames.c` output.


<a id="org6ae7c13"></a>

## tools/ucdprops2code.py


<a id="org6418c6d"></a>

### PY242 [MEDIUM] Strip mappings retain decomposable intermediates

Evidence: `extract_strip()` parses one decomposition level, filters immediate
children in stripped categories, and stores the remaining immediate children
without recursively expanding them.  The generated table consequently maps
U+01D5 to U+00DC, U+01FA to U+00C5, and U+01C4 to U+0044 U+017D.  Runtime stripping looks up each original code point during sizing and again during writing, but writes replacements directly without reprocessing them.

Impact: `apsw.unicode.strip()` is non-idempotent and can leave diacritics or
compatibility forms behind: U+01D5 becomes `Ü` rather than `U`, U+01FA becomes
`Å` rather than `A`, and U+01C4 becomes `DŽ` rather than `DZ`.

-   Direction: recursively resolve retained canonical and compatibility
    decomposition children, filter stripped-category leaves, memoize expansions,
    and reject cycles defensively.
-   Test: require `strip("\u01D5") == "U"`, `strip("\u01FA") == "A"`, and
    `strip("\u01C4") == "DZ"`; exhaustively check that stripping every scalar
    value is idempotent.


<a id="org59a5355"></a>

### PY243 [LOW] Output publication can truncate generated C before generation succeeds

Evidence: argument parsing opens the C destination through
`argparse.FileType("w", encoding="utf8")`, truncating it before UCD acquisition,
version validation, parsing, and generation complete.  C is written directly;
the later `apsw/unicode.py` update can then fail after C has been published.
The normal Make rule also removes the target before invoking the generator.

Impact: direct invocation can destroy the last generated C file on failure,
and a failure during the Python update can leave the C and Python generated
metadata inconsistent.

-   Direction: accept an output path rather than an already-open file, generate
    and validate all output before publication, stage same-directory temporary
    files, replace only after both outputs are ready, and remove the Makefile
    pre-generation deletion.
-   Test: seed both outputs with sentinels, inject source-read and Python-target
    validation failures through both direct invocation and Make, and require
    neither published output to change.


<a id="org474e423"></a>

### PY244 [MEDIUM] Unicode inputs are not bound to one immutable release

Evidence: UCD files are read independently from mutable `/latest/` URLs or a
local directory.  Most version-bearing files are checked, but
`DerivedAge.txt~—which has a parseable version header—is passed to ~populate()`
without `extract_version()`, and `UnicodeData.txt` has no independent snapshot
binding.  Mixed local data can
therefore pass validation.  In particular, code points missing from an older
`UnicodeData.txt` default to a zero strip mapping and can be treated as
stripped despite newer category data.

Impact: a successful run can claim Unicode version N while using age data from
another release or incorrectly stripping newly assigned characters absent from
an older `UnicodeData.txt`.

-   Direction: consume one immutable, explicitly selected UCD release or archive;
    validate `DerivedAge.txt`; and add completeness checks between assigned
    category data and `UnicodeData.txt`, accounting for documented range rules.
-   Test: provide local fixtures with version-N category data plus version-N-1
    `DerivedAge.txt`, and with version-N category data plus version-N-1
    `UnicodeData.txt` containing a newly assigned character; require failure
    before publication.


<a id="orgaada512"></a>

### PY245 [MEDIUM] Optimized Python disables external-UCD integrity checks

Evidence: assertions that validate externally supplied UCD data are removed by
`python -O`.  These include required-property presence, CaseFolding structure
and ASCII assumptions, and agreement between `UnicodeData.txt` and derived
general categories.  For example, an omitted `InCB; Linker` property can
produce an empty generated property under optimized execution rather than a
generation failure.

Impact: optimized generation can publish semantically incorrect segmentation or
strip tables from malformed or incomplete UCD input.

-   Direction: replace external-data assertions with explicit exceptions that
    identify the source file, expected property or code point, and observed
    value; retain `assert` only for invariants whose removal cannot publish bad
    output.
-   Test: under normal Python and `python -O`, run fixtures omitting
    `InCB; Linker` and containing a category mismatch; require equivalent
    failures and unchanged outputs in both modes.


<a id="orgce3ebd5"></a>

### PY246 [MEDIUM] Marker-based update can target or truncate the wrong Python source

Evidence: the generator rewrites relative `apsw/unicode.py` using an
unvalidated begin/end-marker state machine.  Missing closing or reversed
markers can discard content through EOF, duplicate pairs are silently
rewritten, and a missing opening marker is silently ignored.  Unlike
helper-module loading, this target is not resolved from `__file__`, so an
out-of-root invocation may fail after C publication or rewrite a matching
CWD-local file.

Impact: a minor marker edit can truncate manually maintained Python source, and
direct invocation can publish only one output or modify an unintended
`apsw/unicode.py`.

-   Direction: resolve the repository target from `Path(__file__)` or require an
    explicit target; require exactly one ordered marker pair before writes; stage
    both outputs; and document both generated targets.
-   Test: cover missing, reversed, and duplicate markers plus out-of-root
    invocation with an unrelated CWD-local `apsw/unicode.py`; each malformed case
    must fail without changing either intended or unrelated output.


<a id="org92f29cc"></a>

### PY247 [LOW] Invalid `--table-limit` values are accepted until late generation

Evidence: `--table-limit` accepts any integer.  A negative value produces an
empty fast table and later emits invalid C such as `if (c < 0x-001)`.  A value
of `sys.maxunicode + 1` eventually indexes an empty range list and raises
`IndexError` after expensive processing.

Impact: accepted command-line input can generate uncompilable C or fail late;
combined with early destination truncation, this can also discard the prior
output.

-   Direction: validate the supported range during argument parsing, at minimum
    `0 <= table_limit <= sys.maxunicode`, or explicitly support an all-fast-table
    representation without a remaining binary-search range.
-   Test: reject `-1`, `sys.maxunicode + 1`, and very large values before source
    acquisition or output mutation; retain tests for valid 0, default 256, and
    `sys.maxunicode` limits and compile their generated C.


<a id="org5961240"></a>

### PY248 [LOW] Repository Python-source I/O depends on the locale encoding

Evidence: `replace_if_different()` and the marker pass use
`Path.read_text()` and `write_text()` without an explicit encoding, although
the repository's `apsw/unicode.py` contains accented text, Devanagari, Roman
numerals, and emoji.  Other UCD and C-output paths explicitly specify UTF-8.

Impact: on supported non-UTF-8 locale configurations, generation can fail while
reading or rewriting the Python target after C generation has completed.

-   Direction: use explicit UTF-8 for all repository text reads and writes,
    including a defined newline policy, together with staged publication.
-   Test: run the Python-target update under a non-UTF-8 locale with UTF-8 mode
    disabled against a target containing accented text, Devanagari, and emoji;
    require byte-preserving UTF-8 output.


<a id="orga70d023"></a>

## tools/vend.py


<a id="orgf91341a"></a>

### PY249 [MEDIUM] Bootstrap and prerequisite failures can report successful builds

Evidence: compiler-probe failures and recognized support-library or
`sqlite3_stdio` compile/link failures return normally.  The CLI ignores the
result and exits zero.  Consequently, callers such as megatest running under
`set -e` cannot distinguish a build that produced no requested binaries from a
successful one.

Impact: megatest can continue after no requested extras were built, and a full
package build can silently omit all extras after an early bootstrap failure.
Individual extras are intentionally best-effort, so the defect is failure
reporting for total/bootstrap failure rather than a requirement that every
optional extra abort packaging.

-   Direction: return structured build results or raise a dedicated bootstrap
    exception.  Make CLI and megatest fail when no requested item is attempted
    or a required prerequisite fails, while preserving documented best-effort
    handling for independent optional extras in packaging.
-   Test: use a failing fake compiler and require nonzero CLI status and a stopped
    megatest; inject support failures for selected and unselected prerequisite
    paths; verify one optional-extra failure can remain nonfatal when another
    requested extra succeeds.


<a id="orgb4a7f47"></a>

### PY250 [MEDIUM] Extension-only builds compile and publish executable-only prerequisites

Evidence: `libsqlite3_tool` is compiled and copied into the package binary
directory, and `sqlite3_stdio.c` is compiled to objects, before requested types
are filtered.  Extensions explicitly do not use either support component, yet
`compile --only extension` still requires them; only the copied library becomes
an orphan package artifact.

Impact: an extension-only request can fail before extensions are attempted
because of unrelated executable support code, performs unnecessary compilation,
and publishes an orphan support artifact not represented in
`sqlite_extra.json`.

-   Direction: derive prerequisites from the selected `Extra` records.  Skip both
    support components for extension-only builds, and build or publish each
    support library only if a selected executable requires it.
-   Test: instrument an extension-only build and require that neither SQLite-tool
    nor `sqlite3_stdio.c` is compiled; make `sqlite3_stdio.c` fail and verify
    extensions still build with no executable-only support library published.


<a id="org3c178cb"></a>

### PY251 [MEDIUM] RST generation can leave a partial target that Make treats as current

Evidence: RST generation opens and truncates the final target, writes initial
content, then imports APSW and incrementally loads and introspects extensions.
An import or extension-load failure therefore occurs after the target has been
partially replaced.  The Make recipe directly invokes the generator and lacks
`.DELETE_ON_ERROR` protection.

Impact: after a transient failure, a partially rewritten RST file can remain
present and be newer than `tools/vend.py`, its declared prerequisite, so a later
Make invocation can accept it as current and allow incomplete generated
documentation into subsequent processing.

-   Direction: generate into a same-directory temporary file, complete all
    imports, loads, and introspection before `os.replace()`, and clean temporary
    output on failure.  Consider declaring runtime binaries as dependencies or
    otherwise forcing regeneration when available extras change.
-   Test: seed a known-good target, force extension loading to fail after several
    entries, and require nonzero exit, unchanged target bytes, and no temporary
    file; then verify the Make target does not accept partial output as current.


<a id="orgc50872d"></a>

## tools/vtbench.py


<a id="orgc24a659"></a>

### PY252 [MEDIUM] Repr variants use independently generated data and confound comparisons

Evidence: the normal/hidden dataset and `repr` dataset independently call
`gen_value()`.  The latter is not derived from the former by changing only the
sixth row's (`i == 5`) `quadrangle`; corresponding rows instead differ in
nearly every random field, including aggregate blob and string-payload lengths
and totals, while `repr_invalid` is intended to measure value validation and
occasional `repr()` conversion.

Impact: deltas between normal and `repr` variants cannot be attributed cleanly
to `repr_invalid` handling.  Comparisons among the three `repr` variants remain
valid because they share `rows_repr`, as do normal-versus-hidden comparisons
using `rows`.

-   Direction: generate one baseline dataset and derive `rows_repr` from it,
    changing only the sixth row's (`i == 5`) `quadrangle` to the intended invalid object.
-   Test: require corresponding datasets to have equal lengths and equal fields
    except for that one value; require equal string/blob totals and verify the
    invalid value still reaches `repr()` during a query.


<a id="org8f4aa40"></a>

### PY253 [MEDIUM] Fixed ordering and absent warm-up may bias benchmark variants

Evidence: every repetition uses the same ordering—normal access variants, then
`repr` variants, then hidden-column variants—and no untimed query warm-up
precedes recorded samples.  Each label is permanently correlated with ordinal
position and process/cache/CPU state.  Reporting six samples and a median
reduces random noise but cannot remove a systematic order effect.

Impact: if initialization, cache, thermal, or frequency effects are material,
reported differences may partly measure execution position rather than access
mode, `repr_invalid`, or hidden-column handling.  Static inspection establishes
the confound but not its material size.

-   Direction: add untimed warm-ups and use a deterministic balanced schedule,
    such as cyclic or Latin-square rotation, so each variant occupies multiple
    positions.
-   Test: unit-test that each repetition contains every variant once and that
    labels occupy multiple positions while warm-ups are excluded.  In
    fresh-process trials, compare fixed, reversed, and balanced schedules under
    controlled CPU conditions; promote the issue only if rankings or normalized
    deltas change beyond ordinary run-to-run variance.


<a id="org3bd71e0"></a>

# Rejected-candidate and deduplication method

Initial candidates were not retained merely because a suspicious pattern was
present.  The independent pass checked the actual supported call path, runtime
or standard-library contract, current generated inputs, and existing tests.
Style-only improvements, editorial issues, unsupported invocation modes,
hypothetical future inputs without current impact, broad security claims
without a privilege boundary, and test-hardening ideas without a false result
were excluded.  A qualified candidate appears above only with its independently
supported core and proportionate severity.

Repeated manifestations with one fix root are reported once.  Notable examples
are the two copied `assertTablesEqual()` implementations, destructive fixed
example database names, raw importer table-name interpolation, shared
`toc.db` URL/timeout and Windows handling, and generated-stub compatibility
rooted in `src/apswtypes.py`.  Generated artifacts name their canonical source
or generator.  The `tools/genstrings.py` process-global/subinterpreter concern
is intentionally excluded as a duplicate of C002 in `docs/c.org`.


<a id="orgb7effe0"></a>

# Coverage index

Every file below received both the primary and independent audit pass.  A
finding range names the finding whose primary section covers the file; a
cross-file reference identifies consolidated roots.  “No actionable findings”
means all candidates were rejected after validation.

<table border="2" cellspacing="0" cellpadding="6" rules="groups" frame="hsides">


<colgroup>
<col  class="org-left" />

<col  class="org-left" />
</colgroup>
<thead>
<tr>
<th scope="col" class="org-left">File</th>
<th scope="col" class="org-left">Disposition</th>
</tr>
</thead>
<tbody>
<tr>
<td class="org-left">apsw/_<sub>main</sub>_<sub>.py</sub></td>
<td class="org-left">No actionable findings</td>
</tr>

<tr>
<td class="org-left">apsw/aio.py</td>
<td class="org-left">PY001–PY004</td>
</tr>

<tr>
<td class="org-left">apsw/bestpractice.py</td>
<td class="org-left">PY005–PY007</td>
</tr>

<tr>
<td class="org-left">apsw/ext.py</td>
<td class="org-left">PY008–PY015</td>
</tr>

<tr>
<td class="org-left">apsw/fts5.py</td>
<td class="org-left">PY016–PY023</td>
</tr>

<tr>
<td class="org-left">apsw/fts5aux.py</td>
<td class="org-left">PY024–PY025</td>
</tr>

<tr>
<td class="org-left">apsw/fts5query.py</td>
<td class="org-left">PY026–PY033</td>
</tr>

<tr>
<td class="org-left">apsw/shell.py</td>
<td class="org-left">PY034–PY044; related schema metadata in PY010</td>
</tr>

<tr>
<td class="org-left">apsw/speedtest.py</td>
<td class="org-left">PY045–PY052</td>
</tr>

<tr>
<td class="org-left">apsw/sqlite<sub>extra.py</sub></td>
<td class="org-left">PY053–PY054</td>
</tr>

<tr>
<td class="org-left">apsw/tests/_<sub>main</sub>_<sub>.py</sub></td>
<td class="org-left">PY055–PY060</td>
</tr>

<tr>
<td class="org-left">apsw/tests/aiotest.py</td>
<td class="org-left">PY065–PY070</td>
</tr>

<tr>
<td class="org-left">apsw/tests/async<sub>meta.py</sub></td>
<td class="org-left">PY071–PY074</td>
</tr>

<tr>
<td class="org-left">apsw/tests/carray.py</td>
<td class="org-left">PY075–PY076</td>
</tr>

<tr>
<td class="org-left">apsw/tests/extratest.py</td>
<td class="org-left">PY077–PY082</td>
</tr>

<tr>
<td class="org-left">apsw/tests/fork<sub>checker.py</sub></td>
<td class="org-left">PY083–PY086</td>
</tr>

<tr>
<td class="org-left">apsw/tests/ftstests.py</td>
<td class="org-left">PY087–PY090</td>
</tr>

<tr>
<td class="org-left">apsw/tests/jsonb.py</td>
<td class="org-left">PY091–PY093</td>
</tr>

<tr>
<td class="org-left">apsw/tests/sessiontests.py</td>
<td class="org-left">PY094–PY095</td>
</tr>

<tr>
<td class="org-left">apsw/tests/shelltest.py</td>
<td class="org-left">PY061–PY064; shared table helper in PY059</td>
</tr>

<tr>
<td class="org-left">apsw/trace.py</td>
<td class="org-left">PY096–PY103</td>
</tr>

<tr>
<td class="org-left">apsw/unicode.py</td>
<td class="org-left">PY104–PY111</td>
</tr>

<tr>
<td class="org-left">doc/conf.py</td>
<td class="org-left">PY112</td>
</tr>

<tr>
<td class="org-left">examples/async.py</td>
<td class="org-left">PY113–PY116</td>
</tr>

<tr>
<td class="org-left">examples/fts.py</td>
<td class="org-left">PY117–PY120</td>
</tr>

<tr>
<td class="org-left">examples/json.py</td>
<td class="org-left">PY121–PY123</td>
</tr>

<tr>
<td class="org-left">examples/main.py</td>
<td class="org-left">PY124–PY129</td>
</tr>

<tr>
<td class="org-left">examples/session.py</td>
<td class="org-left">PY130–PY132; shared fixed-database root in PY124</td>
</tr>

<tr>
<td class="org-left">setup.py</td>
<td class="org-left">PY133–PY140</td>
</tr>

<tr>
<td class="org-left">src/apswtypes.py</td>
<td class="org-left">PY141–PY148; canonical source for generated stub fixes</td>
</tr>

<tr>
<td class="org-left">tools/aio<sub>bench.py</sub></td>
<td class="org-left">PY149–PY151</td>
</tr>

<tr>
<td class="org-left">tools/checksums.py</td>
<td class="org-left">PY152–PY153</td>
</tr>

<tr>
<td class="org-left">tools/checkversion.py</td>
<td class="org-left">PY154–PY155</td>
</tr>

<tr>
<td class="org-left">tools/code2rst.py</td>
<td class="org-left">PY159–PY165</td>
</tr>

<tr>
<td class="org-left">tools/coverageanalyser.py</td>
<td class="org-left">PY156–PY158</td>
</tr>

<tr>
<td class="org-left">tools/docmissing.py</td>
<td class="org-left">PY173–PY176</td>
</tr>

<tr>
<td class="org-left">tools/docupdate.py</td>
<td class="org-left">PY177–PY179</td>
</tr>

<tr>
<td class="org-left">tools/example2rst.py</td>
<td class="org-left">PY180–PY183</td>
</tr>

<tr>
<td class="org-left">tools/fi.py</td>
<td class="org-left">PY184–PY190</td>
</tr>

<tr>
<td class="org-left">tools/gencompilecommands.py</td>
<td class="org-left">PY191–PY193</td>
</tr>

<tr>
<td class="org-left">tools/genconstants.py</td>
<td class="org-left">PY159–PY160, PY166–PY167</td>
</tr>

<tr>
<td class="org-left">tools/gendocstrings.py</td>
<td class="org-left">PY159–PY160, PY168–PY172</td>
</tr>

<tr>
<td class="org-left">tools/genfaultinject.py</td>
<td class="org-left">PY194–PY197</td>
</tr>

<tr>
<td class="org-left">tools/gensqlitedebug.py</td>
<td class="org-left">PY198–PY200</td>
</tr>

<tr>
<td class="org-left">tools/genstrings.py</td>
<td class="org-left">PY201; C002 duplicate excluded</td>
</tr>

<tr>
<td class="org-left">tools/import<sub>enron.py</sub></td>
<td class="org-left">PY202–PY207</td>
</tr>

<tr>
<td class="org-left">tools/import<sub>html.py</sub></td>
<td class="org-left">PY205, PY208–PY209</td>
</tr>

<tr>
<td class="org-left">tools/json<sub>bench.py</sub></td>
<td class="org-left">PY210–PY214</td>
</tr>

<tr>
<td class="org-left">tools/jsonb<sub>proportion.py</sub></td>
<td class="org-left">PY215–PY216</td>
</tr>

<tr>
<td class="org-left">tools/megatest.py</td>
<td class="org-left">PY217–PY222</td>
</tr>

<tr>
<td class="org-left">tools/names.py</td>
<td class="org-left">PY223–PY225</td>
</tr>

<tr>
<td class="org-left">tools/recipe.py</td>
<td class="org-left">PY226</td>
</tr>

<tr>
<td class="org-left">tools/sqlite<sub>compile</sub><sub>info.py</sub></td>
<td class="org-left">PY227–PY230</td>
</tr>

<tr>
<td class="org-left">tools/sqliteliboptions.py</td>
<td class="org-left">PY231–PY233</td>
</tr>

<tr>
<td class="org-left">tools/types2rst.py</td>
<td class="org-left">PY234–PY238</td>
</tr>

<tr>
<td class="org-left">tools/ucdnames.py</td>
<td class="org-left">PY239–PY241</td>
</tr>

<tr>
<td class="org-left">tools/ucdprops2code.py</td>
<td class="org-left">PY242–PY248</td>
</tr>

<tr>
<td class="org-left">tools/vend.py</td>
<td class="org-left">PY249–PY251</td>
</tr>

<tr>
<td class="org-left">tools/vtbench.py</td>
<td class="org-left">PY252–PY253</td>
</tr>
</tbody>
</table>
