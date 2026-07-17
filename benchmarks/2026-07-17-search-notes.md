# Performance search notes: 2026-07-17

All experiments started from source baseline `22ddc459` and were rebuilt
independently.  Changes without a speedtest improvement above 1.5% were
removed and did not receive patch files.

## Newest cache-slot probe

Probing the newest statement-cache slot reduced a repeated cache hit from
188.4 to 134.0 CPU nanoseconds at cache size 128, but improved the full
`statements` workload by only 0.7%.  It was rejected alone, then included in
the accepted statement-cache patch with the exact negative filter.

## Initial hash-counter implementation

Updating hash-bucket counters on every cache hit regressed repeated-hit cost
by 13% to 36%, depending on cache size.  The accepted implementation instead
keeps a statement's bucket membership reserved while the statement is in use,
eliminating hit-side counter writes.

## Pristine `resetcursor` return

Checking all cursor fields before returning from an already clean
`resetcursor()` increased the focused cached-execute cost from 125.8 to 129.2
CPU nanoseconds.  The existing straight-line cleanup was already cheaper.

## One-argument scalar callback

Specializing argument conversion and decref for one-argument Python scalar
callbacks improved the focused callback benchmark from 83.7 to 79.1 CPU
nanoseconds per callback, a 5.5% gain.  The targeted `statements` speedtest
improved from 1.201 to 1.189 seconds, only 1.0%, so the change was rejected.
