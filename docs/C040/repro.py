"""C040 repro: resetcursor returns SQLITE_OK with an iterator exception pending.

Requires an apsw build.  Debug builds (asserts enabled) abort at
src/cursor.c:222 `assert(res)`.  Release builds surface the generator's
exception instead of IncompleteExecutionError from execute()/close().

Run: PYTHONPATH=/path/to/arsw python3 repro.py
"""

import apsw

def gen():
    yield (1,)
    raise ValueError("boom")

con = apsw.Connection("")
cur = con.cursor()

# Leaves status != C_DONE with emiter set (first row still pending).
cur.executemany("select ?", gen())

# resetcursor(force=0) probes gen -> raises.  res stays SQLITE_OK while the
# exception is pending.
cur.execute("select 2")

# Same via close():
# cur.close()
