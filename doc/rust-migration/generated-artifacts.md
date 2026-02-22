# Phase 0 Generated Artifact Inventory

This file tracks generated runtime artifacts that must not be hand-edited.

| Artifact | Source of truth | Regeneration command |
|---|---|---|
| `src/constants.c` | `tools/genconstants.py` | `make src/constants.c` |
| `src/stringconstants.c` | `tools/genstrings.py` | `make src/stringconstants.c` |
| `src/apsw.docstrings` | `tools/gendocstrings.py` | `make src/apsw.docstrings` |
| `apsw/__init__.pyi` | `tools/gendocstrings.py` | `make apsw/__init__.pyi` |
| `src/apswversion.h` | `Makefile version variables` | `make src/apswversion.h` |
| `src/faultinject.h` | `tools/genfaultinject.py` | `make src/faultinject.h` |

## Snapshot Regeneration

Generate all Phase 0 baseline snapshots:

```bash
env PYTHONPATH=. python tools/rust_migration_phase0.py --all
```
