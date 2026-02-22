#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import inspect
import json
import pathlib
import sys
from typing import Any
from collections.abc import Callable


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "doc" / "rust-migration"


def fq_name(obj: object) -> str:
    cls = obj if inspect.isclass(obj) else obj.__class__
    return f"{cls.__module__}.{cls.__qualname__}"


def safe_signature(obj: Any) -> str | None:
    with contextlib.suppress(TypeError, ValueError):
        return str(inspect.signature(obj))
    return None


def classify_member(obj: object) -> str:
    if inspect.isclass(obj):
        return "class"
    if inspect.isbuiltin(obj) or inspect.isfunction(obj) or inspect.ismethoddescriptor(obj):
        return "callable"
    if callable(obj):
        return "callable"
    if isinstance(obj, (bool, int, float, str, bytes, bytearray, memoryview, type(None))):
        return "scalar"
    if isinstance(obj, (tuple, list, dict, set, frozenset)):
        return "container"
    return fq_name(obj)


def exception_record(func: Callable[[], object]) -> dict[str, object]:
    try:
        value = func()
    except Exception as exc:  # noqa: BLE001
        return {"raised": fq_name(exc), "message": str(exc)}
    return {"raised": None, "result_type": fq_name(value)}


def class_members(klass: type) -> dict[str, list[str]]:
    methods: list[str] = []
    properties: list[str] = []
    descriptors: list[str] = []
    attributes: list[str] = []

    for name in sorted(dir(klass)):
        try:
            static_attr = inspect.getattr_static(klass, name)
        except AttributeError:
            continue
        if isinstance(static_attr, property):
            properties.append(name)
            continue
        if inspect.isdatadescriptor(static_attr) and not callable(static_attr):
            descriptors.append(name)
            continue
        if callable(static_attr) or inspect.ismethoddescriptor(static_attr):
            methods.append(name)
            continue
        attributes.append(name)

    return {
        "methods": methods,
        "properties": properties,
        "descriptors": descriptors,
        "attributes": attributes,
    }


def write_json(path: pathlib.Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf8")


def snapshot_public_surface(output_dir: pathlib.Path) -> pathlib.Path:
    import apsw

    names = sorted(dir(apsw))
    module_kinds: dict[str, str] = {}
    callables: dict[str, str | None] = {}
    classes: dict[str, dict[str, object]] = {}
    exceptions: dict[str, dict[str, object]] = {}
    constants: dict[str, int | float | str | bool | None] = {}

    for name in names:
        obj = getattr(apsw, name)
        module_kinds[name] = classify_member(obj)

        if callable(obj):
            callables[name] = safe_signature(obj)

        if inspect.isclass(obj):
            class_info: dict[str, object] = {
                "module": obj.__module__,
                "qualname": obj.__qualname__,
                "signature": safe_signature(obj),
                "mro": [f"{base.__module__}.{base.__qualname__}" for base in obj.__mro__],
            }
            class_info.update(class_members(obj))
            classes[name] = class_info
            if issubclass(obj, Exception):
                exceptions[name] = {
                    "module": obj.__module__,
                    "qualname": obj.__qualname__,
                    "mro": [f"{base.__module__}.{base.__qualname__}" for base in obj.__mro__],
                }

        if name.isupper() and isinstance(obj, (int, float, str, bool, type(None))):
            constants[name] = obj

    payload: dict[str, object] = {
        "schema_version": 1,
        "apsw_module": {
            "module_filename": pathlib.Path(apsw.__file__).name,
            "names": names,
            "member_kind": module_kinds,
            "callables": callables,
            "constants": constants,
        },
        "classes": classes,
        "exceptions": exceptions,
    }

    output = output_dir / "public-surface.json"
    write_json(output, payload)
    return output


def snapshot_behavior_contract(output_dir: pathlib.Path) -> pathlib.Path:
    import apsw

    def call(name: str, *args: object) -> object:
        return getattr(apsw, name)(*args)

    probes: dict[str, object] = {}
    probes["repr_no_change"] = repr(apsw.no_change)
    probes["str_no_change"] = str(apsw.no_change)
    probes["main_owner"] = getattr(getattr(apsw, "main"), "__module__", None)
    probes["shell_owner"] = getattr(getattr(apsw, "Shell"), "__module__", None)
    probes["async_run_coro_default_is_none"] = apsw.async_run_coro is None

    probes["setattr_async_controller_none"] = exception_record(lambda: setattr(apsw, "async_controller", None))
    probes["setattr_async_cursor_prefetch_none"] = exception_record(
        lambda: setattr(apsw, "async_cursor_prefetch", None)
    )
    probes["setattr_async_run_coro_invalid"] = exception_record(lambda: setattr(apsw, "async_run_coro", 3))
    probes["getattr_missing_attribute"] = exception_record(
        lambda: getattr(apsw, "this_attribute_should_not_exist_for_contract")
    )

    def set_async_runner_and_roundtrip() -> bool:
        def runner() -> None:
            return None

        setattr(apsw, "async_run_coro", runner)
        return apsw.async_run_coro is runner

    probes["setattr_async_run_coro_valid_roundtrip"] = exception_record(set_async_runner_and_roundtrip)
    setattr(apsw, "async_run_coro", None)

    probes["argparse_apsw_version_extra_arg"] = exception_record(lambda: call("apsw_version", 1))
    probes["argparse_format_sql_value_missing_arg"] = exception_record(lambda: call("format_sql_value"))
    probes["argparse_exception_for_missing_arg"] = exception_record(lambda: call("exception_for"))
    probes["argparse_exception_for_wrong_type"] = exception_record(lambda: call("exception_for", "x"))
    probes["argparse_connection_missing_arg"] = exception_record(lambda: call("Connection"))
    probes["argparse_status_missing_arg"] = exception_record(lambda: call("status"))
    probes["argparse_complete_wrong_type"] = exception_record(lambda: call("complete", 1))

    payload: dict[str, object] = {
        "schema_version": 1,
        "probes": probes,
    }

    output = output_dir / "behavior-contract.json"
    write_json(output, payload)
    return output


def snapshot_compile_options(output_dir: pathlib.Path) -> pathlib.Path:
    import apsw

    module_options = sorted(str(item) for item in apsw.compile_options)

    con = apsw.Connection("")
    try:
        pragma_options = sorted(str(item) for item in con.pragma("compile_options"))
    finally:
        con.close()

    module_set = set(module_options)
    pragma_set = set(pragma_options)

    payload: dict[str, object] = {
        "schema_version": 1,
        "apsw_module_compile_options": module_options,
        "sqlite_pragma_compile_options": pragma_options,
        "only_in_apsw_module": sorted(module_set - pragma_set),
        "only_in_sqlite_pragma": sorted(pragma_set - module_set),
    }

    output = output_dir / "compile-options.json"
    write_json(output, payload)
    return output


def snapshot_generated_artifacts(output_dir: pathlib.Path) -> pathlib.Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    entries = [
        ("src/constants.c", "tools/genconstants.py", "make src/constants.c"),
        ("src/stringconstants.c", "tools/genstrings.py", "make src/stringconstants.c"),
        (
            "src/apsw.docstrings",
            "tools/gendocstrings.py",
            "make src/apsw.docstrings",
        ),
        (
            "apsw/__init__.pyi",
            "tools/gendocstrings.py",
            "make apsw/__init__.pyi",
        ),
        (
            "src/apswversion.h",
            "Makefile version variables",
            "make src/apswversion.h",
        ),
        ("src/faultinject.h", "tools/genfaultinject.py", "make src/faultinject.h"),
    ]

    lines = [
        "# Phase 0 Generated Artifact Inventory",
        "",
        "This file tracks generated runtime artifacts that must not be hand-edited.",
        "",
        "| Artifact | Source of truth | Regeneration command |",
        "|---|---|---|",
    ]

    for artifact, source, command in entries:
        lines.append(f"| `{artifact}` | `{source}` | `{command}` |")

    lines.extend(
        [
            "",
            "## Snapshot Regeneration",
            "",
            "Generate all Phase 0 baseline snapshots:",
            "",
            "```bash",
            "env PYTHONPATH=. python tools/rust_migration_phase0.py --all",
            "```",
        ]
    )

    output = output_dir / "generated-artifacts.md"
    output.write_text("\n".join(lines) + "\n", encoding="utf8")
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate APSW->Rust migration Phase 0 baselines")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where baseline artifacts are written",
    )
    parser.add_argument("--public-surface", action="store_true", help="Generate public-surface.json")
    parser.add_argument("--behavior-contract", action="store_true", help="Generate behavior-contract.json")
    parser.add_argument("--compile-options", action="store_true", help="Generate compile-options.json")
    parser.add_argument("--generated-artifacts", action="store_true", help="Generate generated-artifacts.md")
    parser.add_argument("--all", action="store_true", help="Generate all Phase 0 baseline artifacts")
    return parser.parse_args()


def main() -> int:
    options = parse_args()
    output_dir = pathlib.Path(options.output_dir).resolve()

    wants_public = options.all or options.public_surface
    wants_behavior = options.all or options.behavior_contract
    wants_compile = options.all or options.compile_options
    wants_generated = options.all or options.generated_artifacts

    if not any((wants_public, wants_behavior, wants_compile, wants_generated)):
        print("No snapshot type selected. Use --all or one of the specific flags.", file=sys.stderr)
        return 2

    generated: list[pathlib.Path] = []
    try:
        if wants_public:
            generated.append(snapshot_public_surface(output_dir))
        if wants_behavior:
            generated.append(snapshot_behavior_contract(output_dir))
        if wants_compile:
            generated.append(snapshot_compile_options(output_dir))
        if wants_generated:
            generated.append(snapshot_generated_artifacts(output_dir))
    except ImportError as exc:
        print("Unable to import apsw. Build in-place first (`make build_ext`).", file=sys.stderr)
        print(f"Import error: {exc}", file=sys.stderr)
        return 1

    for path in generated:
        print(path.relative_to(PROJECT_ROOT))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
