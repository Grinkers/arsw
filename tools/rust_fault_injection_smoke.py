#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path


PROCEED = 0x1FACADE
PROCEED_RETURN18 = 0x2FACADE

ALL_NAMED_FAULTS = {
    "APSWVFSBadVersion",
    "xUnlockFails",
    "xSyncFails",
    "xFileSizeFails",
    "xCheckReservedLockFails",
    "xCheckReservedLockIsTrue",
    "xCloseFails",
    "xTokenCBFlagsBad",
    "xTokenCBOffsetsBad",
    "xTokenCBColocatedBad",
    "TokenizeRC",
    "TokenizeRC2",
    "xRowCountErr",
    "xSetAuxDataErr",
    "xQueryTokenErr",
    "xInstCountErr",
    "xTokenizeErr",
    "ConnectionAsyncTpNewFails",
    "ConnectionReadError",
    "FTS5TokenizerRegister",
    "FTS5FunctionRegister",
    "BlobWriteTooBig",
}

VFS_NAMED_FAULTS = {
    "APSWVFSBadVersion",
    "xUnlockFails",
    "xSyncFails",
    "xFileSizeFails",
    "xCheckReservedLockFails",
    "xCheckReservedLockIsTrue",
    "xCloseFails",
}

TOKENIZER_NAMED_FAULTS = {
    "xTokenCBFlagsBad",
    "xTokenCBOffsetsBad",
    "xTokenCBColocatedBad",
    "TokenizeRC",
    "TokenizeRC2",
    "xRowCountErr",
    "xSetAuxDataErr",
    "xQueryTokenErr",
    "xInstCountErr",
    "xTokenizeErr",
}


def load_apsw_module(module_path: Path):
    spec = importlib.util.spec_from_file_location("apsw", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["apsw"] = module
    spec.loader.exec_module(module)
    return module


def expect_raises(exc_type: type[BaseException], func, label: str) -> None:
    try:
        func()
    except exc_type:
        return
    except Exception as exc:  # pragma: no cover - smoke diagnostics
        raise AssertionError(f"{label}: expected {exc_type.__name__}, got {type(exc).__name__}: {exc}") from exc
    raise AssertionError(f"{label}: expected {exc_type.__name__}")


def prepare_blob_connection(apsw):
    con = apsw.Connection("")
    con.execute("create table if not exists __fi_blob(data blob)")
    con.execute("delete from __fi_blob")
    rowid = con.execute("insert into __fi_blob values(zeroblob(32)) returning rowid").fetchone()[0]
    blob = con.blob_open("main", "__fi_blob", "data", rowid, True)
    return con, blob


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--module", required=True, help="Path to Rust-built apsw shared object")
    args = parser.parse_args()

    module_path = Path(args.module)
    if not module_path.exists():
        raise SystemExit(f"Module not found: {module_path}")

    apsw = load_apsw_module(module_path)

    seen_fault_names: list[str] = []
    seen_control_apis: list[str] = []
    should_fault_names: set[str] = set()
    control_overrides: dict[str, object] = {}

    def apsw_should_fault(name, pending_exception):
        if not isinstance(name, str):
            raise TypeError(f"fault name must be str, got {type(name)}")
        if not isinstance(pending_exception, tuple) or len(pending_exception) != 3:
            raise TypeError(f"pending_exception must be 3-item tuple, got {pending_exception!r}")
        seen_fault_names.append(name)
        return name in should_fault_names

    def apsw_fault_inject_control(key):
        if not isinstance(key, tuple) or len(key) != 5:
            raise TypeError(f"fault key must be 5-item tuple, got {key!r}")
        api, filename, funcname, linenum, arg_text = key
        if not isinstance(api, str):
            raise TypeError(f"api must be str, got {type(api)}")
        if not isinstance(filename, str):
            raise TypeError(f"filename must be str, got {type(filename)}")
        if not isinstance(funcname, str):
            raise TypeError(f"funcname must be str, got {type(funcname)}")
        if not isinstance(linenum, int):
            raise TypeError(f"linenum must be int, got {type(linenum)}")
        if not isinstance(arg_text, str):
            raise TypeError(f"arg_text must be str, got {type(arg_text)}")
        seen_control_apis.append(api)
        return control_overrides.get(api, PROCEED)

    old_should = getattr(sys, "apsw_should_fault", None)
    old_control = getattr(sys, "apsw_fault_inject_control", None)
    setattr(sys, "apsw_should_fault", apsw_should_fault)
    setattr(sys, "apsw_fault_inject_control", apsw_fault_inject_control)

    try:
        for fault_name in sorted(ALL_NAMED_FAULTS):
            should_fault_names = {fault_name}
            control_overrides = {}

            if fault_name == "ConnectionAsyncTpNewFails":
                expect_raises(MemoryError, lambda: apsw.Connection(""), fault_name)
                continue

            if fault_name in VFS_NAMED_FAULTS:
                expect_raises(OSError, lambda: apsw.vfs_details(), fault_name)
                continue

            if fault_name == "FTS5TokenizerRegister":
                con = apsw.Connection("")
                try:
                    expect_raises(
                        apsw.TooBigError,
                        lambda: con.register_fts5_tokenizer("tok_fault", lambda *_: lambda *_: []),
                        fault_name,
                    )
                finally:
                    con.close()
                continue

            if fault_name == "FTS5FunctionRegister":
                con = apsw.Connection("")
                try:
                    expect_raises(
                        apsw.TooBigError,
                        lambda: con.register_fts5_function("identity", lambda api, value: value),
                        fault_name,
                    )
                finally:
                    con.close()
                continue

            if fault_name in TOKENIZER_NAMED_FAULTS:
                con = apsw.Connection("")
                try:
                    con.register_fts5_tokenizer("tok_call", lambda *_: lambda *_: [])
                    tokenizer = con.fts5_tokenizer("tok_call", [])
                    expect_raises(
                        Exception, lambda: tokenizer(b"abcdef", apsw.FTS5_TOKENIZE_DOCUMENT, None), fault_name
                    )
                finally:
                    con.close()
                continue

            if fault_name == "BlobWriteTooBig":
                con, blob = prepare_blob_connection(apsw)
                try:
                    expect_raises(ValueError, lambda: blob.write(b"X"), fault_name)
                finally:
                    blob.close()
                    con.close()
                continue

            if fault_name == "ConnectionReadError":
                con, blob = prepare_blob_connection(apsw)
                try:
                    expect_raises(OSError, lambda: blob.read(1), fault_name)
                finally:
                    blob.close()
                    con.close()
                continue

            raise AssertionError(f"Unhandled fault name in smoke script: {fault_name}")

        should_fault_names = set()
        control_overrides = {"sqlite3_create_function_v2": PROCEED_RETURN18}
        con = apsw.Connection("")
        try:
            expect_raises(apsw.TooBigError, lambda: con.create_scalar_function("f", lambda x: x, 1), "ProceedReturn18")

            control_overrides = {
                "sqlite3_prepare_v3": (-1, apsw.Error, "Fault injection synthesized failure"),
            }
            expect_raises(apsw.Error, lambda: con.execute("select 1").fetchall(), "TupleReturnProtocol")
        finally:
            con.close()

        should_fault_names = set()
        control_overrides = {}
        con, blob = prepare_blob_connection(apsw)
        try:
            blob.write(b"A")
            blob.seek(0)
            blob.read(1)
        finally:
            blob.close()
            con.close()

        con = apsw.Connection("")
        try:
            con.create_collation("fi_coll", lambda one, two: 0)
            con.create_collation("fi_coll", None)
            con.execute("select ?", (1,)).fetchall()
        finally:
            con.close()

        try:
            apsw.session_config(0)
        except Exception:
            pass

        control_overrides = {"sqlite3_open_v2": apsw.SQLITE_CANTOPEN}
        expect_raises(apsw.CantOpenError, lambda: apsw.Connection(""), "OpenControlOverride")

    finally:
        if old_should is None:
            delattr(sys, "apsw_should_fault")
        else:
            setattr(sys, "apsw_should_fault", old_should)

        if old_control is None:
            delattr(sys, "apsw_fault_inject_control")
        else:
            setattr(sys, "apsw_fault_inject_control", old_control)

    missing_faults = ALL_NAMED_FAULTS - set(seen_fault_names)
    if missing_faults:
        raise AssertionError(f"Missing named fault callbacks: {sorted(missing_faults)}")

    expected_controls = {
        "sqlite3_open_v2",
        "sqlite3_close_v2",
        "sqlite3_prepare_v3",
        "sqlite3_create_function_v2",
        "sqlite3_blob_open",
        "sqlite3_blob_read",
        "sqlite3_blob_write",
        "sqlite3_blob_close",
        "sqlite3_bind_int64",
        "sqlite3_create_collation_v2",
        "sqlite3session_config",
    }
    missing_controls = expected_controls - set(seen_control_apis)
    if missing_controls:
        raise AssertionError(f"Missing control callbacks: {sorted(missing_controls)}")

    print("rust fault smoke ok", f"named={len(seen_fault_names)}", f"control={len(seen_control_apis)}")


if __name__ == "__main__":
    main()
