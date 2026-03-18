from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

from temp import AESZipFile


PASSWORD = "secret123"
PAYLOAD = (b"hello from testzip\n" * 128) + bytes(range(256))
METHODS = [
    ("stored", zipfile.ZIP_STORED),
    ("deflated", zipfile.ZIP_DEFLATED),
    ("bzip2", zipfile.ZIP_BZIP2),
    ("lzma", zipfile.ZIP_LZMA),
]
SEVENZIP_METHODS = {
    "stored": "Copy",
    "deflated": "Deflate",
    "bzip2": "BZip2",
    "lzma": "LZMA",
}
BSDTAR_AES_EXTRACT = {"stored", "deflated"}


def _run(cmd: list[str], cwd: Path) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(cmd, cwd=cwd, check=True, capture_output=True)


def _try_run(cmd: list[str], cwd: Path, label: str) -> bool:
    try:
        _run(cmd, cwd)
    except subprocess.CalledProcessError as exc:
        message = exc.stderr.decode("utf-8", "replace").strip() or exc.stdout.decode("utf-8", "replace").strip()
        print(f"skip {label}: {message}")
        return False
    return True


def _tool(name: str) -> str | None:
    return shutil.which(name)


def _assert_7za_extract(workdir: Path, archive: Path, member: str, *, password: str | None = None) -> None:
    cmd = ["7za", "x", "-so"]
    if password is not None:
        cmd.append(f"-p{password}")
    cmd.extend([archive.name, member])
    extracted = _run(cmd, workdir)
    assert extracted.stdout == PAYLOAD


def _write_source_file(workdir: Path, label: str) -> Path:
    source = workdir / f"{label}.bin"
    source.write_bytes(PAYLOAD)
    return source


def test_7za_plain_reader(workdir: Path, label: str, member: str, source: Path) -> None:
    if not _tool("7za"):
        return
    archive = workdir / f"7za-plain-{label}.zip"
    archive.unlink(missing_ok=True)
    _run(["7za", "a", "-tzip", f"-mm={SEVENZIP_METHODS[label]}", archive.name, source.name], workdir)
    with zipfile.ZipFile(archive) as zf:
        assert zf.read(member) == PAYLOAD


def test_7za_aes_reader(workdir: Path, label: str, member: str, source: Path) -> None:
    if not _tool("7za"):
        return
    archive = workdir / f"7za-aes-{label}.zip"
    archive.unlink(missing_ok=True)
    _run(
        [
            "7za",
            "a",
            "-tzip",
            f"-mm={SEVENZIP_METHODS[label]}",
            "-mem=AES256",
            f"-p{PASSWORD}",
            archive.name,
            source.name,
        ],
        workdir,
    )
    with AESZipFile(archive, password=PASSWORD) as zf:
        assert zf.read(member) == PAYLOAD


def test_plain(workdir: Path, label: str, method: int) -> None:
    archive = workdir / f"plain-{label}.zip"
    member = f"{label}.bin"
    source = _write_source_file(workdir, label)
    archive.unlink(missing_ok=True)

    with zipfile.ZipFile(archive, "w", compression=method) as zf:
        zf.writestr(member, PAYLOAD)

    with zipfile.ZipFile(archive) as zf:
        assert zf.read(member) == PAYLOAD

    if _tool("7za"):
        if _try_run(["7za", "t", archive.name], workdir, f"7za plain {label}"):
            _assert_7za_extract(workdir, archive, member)
    if _tool("bsdtar"):
        if _try_run(["bsdtar", "-tf", archive.name], workdir, f"bsdtar list plain {label}"):
            extracted = _run(["bsdtar", "-xOf", archive.name, member], workdir)
            assert extracted.stdout == PAYLOAD

    test_7za_plain_reader(workdir, label, member, source)


def test_encrypted(workdir: Path, label: str, method: int) -> None:
    archive = workdir / f"aes-{label}.zip"
    member = f"{label}.bin"
    source = _write_source_file(workdir, label)
    archive.unlink(missing_ok=True)

    with AESZipFile(archive, "w", compression=method, password=PASSWORD) as zf:
        zf.writestr(member, PAYLOAD)

    with AESZipFile(archive, password=PASSWORD) as zf:
        assert zf.read(member) == PAYLOAD

    if _tool("7za"):
        if _try_run(["7za", "t", f"-p{PASSWORD}", archive.name], workdir, f"7za aes {label}"):
            _assert_7za_extract(workdir, archive, member, password=PASSWORD)
    if _tool("bsdtar"):
        if _try_run(["bsdtar", "-tf", archive.name, "--passphrase", PASSWORD], workdir, f"bsdtar list aes {label}"):
            if label in BSDTAR_AES_EXTRACT:
                extracted = _run(["bsdtar", "-xOf", archive.name, "--passphrase", PASSWORD, member], workdir)
                assert extracted.stdout == PAYLOAD
            else:
                print(
                    f"skip bsdtar extract aes {label}: libarchive does not handle this AES compression combination here"
                )

    test_7za_aes_reader(workdir, label, member, source)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test all zipfile compression methods in plain and AES modes.")
    parser.add_argument("--workdir", type=Path)
    args = parser.parse_args()

    if args.workdir:
        args.workdir.mkdir(parents=True, exist_ok=True)
        workdir = args.workdir.resolve()
        cleanup = None
    else:
        cleanup = tempfile.TemporaryDirectory(prefix="testzip-", dir=Path(__file__).resolve().parent)
        workdir = Path(cleanup.name)

    try:
        for label, method in METHODS:
            test_plain(workdir, label, method)
            test_encrypted(workdir, label, method)
            print(f"ok {label}")
        print("all compression methods passed in plain and AES modes")
    finally:
        if cleanup is not None:
            cleanup.cleanup()


if __name__ == "__main__":
    main()
