from __future__ import annotations

import argparse
import hashlib
import hmac
import io
import os
import struct
import subprocess
import tempfile
import time
import zlib
from pathlib import Path

import zipfile
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


AES_METHOD = 99
AES_EXTRA = 0x9901
AES_VERSION_AE1 = 1
AES_VERSION_AE2 = 2
AES_VENDOR = b"AE"
AES_STRENGTH_256 = 3
AES_EXTRACT_VERSION = 51
AES_SALT_LEN = 16
AES_VERIFIER_LEN = 2
AES_AUTH_LEN = 10


def _bytes(value, label: str) -> bytes | None:
    if value is None or isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    raise TypeError(f"{label}: expected str, bytes, or None, got {type(value).__name__}")


def _derive(password: bytes, salt: bytes) -> tuple[bytes, bytes, bytes]:
    material = hashlib.pbkdf2_hmac("sha1", password, salt, 1000, dklen=66)
    return material[:32], material[32:64], material[64:66]


def _crypt(data: bytes, key: bytes) -> bytes:
    cipher = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
    out = bytearray()
    for counter, start in enumerate(range(0, len(data), 16), 1):
        out.extend(a ^ b for a, b in zip(data[start : start + 16], cipher.update(counter.to_bytes(16, "little"))))
    cipher.finalize()
    return bytes(out)


def _compress(method: int, level: int | None, data: bytes) -> bytes:
    compressor = zipfile._get_compressor(method, level)
    return data if compressor is None else compressor.compress(data) + compressor.flush()


def _decompress(method: int, data: bytes) -> bytes:
    decompressor = zipfile._get_decompressor(method)
    if decompressor is None:
        return data
    out = decompressor.decompress(data)
    flush = getattr(decompressor, "flush", None)
    return out if flush is None else out + flush()


def _aes_extra(method: int) -> bytes:
    payload = struct.pack("<H2sBH", AES_VERSION_AE2, AES_VENDOR, AES_STRENGTH_256, method)
    return struct.pack("<HH", AES_EXTRA, len(payload)) + payload


def _aes_meta(zinfo: zipfile.ZipInfo) -> tuple[int, bytes, int, int] | None:
    if zinfo.compress_type != AES_METHOD:
        return None
    for field in zipfile._Extra.split(zinfo.extra):
        if field.id == AES_EXTRA:
            if len(field) != 11:
                raise zipfile.BadZipFile(f"Invalid AES extra field for {zinfo.filename!r}")
            return struct.unpack("<H2sBH", field[4:])
    raise zipfile.BadZipFile(f"AES member {zinfo.filename!r} is missing extra field 0x9901")


def _writer_info(zf: zipfile.ZipFile, name: str) -> zipfile.ZipInfo:
    zinfo = zipfile.ZipInfo(name, time.localtime(time.time())[:6])
    zinfo.compress_type = zf.compression
    zinfo.compress_level = zf.compresslevel
    if zinfo.filename.endswith("/"):
        zinfo.external_attr = 0o40775 << 16
        zinfo.external_attr |= 0x10
    else:
        zinfo.external_attr = 0o600 << 16
    helper = getattr(zinfo, "_for_archive", None)
    return helper(zf) if helper else zinfo


class _AESWriter(io.BytesIO):
    def __init__(self, zf: "AESZipFile", zinfo: zipfile.ZipInfo, force_zip64: bool):
        super().__init__()
        self._zipfile = zf
        self._zinfo = zinfo
        self._force_zip64 = force_zip64

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._zipfile._write_aes(self._zinfo, self.getvalue(), self._force_zip64)
        finally:
            self._zipfile._writing = False
            super().close()


class AESZipFile(zipfile.ZipFile):
    def __init__(self, file, mode: str = "r", *, password=None, **kwargs):
        super().__init__(file, mode=mode, **kwargs)
        self.pwd = _bytes(password, "password")

    def setpassword(self, pwd) -> None:
        pwd = _bytes(pwd, "pwd")
        self.pwd = pwd

    def open(self, name, mode: str = "r", pwd=None, *, force_zip64: bool = False):
        if mode == "w" and self.pwd:
            if pwd is not None:
                raise ValueError("pwd is only supported for reading files")
            if not self.fp:
                raise ValueError("Attempt to use ZIP archive that was already closed")
            if self._writing:
                raise ValueError("Can't write to the ZIP file while another write handle is open")
            zinfo = name if isinstance(name, zipfile.ZipInfo) else _writer_info(self, name)
            if zinfo.is_dir():
                return super().open(zinfo, mode=mode, force_zip64=force_zip64)
            self._writing = True
            return _AESWriter(self, zinfo, force_zip64)

        if mode == "r":
            zinfo = name if isinstance(name, zipfile.ZipInfo) else self.getinfo(name)
            aes = _aes_meta(zinfo)
            if aes:
                return self._open_aes(zinfo, aes, pwd)

        return super().open(name, mode=mode, pwd=pwd, force_zip64=force_zip64)

    def _write_aes(self, zinfo: zipfile.ZipInfo, data: bytes, force_zip64: bool = False) -> None:
        method = zinfo.compress_type
        zipfile._check_compression(method)

        compressed = _compress(method, getattr(zinfo, "compress_level", None), data)
        salt = os.urandom(AES_SALT_LEN)
        enc_key, mac_key, verifier = _derive(self.pwd, salt)
        ciphertext = _crypt(compressed, enc_key)
        auth = hmac.new(mac_key, ciphertext, hashlib.sha1).digest()[:AES_AUTH_LEN]

        zinfo.file_size = len(data)
        zinfo.compress_size = AES_SALT_LEN + AES_VERIFIER_LEN + len(ciphertext) + AES_AUTH_LEN
        zip64 = force_zip64 or zinfo.file_size > zipfile.ZIP64_LIMIT or zinfo.compress_size > zipfile.ZIP64_LIMIT

        if self._seekable:
            self.fp.seek(self.start_dir)
        zinfo.header_offset = self.fp.tell()
        self._writecheck(zinfo)
        if zip64 and not self._allowZip64:
            raise zipfile.LargeZipFile("Filesize would require ZIP64 extensions")

        zinfo.CRC = 0
        zinfo.flag_bits = zipfile._MASK_ENCRYPTED
        if method == zipfile.ZIP_LZMA:
            zinfo.flag_bits |= zipfile._MASK_COMPRESS_OPTION_1
        zinfo.compress_type = AES_METHOD
        zinfo.extract_version = max(zinfo.extract_version, AES_EXTRACT_VERSION)
        zinfo.create_version = max(zinfo.create_version, AES_EXTRACT_VERSION)
        zinfo.extra = zipfile._Extra.strip(zinfo.extra, (AES_EXTRA,)) + _aes_extra(method)
        if not zinfo.external_attr:
            zinfo.external_attr = 0o600 << 16

        self._didModify = True
        self.fp.write(zinfo.FileHeader(zip64))
        self.fp.write(salt)
        self.fp.write(verifier)
        self.fp.write(ciphertext)
        self.fp.write(auth)
        self.start_dir = self.fp.tell()
        self.filelist.append(zinfo)
        self.NameToInfo[zinfo.filename] = zinfo

    def _open_aes(self, zinfo: zipfile.ZipInfo, aes: tuple[int, bytes, int, int], pwd):
        password = _bytes(pwd, "pwd") if pwd is not None else self.pwd
        if not password:
            raise RuntimeError(f"File {zinfo.filename!r} is encrypted, password required for extraction")

        version, vendor, strength, method = aes
        if vendor != AES_VENDOR or strength != AES_STRENGTH_256:
            raise NotImplementedError("This minimal AES example supports WinZip AES-256 only")

        with self._lock:
            self.fp.seek(zinfo.header_offset)
            header = self.fp.read(zipfile.sizeFileHeader)
            if len(header) != zipfile.sizeFileHeader:
                raise zipfile.BadZipFile("Truncated file header")
            header = struct.unpack(zipfile.structFileHeader, header)
            if header[zipfile._FH_SIGNATURE] != zipfile.stringFileHeader:
                raise zipfile.BadZipFile("Bad magic number for file header")
            raw_name = self.fp.read(header[zipfile._FH_FILENAME_LENGTH])
            self.fp.read(header[zipfile._FH_EXTRA_FIELD_LENGTH])
            name = raw_name.decode(
                "utf-8"
                if header[zipfile._FH_GENERAL_PURPOSE_FLAG_BITS] & zipfile._MASK_UTF_FILENAME
                else self.metadata_encoding or "cp437"
            )
            if name != zinfo.orig_filename:
                raise zipfile.BadZipFile(f"Header name {name!r} does not match directory name {zinfo.orig_filename!r}")
            payload = self.fp.read(zinfo.compress_size)

        if len(payload) < AES_SALT_LEN + AES_VERIFIER_LEN + AES_AUTH_LEN:
            raise zipfile.BadZipFile(f"Truncated AES payload for {zinfo.filename!r}")

        salt = payload[:AES_SALT_LEN]
        verifier = payload[AES_SALT_LEN : AES_SALT_LEN + AES_VERIFIER_LEN]
        ciphertext = payload[AES_SALT_LEN + AES_VERIFIER_LEN : -AES_AUTH_LEN]
        auth = payload[-AES_AUTH_LEN:]

        enc_key, mac_key, expected = _derive(password, salt)
        if verifier != expected:
            raise RuntimeError(f"Bad password for file {zinfo.filename!r}")
        if hmac.new(mac_key, ciphertext, hashlib.sha1).digest()[:AES_AUTH_LEN] != auth:
            raise zipfile.BadZipFile(f"AES authentication failed for {zinfo.filename!r}")

        data = _decompress(method, _crypt(ciphertext, enc_key))
        if len(data) != zinfo.file_size:
            raise zipfile.BadZipFile(f"Bad size for {zinfo.filename!r}")
        if version == AES_VERSION_AE1 and zlib.crc32(data) & 0xFFFFFFFF != zinfo.CRC:
            raise zipfile.BadZipFile(f"Bad CRC-32 for {zinfo.filename!r}")
        return io.BytesIO(data)


def _run(cmd: list[str], cwd: Path) -> None:
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=True)
    if proc.stdout.strip():
        print(proc.stdout.strip())
    if proc.stderr.strip():
        print(proc.stderr.strip())


def demo(workdir: Path) -> None:
    password = "secret123"
    source = workdir / "hello.txt"
    source.write_text("hello from AES zip\n", encoding="utf-8")

    created = workdir / "python-aes.zip"
    created.unlink(missing_ok=True)
    with AESZipFile(created, "w", compression=zipfile.ZIP_DEFLATED, password=password) as zf:
        zf.write(source, arcname="hello.txt")
        zf.writestr("folder/note.txt", "second file\n")

    with AESZipFile(created, password=password) as zf:
        assert zf.read("hello.txt") == source.read_bytes()
        assert zf.read("folder/note.txt") == b"second file\n"

    print(f"created {created}")
    _run(["7za", "t", f"-p{password}", created.name], workdir)
    _run(["bsdtar", "-tf", created.name, "--passphrase", password], workdir)
    extracted = subprocess.run(
        ["bsdtar", "-xOf", created.name, "--passphrase", password, "hello.txt"],
        cwd=workdir,
        capture_output=True,
        check=True,
    )
    assert extracted.stdout == source.read_bytes()
    print("bsdtar extracted hello.txt correctly")

    from_7za = workdir / "from-7za.zip"
    from_7za.unlink(missing_ok=True)
    _run(["7za", "a", "-tzip", "-mem=AES256", f"-p{password}", from_7za.name, source.name], workdir)
    with AESZipFile(from_7za, password=password) as zf:
        assert zf.read("hello.txt") == source.read_bytes()
    print(f"read {from_7za} with AESZipFile")

    from_bsdtar = workdir / "from-bsdtar.zip"
    from_bsdtar.unlink(missing_ok=True)
    _run(
        [
            "bsdtar",
            "-cf",
            from_bsdtar.name,
            "--format=zip",
            "--options=zip:compression=deflate,zip:encryption=aes256",
            "--passphrase",
            password,
            source.name,
        ],
        workdir,
    )
    with AESZipFile(from_bsdtar, password=password) as zf:
        assert zf.read("hello.txt") == source.read_bytes()
    print(f"read {from_bsdtar} with AESZipFile")

    forced_zip64 = workdir / "forced-zip64.zip"
    forced_zip64.unlink(missing_ok=True)
    with AESZipFile(forced_zip64, "w", compression=zipfile.ZIP_STORED, password=password, allowZip64=True) as zf:
        with zf.open("zip64.txt", "w", force_zip64=True) as fp:
            fp.write(b"zip64 data\n")
    with AESZipFile(forced_zip64, password=password) as zf:
        assert zf.read("zip64.txt") == b"zip64 data\n"
    _run(["7za", "t", f"-p{password}", forced_zip64.name], workdir)
    _run(["bsdtar", "-tf", forced_zip64.name, "--passphrase", password], workdir)
    extracted = subprocess.run(
        ["bsdtar", "-xOf", forced_zip64.name, "--passphrase", password, "zip64.txt"],
        cwd=workdir,
        capture_output=True,
        check=True,
    )
    assert extracted.stdout == b"zip64 data\n"
    print(f"read {forced_zip64} with forced ZIP64 local headers")
    print("all checks passed")


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal zipfile AES-256 extension demo.")
    parser.add_argument("--workdir", type=Path)
    args = parser.parse_args()
    if args.workdir:
        args.workdir.mkdir(parents=True, exist_ok=True)
        demo(args.workdir.resolve())
        return
    with tempfile.TemporaryDirectory(prefix="aeszip-demo-", dir=Path(__file__).resolve().parent) as tmp:
        demo(Path(tmp))


if __name__ == "__main__":
    main()
