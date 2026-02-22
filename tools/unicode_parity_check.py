#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.machinery
import importlib.util
import sys


def load_rust_unicode(path: str):
    loader = importlib.machinery.ExtensionFileLoader("_unicode", path)
    spec = importlib.util.spec_from_loader("_unicode", loader)
    if spec is None:
        raise RuntimeError(f"Unable to create import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def capture(callable_):
    try:
        return ("ok", callable_())
    except Exception as exc:  # noqa: BLE001
        return ("err", type(exc).__name__, str(exc))


def compare(c_mod, r_mod):
    mismatches: list[tuple[str, tuple, tuple]] = []

    def check(name, c_fn, r_fn):
        c_res = capture(c_fn)
        r_res = capture(r_fn)
        if c_res != r_res:
            mismatches.append((name, c_res, r_res))

    check("unicode_version", lambda: c_mod.unicode_version, lambda: r_mod.unicode_version)
    check("hard_breaks", lambda: tuple(sorted(c_mod.hard_breaks)), lambda: tuple(sorted(r_mod.hard_breaks)))

    for codepoint in [0, 32, 65, 97, 0x0301, 0x1100, 0x1F1E6, 0x1F3FB, 0x1F525, 0x10FFFF]:
        check(
            f"category_category({codepoint})",
            lambda cp=codepoint: c_mod.category_category(cp),
            lambda cp=codepoint: r_mod.category_category(cp),
        )

    for text in ["", "abc", "Istanbul", "İstanbul", "straße", "áççéñțś", "🄷🄴🄻🄻🄾", "a🇬🇧bc", "a🤦🏼\u200d♂️bc"]:
        check(f"casefold({text!r})", lambda s=text: c_mod.casefold(s), lambda s=text: r_mod.casefold(s))
        check(f"strip({text!r})", lambda s=text: c_mod.strip(s), lambda s=text: r_mod.strip(s))

    for text in ["", "abc", "a\u0303b", "🐦\u200d🔥x", "\r\n", "a b", "Hello. world!", "a🇬🇧bc"]:
        text_len = len(text)
        for offset in range(text_len + 1):
            check(
                f"grapheme_next_break({text!r},{offset})",
                lambda s=text, o=offset: c_mod.grapheme_next_break(s, o),
                lambda s=text, o=offset: r_mod.grapheme_next_break(s, o),
            )
            check(
                f"word_next_break({text!r},{offset})",
                lambda s=text, o=offset: c_mod.word_next_break(s, o),
                lambda s=text, o=offset: r_mod.word_next_break(s, o),
            )
            check(
                f"sentence_next_break({text!r},{offset})",
                lambda s=text, o=offset: c_mod.sentence_next_break(s, o),
                lambda s=text, o=offset: r_mod.sentence_next_break(s, o),
            )
            check(
                f"line_next_break({text!r},{offset})",
                lambda s=text, o=offset: c_mod.line_next_break(s, o),
                lambda s=text, o=offset: r_mod.line_next_break(s, o),
            )
            check(
                f"line_next_hard_break({text!r},{offset})",
                lambda s=text, o=offset: c_mod.line_next_hard_break(s, o),
                lambda s=text, o=offset: r_mod.line_next_hard_break(s, o),
            )
            check(
                f"grapheme_length({text!r},{offset})",
                lambda s=text, o=offset: c_mod.grapheme_length(s, o),
                lambda s=text, o=offset: r_mod.grapheme_length(s, o),
            )
            check(
                f"text_width({text!r},{offset})",
                lambda s=text, o=offset: c_mod.text_width(s, o),
                lambda s=text, o=offset: r_mod.text_width(s, o),
            )

    for text, start, stop in [
        ("abc", 0, None),
        ("abc", -1, None),
        ("🐦\u200d🔥c\u0303", 1, 2),
        ("🐦\u200d🔥c\u0303", -10, 20),
    ]:
        check(
            f"grapheme_substr({text!r},{start},{stop})",
            lambda s=text, a=start, b=stop: c_mod.grapheme_substr(s, a, b),
            lambda s=text, a=start, b=stop: r_mod.grapheme_substr(s, a, b),
        )

    for text, needle, start, end in [
        ("abca", "a", 0, 4),
        ("aaaaa", "aaa", -10, 10),
        ("🐦\u200d🔥🔥", "🔥", 0, 10),
        ("a\u0303b", "a", 0, 3),
    ]:
        check(
            f"grapheme_find({text!r},{needle!r},{start},{end})",
            lambda s=text, n=needle, a=start, b=end: c_mod.grapheme_find(s, n, a, b),
            lambda s=text, n=needle, a=start, b=end: r_mod.grapheme_find(s, n, a, b),
        )

    check(
        "to_utf8_position_mapper type error",
        lambda: c_mod.to_utf8_position_mapper("abc"),
        lambda: r_mod.to_utf8_position_mapper("abc"),
    )
    check(
        "from_utf8_position_mapper type error",
        lambda: c_mod.from_utf8_position_mapper(b"abc"),
        lambda: r_mod.from_utf8_position_mapper(b"abc"),
    )

    check(
        "to_utf8_position_mapper value flow",
        lambda: (lambda m: (m.str, m(0), m(1), m(2), m(3), m(4)))(c_mod.to_utf8_position_mapper("aé🙂".encode("utf8"))),
        lambda: (lambda m: (m.str, m(0), m(1), m(2), m(3), m(4)))(r_mod.to_utf8_position_mapper("aé🙂".encode("utf8"))),
    )
    check(
        "to_utf8_position_mapper out-of-range",
        lambda: (lambda m: m(5))(c_mod.to_utf8_position_mapper("aé🙂".encode("utf8"))),
        lambda: (lambda m: m(5))(r_mod.to_utf8_position_mapper("aé🙂".encode("utf8"))),
    )

    check(
        "from_utf8_position_mapper value flow",
        lambda: (lambda m: (bytes(m.bytes), m(0), m(1), m(2), m(3), m(4), m(7)))(
            c_mod.from_utf8_position_mapper("aé🙂")
        ),
        lambda: (lambda m: (bytes(m.bytes), m(0), m(1), m(2), m(3), m(4), m(7)))(
            r_mod.from_utf8_position_mapper("aé🙂")
        ),
    )

    check(
        "offset_mapper flow",
        lambda: (lambda m: (m.add("ab", 10, 12), m.separate(), m.add("x", 20, 21), m.text, m(0), m(1), m(2), m(3)))(
            c_mod.OffsetMapper()
        ),
        lambda: (lambda m: (m.add("ab", 10, 12), m.separate(), m.add("x", 20, 21), m.text, m(0), m(1), m(2), m(3)))(
            r_mod.OffsetMapper()
        ),
    )
    check(
        "offset_mapper add-after-text",
        lambda: (lambda m: (m.add("ab", 0, 2), m.text, m.add("x", 3, 4)))(c_mod.OffsetMapper()),
        lambda: (lambda m: (m.add("ab", 0, 2), m.text, m.add("x", 3, 4)))(r_mod.OffsetMapper()),
    )

    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare C apsw._unicode and Rust _unicode behavior on a sampled corpus"
    )
    parser.add_argument(
        "--rust-module",
        default="/opt/.cargocache/target/release/lib_unicode.so",
        help="Path to Rust _unicode extension module",
    )
    args = parser.parse_args()

    import apsw._unicode as c_unicode

    rust_unicode = load_rust_unicode(args.rust_module)
    mismatches = compare(c_unicode, rust_unicode)

    if not mismatches:
        print("No sampled mismatches")
        return 0

    print(f"Sampled mismatches: {len(mismatches)}")
    for name, c_res, r_res in mismatches[:200]:
        print(name)
        print(f"  C: {c_res}")
        print(f"  R: {r_res}")

    if len(mismatches) > 200:
        print(f"... {len(mismatches) - 200} additional mismatches omitted")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
