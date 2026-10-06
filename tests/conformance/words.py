"""Words of the scenario notation (spec/conformance.md, Lines)."""

import json
import re
from dataclasses import dataclass


class NotationError(Exception):
    """A scenario block the notation does not allow."""


@dataclass(frozen=True, slots=True)
class Word:
    """One word: its text as written, and, for a ``<field>=<value>`` word,
    the field and the value. A JSON string literal, a whole word or a
    field's value, is decoded."""

    text: str
    key: str | None
    value: str
    quoted: bool


_KEY = re.compile(r"[a-z_][a-z0-9_.]*")


def split_words(line: str) -> list[Word]:
    """Words are separated by spaces. A JSON string literal stays within one
    word, from its opening quote to its closing one, and may hold spaces."""
    words = []
    i = 0
    while i < len(line):
        if line[i] == " ":
            i += 1
            continue
        start = i
        if line[i] == '"':
            i = _string_end(line, i)
            literal = line[start:i]
            words.append(Word(literal, None, _decode(literal), True))
        else:
            while i < len(line) and line[i] != " ":
                if (
                    line[i] == "="
                    and line[i + 1 : i + 2] == '"'
                    and _KEY.fullmatch(line[start:i])
                ):
                    end = _string_end(line, i + 1)
                    literal = line[i + 1 : end]
                    words.append(
                        Word(line[start:end], line[start:i], _decode(literal), True)
                    )
                    i = end
                    break
                i += 1
            else:
                text = line[start:i]
                key, equals, value = text.partition("=")
                if equals and _KEY.fullmatch(key):
                    words.append(Word(text, key, value, False))
                else:
                    words.append(Word(text, None, text, False))
                continue
        if i < len(line) and line[i] != " ":
            raise NotationError(f"text follows a JSON string: {line[start:]}")
    return words


def _decode(literal: str) -> str:
    try:
        value = json.loads(literal)
    except ValueError as error:
        raise NotationError(f"an invalid JSON string {literal}: {error}") from None
    assert isinstance(value, str)
    return value


def _string_end(line: str, start: int) -> int:
    """The index just past the JSON string literal that opens at ``start``."""
    i = start + 1
    while i < len(line):
        if line[i] == "\\":
            i += 2
        elif line[i] == '"':
            return i + 1
        else:
            i += 1
    raise NotationError(f"an unterminated JSON string: {line[start:]}")
