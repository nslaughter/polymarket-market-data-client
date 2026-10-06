"""Citations of the documents that govern the code, as AGENTS.md requires them.

A citation names a document and what in it applies, such as
``(spec/client.md, Nesting depth)``, or a transition or owner specification of
the contract, such as ``(T14)`` or ``(D8)``. Every citation in a comment or
docstring must name something its document holds. Each module's docstring
cites what the module implements or tests; each transition is cited in the
package and in the tests; and each owner specification of a step marked Done
is cited in the package or its build (spec/client.md, Per-token state machine
and Owner specifications; docs/implementation-plan.md, Progress).
"""

import ast
import io
import re
import tokenize
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# The documents a citation may name: those AGENTS.md says to read first, apart
# from AGENTS.md itself.
DOCUMENTS = (
    "spec/client.md",
    "spec/conformance.md",
    "docs/implementation-plan.md",
    "docs/source-behavior.md",
)


def modules(root: Path) -> list[Path]:
    """The Python modules of the package, its tests, and its examples."""
    directories = ("src", "tests", "examples")
    return sorted(path for name in directories for path in (root / name).rglob("*.py"))


def build_files(root: Path) -> list[Path]:
    """pyproject.toml, and the YAML files of CI."""
    ci = (root / ".github").rglob("*")
    yaml = (path for path in ci if path.suffix in {".yml", ".yaml"})
    return [root / "pyproject.toml", *sorted(yaml)]


MODULES = modules(ROOT)
PACKAGE = sorted((ROOT / "src").rglob("*.py"))
TESTS = sorted((ROOT / "tests").rglob("*.py"))
BUILD = build_files(ROOT)

CITATION = re.compile(r"\(((?:[\w.-]+/)*[\w.-]+\.md, [^()]*)\)")
PART = re.compile(r"((?:[\w.-]+/)*[\w.-]+\.md), (.+)")
ID = re.compile(r"\b[TD][1-9]\d*\b")
SEPARATORS = (", and ", ", ", " and ")

# In TOML or YAML, a quoted string, or a comment: a ``#`` that begins the line
# or follows whitespace, and the rest of the line. A quote after a letter or
# digit, as in ``client's``, is an apostrophe.
BUILD_COMMENT = re.compile(r"""(?<!\w)"(?:[^"\\]|\\.)*"|(?<!\w)'[^']*'|(?<!\S)#.*""")

HEADING = re.compile(r"#{1,6} (.+)")
RULE = re.compile(r"(\d+)\. ")
TRANSITION = re.compile(r"\| (T\d+) \|")
OWNER = re.compile(r"(D\d+)\. ")
SECTION = re.compile(r"(\d+)\. ")
STEP = re.compile(r"\| (\d+)\. [^|]*\|([^|]*)\|([^|]*)\|")


@dataclass(frozen=True, slots=True)
class Text:
    """A docstring, or a comment with those on the lines that follow it."""

    path: Path
    line: int
    text: str

    def where(self) -> str:
        return f"{self.path.relative_to(ROOT)}:{self.line}"


def python_texts(path: Path) -> Iterator[Text]:
    source = path.read_text()
    run: list[tokenize.TokenInfo] = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type != tokenize.COMMENT:
            continue
        if run and token.start[0] != run[-1].start[0] + 1:
            yield comment(path, run[0].start[0], (t.string for t in run))
            run = []
        run.append(token)
    if run:
        yield comment(path, run[0].start[0], (t.string for t in run))
    for node in ast.walk(ast.parse(source)):
        # A docstring is a string that stands as a statement: one that opens a
        # module, class, or function, or one that follows an attribute.
        match node:
            case ast.Expr(ast.Constant(str() as docstring)):
                yield Text(path, node.lineno, " ".join(docstring.split()))


def build_texts(path: Path) -> Iterator[Text]:
    """The comments of a TOML or YAML file, whole lines or after a value."""
    run: list[tuple[int, str]] = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        found = [m.group() for m in BUILD_COMMENT.finditer(line)]
        if not (found and found[-1].startswith("#")):
            continue
        if run and number != run[-1][0] + 1:
            yield comment(path, run[0][0], (text for _, text in run))
            run = []
        run.append((number, found[-1]))
    if run:
        yield comment(path, run[0][0], (text for _, text in run))


def comment(path: Path, line: int, lines: Iterable[str]) -> Text:
    return Text(path, line, " ".join(text.lstrip("#").strip() for text in lines))


def texts(paths: Iterable[Path]) -> Iterator[Text]:
    for path in paths:
        yield from python_texts(path) if path.suffix == ".py" else build_texts(path)


def module_docstring(path: Path) -> str:
    return " ".join((ast.get_docstring(ast.parse(path.read_text())) or "").split())


@cache
def citable(document: str) -> frozenset[str]:
    """What a citation of ``document`` may name: each heading, without its
    backticks; each numbered rule of a section, as ``<heading>, rule <n>``
    or ``<heading>, rules <n> and <m>``; and, for spec/client.md, each
    transition and owner specification, and for docs/source-behavior.md,
    each numbered section, as ``§<n>``."""
    names: set[str] = set()
    rules: dict[str, list[str]] = {}
    heading = ""
    fenced = False
    for line in (ROOT / document).read_text().splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
        elif fenced:
            continue
        elif match := HEADING.fullmatch(line):
            heading = match.group(1).replace("`", "")
            names.add(heading)
            if document == "spec/client.md" and (owner := OWNER.match(heading)):
                names.add(owner.group(1))
            if document == "docs/source-behavior.md" and (
                section := SECTION.match(heading)
            ):
                names.add(f"§{section.group(1)}")
        elif match := RULE.match(line):
            rules.setdefault(heading, []).append(match.group(1))
        elif document == "spec/client.md" and (match := TRANSITION.match(line)):
            names.add(match.group(1))
    for title, numbers in rules.items():
        names.update(f"{title}, rule {n}" for n in numbers)
        names.update(
            f"{title}, rules {n} and {m}"
            for i, n in enumerate(numbers)
            for m in numbers[i + 1 :]
        )
    return frozenset(names)


@cache
def ids() -> frozenset[str]:
    return frozenset(name for name in citable("spec/client.md") if ID.fullmatch(name))


def is_list_of(text: str, names: frozenset[str]) -> bool:
    """Whether ``text`` is one of ``names``, or a list of them written as
    ``A and B`` or ``A, B, and C``."""

    @cache
    def from_(start: int) -> bool:
        for name in names:
            if not text.startswith(name, start):
                continue
            end = start + len(name)
            if end == len(text) or any(
                text.startswith(separator, end) and from_(end + len(separator))
                for separator in SEPARATORS
            ):
                return True
        return False

    return from_(0)


def problems(text: str) -> Iterator[str]:
    """Each citation in ``text`` that names a document it may not, or
    something its document does not hold."""
    for citation in CITATION.finditer(text):
        for part in citation.group(1).split("; "):
            match = PART.fullmatch(part)
            if match is None:
                yield f"({part}) is not one document and what it names"
                continue
            document, names = match.groups()
            if document not in DOCUMENTS:
                full = [d for d in DOCUMENTS if d.endswith(f"/{document}")]
                yield (
                    f"cite {document} as {full[0]}"
                    if full
                    else f"{document} is not one of {', '.join(DOCUMENTS)}"
                )
            elif not is_list_of(names.replace("`", ""), citable(document)):
                yield f"({part}) names something {document} does not hold"
    for name in ID.findall(text):
        if name not in ids():
            yield f"{name} is not a transition or owner specification"


def cited(paths: Iterable[Path]) -> set[str]:
    return {name for text in texts(paths) for name in ID.findall(text.text)}


def owner_specifications_done() -> dict[str, list[int]]:
    """The owner specifications each step marked Done in the plan's Progress
    table follows, with the steps that follow each."""
    plan = (ROOT / "docs/implementation-plan.md").read_text()
    progress = plan.split("\n## Progress\n", 1)[1].split("\n## ", 1)[0]
    steps = [
        match.groups() for match in map(STEP.match, progress.splitlines()) if match
    ]
    assert steps, "no step rows in the Progress table"
    done: dict[str, list[int]] = {}
    for step, specifications, status in steps:
        if status.strip() == "Done":
            for name in ID.findall(specifications):
                done.setdefault(name, []).append(int(step))
    return done


# The checks.


def test_citations_name_what_their_documents_hold() -> None:
    found = [
        f"{text.where()}: {problem}"
        for text in texts([*MODULES, *BUILD])
        for problem in problems(text.text)
    ]
    assert not found, "\n".join(found)


def test_each_module_cites_what_it_implements_or_tests() -> None:
    uncited = [
        str(path.relative_to(ROOT))
        for path in MODULES
        if not (
            CITATION.search(module_docstring(path)) or ID.search(module_docstring(path))
        )
    ]
    assert not uncited, "no citation in the module docstring:\n" + "\n".join(uncited)


@pytest.mark.parametrize(("where", "paths"), [("src", PACKAGE), ("tests", TESTS)])
def test_each_transition_is_cited(where: str, paths: list[Path]) -> None:
    transitions = sorted(
        (name for name in ids() if name.startswith("T")), key=lambda t: int(t[1:])
    )
    assert transitions, "no transitions found in spec/client.md"
    missing = [name for name in transitions if name not in cited(paths)]
    assert not missing, f"not cited in {where}: {', '.join(missing)}"


def test_each_owner_specification_of_a_done_step_is_cited() -> None:
    implemented = cited([*PACKAGE, *BUILD])
    missing = [
        f"{name} (step {', '.join(map(str, steps))})"
        for name, steps in sorted(owner_specifications_done().items())
        if name not in implemented
    ]
    assert not missing, "not cited in src, pyproject.toml, or CI: " + ", ".join(missing)


# The checks' own rules.


def test_the_check_reads_every_module_and_build_file(tmp_path: Path) -> None:
    names = [
        ".github/workflows/ci.yml",
        ".github/workflows/release.yaml",
        "examples/research.py",
        "pyproject.toml",
        "src/package/module.py",
        "tests/test_module.py",
    ]
    for name in names:
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).touch()
    read = [*modules(tmp_path), *build_files(tmp_path)]
    assert sorted(path.relative_to(tmp_path).as_posix() for path in read) == names


@pytest.mark.parametrize(
    ("name", "source", "found"),
    [
        ("ci.yml", "# Each release\n# (D5).\npython: [3.12]\n", ["Each release (D5)."]),
        (
            "ci.yml",
            'python: ["3.12"]  # Each release\n# (D5).\n',
            ["Each release (D5)."],
        ),
        ("ci.yml", "- name: Check the client's types  # (D5)\n", ["(D5)"]),
        ("ci.yml", "run: echo \"a # (D5)\" 'b # (D5)' c#(D5)\n", []),
        ("pyproject.toml", 'sdk = ["polymarket-client==0.12.0"]  # (D6)\n', ["(D6)"]),
        ("module.py", "x = 1  # (D8)\n", ["(D8)"]),
        ("module.py", '"""A module (D8)."""\n', ["A module (D8)."]),
        ("module.py", 'class A:\n    x: int\n    """Its x (D8)."""\n', ["Its x (D8)."]),
        ("module.py", 'X = 1\n"""Its value (D8)."""\n', ["Its value (D8)."]),
        ("module.py", 'X = "(D8)"\nprint("(D8)", f"{X} (D8)")\n', []),
    ],
)
def test_the_check_reads_every_comment_and_docstring(
    tmp_path: Path, name: str, source: str, found: list[str]
) -> None:
    path = tmp_path / name
    path.write_text(source)
    assert [text.text for text in texts([path])] == found


@pytest.mark.parametrize(
    "text",
    [
        "(spec/client.md, Nesting depth)",
        "(spec/client.md, new_market)",
        "(spec/client.md, `new_market`)",
        "(spec/client.md, Cancellation and shutdown)",
        "(spec/client.md, Decoding and Repeated messages)",
        "(spec/client.md, Per-token state machine, Recovery contract, Settlement,"
        " and Record order)",
        "(spec/client.md, Record order, rule 3)",
        "(spec/client.md, Record order, rules 3 and 4)",
        "(spec/client.md, D4)",
        "(spec/conformance.md, Frame notation; spec/client.md, Order-book hash)",
        "(spec/conformance.md, initial-books)",
        "(docs/source-behavior.md, §4)",
        "(docs/implementation-plan.md, Progress)",
        "the transitions T1 to T18 (D8)",
        "T0 is not an ID, nor is A1",
        "a parenthesis (with no document in it)",
    ],
)
def test_a_citation_of_what_a_document_holds_passes(text: str) -> None:
    assert list(problems(text)) == []


@pytest.mark.parametrize(
    ("text", "problem"),
    [
        ("(spec/client.md, Nesting dept)", "does not hold"),
        ("(spec/client.md, Decoding, its third row)", "does not hold"),
        ("(spec/client.md, Record order, rule 9)", "does not hold"),
        ("(spec/client.md, Record order rule 3)", "does not hold"),
        ("(docs/source-behavior.md, §7)", "does not hold"),
        ("(spec/conformance.md, Nesting depth)", "does not hold"),
        ("(client.md, Nesting depth)", "cite client.md as spec/client.md"),
        ("(README.md, Scope)", "README.md is not one of"),
        ("(T19)", "T19 is not"),
        ("(D9)", "D9 is not"),
    ],
)
def test_a_citation_of_something_a_document_lacks_fails(
    text: str, problem: str
) -> None:
    assert any(problem in found for found in problems(text)), list(problems(text))
