"""How a scenario fails (spec/conformance.md, Failure reports)."""

from pathlib import Path

SPEC = Path(__file__).resolve().parents[2] / "spec" / "conformance.md"
"""The document the scenarios are read from."""


class StepFailed(Exception):
    """A step did not pass: what was expected and what happened instead."""

    def __init__(
        self,
        message: str,
        *,
        expected: str | None = None,
        actual: str | None = None,
        record: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.expected = expected
        self.actual = actual
        self.record = record


class ScenarioFailed(Exception):
    """A scenario's first failing step, as the failure report gives it: the
    scenario's name, the step's line in spec/conformance.md, the expected and
    actual values at the first difference, and the record read, if any."""

    def __init__(
        self, scenario: str, line: int, step: str, failure: StepFailed
    ) -> None:
        self.scenario = scenario
        self.line = line
        self.step = step
        self.failure = failure
        super().__init__(self.report())

    def report(self) -> str:
        lines = [
            f"scenario {self.scenario} failed at {SPEC.name}:{self.line}: {self.step}",
            f"  {self.failure.message}",
        ]
        if self.failure.expected is not None:
            lines.append(f"  expected: {self.failure.expected}")
        if self.failure.actual is not None:
            lines.append(f"  actual:   {self.failure.actual}")
        if self.failure.record is not None:
            lines.append(f"  record:   {self.failure.record}")
        return "\n".join(lines)
