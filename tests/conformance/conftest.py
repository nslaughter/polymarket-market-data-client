"""The pytest entry point: each scenario listed in ``enabled.txt`` is a test,
named after the scenario, so ``pytest tests/conformance -k <name>`` runs
one."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from . import notation, runner
from .failures import SPEC, ScenarioFailed

ENABLED = Path(__file__).with_name("enabled.txt")


def enabled_names(text: str, known: set[str]) -> list[str]:
    """The scenario names ``enabled.txt`` lists, one per line."""
    names = [line.strip() for line in text.splitlines() if line.strip()]
    for name in names:
        if name not in known:
            raise ValueError(f"enabled.txt lists {name}, which is not a scenario")
        if names.count(name) > 1:
            raise ValueError(f"enabled.txt lists {name} twice")
    return names


def pytest_collect_file(
    file_path: Path, parent: pytest.Collector
) -> pytest.File | None:
    if file_path == ENABLED:
        return EnabledScenarios.from_parent(parent, path=file_path)
    return None


class EnabledScenarios(pytest.File):
    def collect(self) -> Iterator[pytest.Item]:
        scenarios = {scenario.name: scenario for scenario in notation.load()}
        for name in enabled_names(self.path.read_text(), set(scenarios)):
            yield ScenarioItem.from_parent(self, name=name, scenario=scenarios[name])


class ScenarioItem(pytest.Item):
    def __init__(self, *, scenario: notation.Scenario, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.scenario = scenario

    def runtest(self) -> None:
        runner.run(self.scenario)

    def repr_failure(
        self,
        excinfo: pytest.ExceptionInfo[BaseException],
        style: Any = None,
    ) -> Any:
        if isinstance(excinfo.value, ScenarioFailed):
            return excinfo.value.report()
        return super().repr_failure(excinfo, style)

    def reportinfo(self) -> tuple[Path, int, str]:
        return SPEC, self.scenario.line - 1, f"scenario {self.scenario.name}"
