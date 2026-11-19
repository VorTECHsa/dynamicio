"""Run the Python examples of the documentation, so they cannot drift from the public API.

A fenced `python` block that follows a `<!-- doctest -->` comment is executed. The blocks of one page run in order, in one namespace, in an empty
temporary directory, so a page reads as a script. The examples use `LOCAL` resources only: they need no credentials.

The `Examples` of the docstrings in `DOCSTRINGS` run after the page that they name, in a copy of its namespace: they use the objects of the page,
for example `orders`. A page that imports polars is skipped without polars. A page can set the engine of the process, so each page starts and ends on the pandas engine, not
frozen. The polars validators and metrics that a page registers, and the engines that it creates, are removed after the page.
"""

import doctest
import importlib.util
import pydoc
import re
import sys
import types
from pathlib import Path

import pytest

from dynamicio.engines import EngineSetting

DOCS = Path(__file__).resolve().parent.parent / "docs"
BLOCK = re.compile(r"<!-- doctest -->\s*\n```python\n(.*?)\n```", re.DOTALL)
PAGES = sorted(path for path in DOCS.rglob("*.md") if BLOCK.search(path.read_text(encoding="utf-8")))
DOCSTRINGS = {
    "polars-engine.md": [
        "dynamicio.engines.engine_setting.EngineSetting.configure",
        "dynamicio.schema.field.Field",
        "dynamicio.schema.polars.DataModel",
        "dynamicio.core.DynamicDataIO.scan",
        "dynamicio.core.DynamicDataIO.sink",
        "dynamicio.core.DynamicDataIO.footer",
    ],
}


class TestDocsExamples:
    """The `<!-- doctest -->` examples of each page run."""

    @staticmethod
    def blocks(page: Path) -> list:
        """The sources of the `<!-- doctest -->` blocks of a page, in order."""
        return BLOCK.findall(page.read_text(encoding="utf-8"))

    @staticmethod
    def keep_the_polars_registries(monkeypatch) -> None:
        """Give each registry a copy for the page, so a validator or a metric of the page does not stay registered.

        An engine keeps the registries that it was created with, so the page also gets its own engine cache. An engine of the page thus does not stay
        in the cache with the copies of the page.
        """
        from dynamicio.schema.polars_metrics import PolarsMetrics  # pylint: disable=import-outside-toplevel
        from dynamicio.schema.polars_validations import PolarsValidations  # pylint: disable=import-outside-toplevel

        for registry in (PolarsValidations, PolarsMetrics):
            monkeypatch.setattr(registry, "ALL", dict(registry.ALL))
        monkeypatch.setattr(EngineSetting, "_instances", {})

    @staticmethod
    def run_docstrings(names: list, namespace: dict) -> None:
        """Run the `>>>` examples of the docstrings of `names`, in order. A failed example raises `AssertionError` with the doctest report."""
        runner = doctest.DocTestRunner(optionflags=doctest.ELLIPSIS)
        for name in names:
            tests = doctest.DocTestFinder(recurse=False).find(pydoc.locate(name), name, globs=namespace)
            assert tests and tests[0].examples, f"{name} has no examples"
            for test in tests:
                output: list = []
                if runner.run(test, out=output.append).failed:
                    raise AssertionError("".join(output))

    @pytest.mark.unit
    def test_the_documentation_has_examples(self):
        """The pages with runnable examples are found, so the tests below cannot pass by collecting nothing."""
        assert {page.name for page in PAGES} >= {"quickstart.md", "basic-io.md", "polars-engine.md"}

    @pytest.mark.unit
    @pytest.mark.parametrize("page", PAGES, ids=lambda page: str(page.relative_to(DOCS)))
    def test_examples_of_a_page_run(self, page, tmp_path, monkeypatch):
        """Every example of the page runs, in order, in one namespace."""
        # Given
        blocks = self.blocks(page)
        if any(re.search(r"^\s*import polars", block, re.MULTILINE) for block in blocks):
            if importlib.util.find_spec("polars") is None:
                pytest.skip("the page needs polars")
            self.keep_the_polars_registries(monkeypatch)
        monkeypatch.chdir(tmp_path)
        # a real module in `sys.modules`: dynamicio inspects the module that defines an IO class
        name = "docs_example"
        module = types.ModuleType(name)
        monkeypatch.setitem(sys.modules, name, module)
        EngineSetting.reset()

        # When / Then
        try:
            for number, source in enumerate(blocks, start=1):
                exec(compile(source, f"{page.name}[block {number}]", "exec"), module.__dict__)  # pylint: disable=exec-used
            self.run_docstrings(DOCSTRINGS.get(page.name, []), module.__dict__)
        finally:
            EngineSetting.reset()
