"""Run the Python examples of the documentation, so they cannot drift from the public API.

A fenced `python` block that follows a `<!-- doctest -->` comment is executed. The blocks of one page run in order, in one namespace, in an empty
temporary directory, so a page reads as a script. The examples use `LOCAL` resources only: they need no credentials.
"""

import re
import sys
import types
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parent.parent / "docs"
BLOCK = re.compile(r"<!-- doctest -->\s*\n```python\n(.*?)\n```", re.DOTALL)
PAGES = sorted(path for path in DOCS.rglob("*.md") if BLOCK.search(path.read_text(encoding="utf-8")))


@pytest.mark.unit
def test_the_documentation_has_examples():
    """The pages with runnable examples are found, so the tests below cannot pass by collecting nothing."""
    assert {page.name for page in PAGES} >= {"quickstart.md", "basic-io.md"}


@pytest.mark.unit
@pytest.mark.parametrize("page", PAGES, ids=lambda page: str(page.relative_to(DOCS)))
def test_examples_of_a_page_run(page, tmp_path, monkeypatch):
    """Every example of the page runs, in order, in one namespace."""
    monkeypatch.chdir(tmp_path)
    # a real module in `sys.modules`: dynamicio inspects the module that defines an IO class
    name = "docs_example"
    module = types.ModuleType(name)
    monkeypatch.setitem(sys.modules, name, module)
    for number, source in enumerate(BLOCK.findall(page.read_text(encoding="utf-8")), start=1):
        exec(compile(source, f"{page.name}[block {number}]", "exec"), module.__dict__)  # pylint: disable=exec-used
