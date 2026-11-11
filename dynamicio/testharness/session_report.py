"""The account of what an :class:`ExpectedOutputs` session verified, for passing runs as well as failing ones."""

from typing import ClassVar, List, Mapping, Sequence, Tuple


class SessionReport:
    """Lists the outputs a session matched, failed, updated and left out, one line each.

    Which outputs a test verifies can be decided at runtime (``check_all`` looks them up in the resource definitions), so a green run says which ones
    they were. Sessions record their report in :attr:`recorded`, and the pytest plugin of this package prints them at the end of the run.

    Args:
        matched: Name of each output that matched, with a short description of it.
        failed: ``(name, message)`` of each output that did not match.
        updated: Names of the outputs whose expected file was rewritten.
        skipped: Names of the outputs ``check_all`` left out.
        test: The test that ran the session (``PYTEST_CURRENT_TEST``), when known.
    """

    recorded: ClassVar[List["SessionReport"]] = []

    def __init__(self, matched: Mapping[str, str], failed: Sequence[Tuple[str, str]], updated: Sequence[str], skipped: Sequence[str], test: str = ""):
        self.matched = dict(matched)
        self.failed = list(failed)
        self.updated = list(updated)
        self.skipped = list(skipped)
        self.test = test

    @property
    def is_empty(self) -> bool:
        """Whether the session checked nothing."""
        return not (self.matched or self.failed or self.updated or self.skipped)

    @property
    def title(self) -> str:
        """The headline: the counts, and the test."""
        counts = [f"{len(self.matched)} matched", f"{len(self.failed)} failed"]
        counts += [f"{len(self.updated)} updated"] if self.updated else []
        counts += [f"{len(self.skipped)} skipped"] if self.skipped else []
        return f"{', '.join(counts)}" + (f"  [{self.test}]" if self.test else "")

    def lines(self) -> List[Tuple[str, str]]:
        """``(status, text)`` per output, where status is ``matched``, ``failed``, ``updated`` or ``skipped``, in the order of the checks."""
        lines = [("matched", f"✓ {name}  {description}") for name, description in self.matched.items()]
        lines += [("failed", f"✗ {name}  {message.splitlines()[0] if message else ''}") for name, message in self.failed]
        lines += [("updated", f"↻ {name}  expected file rewritten") for name in self.updated]
        lines += [("skipped", f"- {name}  has {{placeholders}} no params fill; use check_each") for name in self.skipped]
        return lines

    def text(self) -> str:
        """The plain-text report."""
        return "\n".join([f"dynamicio expected outputs: {self.title}"] + [f"  {line}" for _, line in self.lines()])
