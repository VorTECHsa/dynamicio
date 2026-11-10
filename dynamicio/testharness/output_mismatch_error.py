"""The error raised when generated outputs differ from their expected counterparts."""

from typing import List, Sequence, Tuple


class OutputMismatchError(AssertionError):
    """Raised once, after all checks ran, listing every output that did not match its expected counterpart.

    Attributes:
        errors: ``(name, message)`` for each failed check, in the order the checks ran.
    """

    MAX_MESSAGE_CHARS = 1000

    def __init__(self, errors: Sequence[Tuple[str, str]]):
        self.errors: List[Tuple[str, str]] = list(errors)
        super().__init__(self._format(self.errors))

    @classmethod
    def _format(cls, errors: Sequence[Tuple[str, str]]) -> str:
        lines = [f"The following AssertionErrors occurred ({len(errors)}):"]
        for name, message in errors:
            shortened = message if len(message) <= cls.MAX_MESSAGE_CHARS else f"{message[:cls.MAX_MESSAGE_CHARS]}... (truncated)"
            lines.append(f"\n[{name}]\n{shortened}")
        return "\n".join(lines)
