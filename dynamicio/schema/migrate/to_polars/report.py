"""The Markdown report of a polars migration."""

from typing import List, Mapping, Sequence


class PolarsMigrationReport:
    """The Markdown report of a polars migration, for the user or for an LLM."""

    @staticmethod
    def section(title: str, intro: str, lines: Sequence[str]) -> List[str]:
        """A report section: a heading, a sentence, and a list. A section without lines says so."""
        return [f"## {title}", "", intro, ""] + ([f"- {line}" for line in lines] if lines else ["None."]) + [""]

    @classmethod
    def render(cls, output: str, models: Sequence[str], todos: Sequence[str], resources: Mapping[str, Sequence[str]], objects: Sequence[str] = ()) -> str:
        """The report of the models at ``output``, the checks to port, the ``object`` columns, and the findings of the resource definitions."""
        lines = [
            "# Polars migration",
            "",
            f"`dynamicio migrate --engine polars` wrote {len(models)} polars models to `{output}`. Each model has the dtypes, validations, metrics and names of its pandas model.",
            "",
            "1. Replace the imports of the pandas models with the imports of the polars models.",
            '2. Call `dynamicio.configure(engine="polars")` at the start of each process, before the first read or write.',
            "3. Change the transformation code from the pandas API to the polars API.",
            "4. Run the tests with `pytest --dynamicio-engine=polars`.",
            "",
        ]
        lines += cls.section("Checks to port", "These Pandera checks are pandas code. Write them again for polars.", todos)
        lines += cls.section(
            "Columns of the object dtype",
            "The polars models give these columns the `String` dtype. If a column holds lists, declare `pl.List` of the item dtype, for example `pl.List(pl.String)`.",
            objects,
        )
        lines += cls.section(
            "Resource definitions that the polars engine refuses",
            "Change these definitions, or keep these resources on the pandas engine. The options that the code gives to an IO object are not in this list.",
            resources["refused"],
        )
        lines += cls.section(
            "CSV definitions without an index option",
            "The polars engine writes the leading index column with the row numbers 0 to n-1. Set `index: false` if the file has no index column.",
            resources["csv_index"],
        )
        return "\n".join(lines)
