"""The polars migration of the pandas models under a path: the models, the parity check and the report."""

import os
from typing import Any, Dict, List, Sequence


from dynamicio.schema.migrate import Migration, ModelRenderer
from dynamicio.schema.migrate.to_polars.pandas_models import PandasModels
from dynamicio.schema.migrate.to_polars.ported_model import PortedModel
from dynamicio.schema.migrate.to_polars.renderer import PolarsModelRenderer
from dynamicio.schema.migrate.to_polars.report import PolarsMigrationReport
from dynamicio.schema.migrate.to_polars.resource_review import ResourceReview


class PolarsMigration:
    """Writes a polars model for each pandas model under a path, checks parity, and writes the report next to the models."""

    REPORT = "POLARS_MIGRATION.md"

    @classmethod
    def migrate(cls, path: str, output: str, scan: str = "", root: str = "") -> Dict[str, Any]:
        """Write a polars model for each pandas model at ``path`` (a ``.py`` module or a package directory), check parity, and write a report.

        If ``output`` ends with ``.py``, all models go into that one module. Else ``output`` is a package directory with one module for each module of
        the pandas models. The modules at ``path`` are imported by their dotted name relative to ``root`` (default: the current directory).

        Returns:
            ``models`` (the dotted names of the pandas models), ``module``, ``report`` (the path of the report), ``todos`` (the checks to port),
            ``objects`` (the ``object`` columns, which can hold lists), ``refused`` and ``csv_index`` (the findings of the resource definitions under ``scan``).

        Raises:
            ValueError: No pandas model is found, two models map to one polars model, a dtype or a validation has no polars equivalent, or a generated
                model does not compile to its pandas model.
        """
        return cls(path, output, scan, root).run()

    def __init__(self, path: str, output: str, scan: str = "", root: str = ""):
        self.path = path
        self.output = output
        self.scan = scan or os.getcwd()
        self.root = os.path.abspath(root or os.getcwd())
        self.package = not output.endswith(".py")

    def _ported(self) -> Dict[str, List[PortedModel]]:
        """The ported models of each output module: the source module name in package mode, else one module."""
        found = PandasModels.find(self.path, self.root)
        if not found:
            raise ValueError(f"No pandas models found under {self.path}")
        modules: Dict[str, List[PortedModel]] = {}
        seen: Dict[str, str] = {}
        for module, models in found.items():
            target = module.rsplit(".", 1)[-1] if self.package else ""
            for model in models:
                key = f"{target}.{model.__name__}"
                if key in seen:
                    raise ValueError(f"The pandas models {seen[key]} and {module}.{model.__name__} both map to the polars model {model.__name__} in one module")
                seen[key] = f"{module}.{model.__name__}"
                modules.setdefault(target, []).append(PortedModel(model))
        return modules

    @staticmethod
    def _verify_parity(source: str, ported: Sequence[PortedModel]) -> None:
        """Compile a generated module and compare each polars model with its pandas model."""
        namespace = ModelRenderer.compile_module(source)
        for model in ported:
            problems = model.differences(namespace[model.model.__name__])
            if problems:
                raise ValueError(f"The polars model {model.model.__name__} is not equivalent to {model.label}:\n" + "\n".join(problems))

    INIT = '"""The polars models of the datasets, one module for each module of the pandas models."""\n'

    def run(self) -> Dict[str, Any]:
        """Run the migration; see ``migrate``."""
        modules = self._ported()
        sources = {name: PolarsModelRenderer.polars_module([model.class_source() for model in ported]) for name, ported in modules.items()}
        for name, ported in modules.items():
            self._verify_parity(sources[name], ported)
        Migration.write_modules(self.output, sources, self.INIT)

        models = [model.label for ported in modules.values() for model in ported]
        todos = [todo for ported in modules.values() for model in ported for todo in model.todos]
        objects = [column for ported in modules.values() for model in ported for column in model.objects]
        resources = ResourceReview.review(self.scan)
        directory = self.output if self.package else os.path.dirname(os.path.abspath(self.output))
        report_path = os.path.join(directory, self.REPORT)
        with open(report_path, "w", encoding="utf-8") as handle:
            handle.write(PolarsMigrationReport.render(self.output, models, todos, resources, objects))
        return {"models": models, "module": self.output, "report": report_path, "todos": todos, "objects": objects, **resources}
