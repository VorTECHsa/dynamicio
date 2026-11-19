"""The entry point of the Airflow tasks of the polars demo pipeline: `python -m demo.polars_pipeline.src --with_module staging`."""
import dynamicio
from demo.polars_pipeline.src.runners import staging, transform
from demo.src.runners.utils.runner_selection import choose_module, create_parser

AIRFLOW_TASK_MODULES = {"staging": staging, "transform": transform}

# The engine is a setting of the process: set it before the first read or write
dynamicio.configure(engine="polars")

args = create_parser(AIRFLOW_TASK_MODULES).parse_args()
choose_module(args.with_module, AIRFLOW_TASK_MODULES).main()
