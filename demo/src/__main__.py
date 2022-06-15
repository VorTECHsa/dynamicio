"""The main module that serves as the entry point for all the other sub-modules under the: loader, pre_process and the publish packages."""
# pylint: disable=unused-argument

from functools import partial
from signal import SIGINT, SIGTERM, getsignal, signal

from demo.src.runner_selection import choose_module, create_parser, custom_signal_handler
from demo.src.runners import staging, transform

AIRFLOW_TASK_MODULES = {"staging": staging, "transform": transform}

# Run the respective handler() functions when SIGINT or SIGTERM is received
signal(SIGINT, partial(custom_signal_handler, default_func=(getsignal(SIGINT))))
signal(SIGTERM, partial(custom_signal_handler, default_func=(getsignal(SIGTERM))))

# Load input args
parser = create_parser(AIRFLOW_TASK_MODULES)
args = parser.parse_args()

func = choose_module(args.with_module, AIRFLOW_TASK_MODULES)
func.main()
