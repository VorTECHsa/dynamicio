"""A harness for testing a pipeline task end to end against the resource definitions it already uses.

A task writes its outputs through dynamicio. In a ``LOCAL`` environment those land on disk, so a test can run the task and then compare every output
with the reference file declared next to it, under the optional ``expected`` key of the resource definition::

    FINAL_BAR:
      LOCAL:
        type: "local"
        local:
          file_path: "[[ TEST_RESOURCES ]]/data/processed/final_bar.parquet"
          file_type: "parquet"
        expected:
          path: "[[ TEST_RESOURCES ]]/data/processed/expected/final_bar.parquet"

The pieces, one class per module:

- :class:`EnvironmentSwitch`: runs a test in an explicitly chosen environment of the resource definitions, instead of patching ``env_identifier``.
- :class:`ExpectedOutputs`: the session. Collects every mismatch, raises one error, cleans up, and can regenerate the expected files.
- :class:`Expectations`: the curated rules of a comparison (order, dtypes, tolerances, ignored columns, custom assertions), strict or relaxed.
- :class:`FrameComparison`: applies the rules to two frames.
- :class:`OutputFiles`: finds, reads, removes and promotes the generated and expected files of a resource.
- :class:`OutputMismatchError`: the error that reports all failed checks at once.
- :class:`OutputSandbox`: redirects the local outputs of a task into a scratch directory, so tests that write the same files can run in parallel.
- :class:`ParquetSchemaComparison`: compares the arrow schema and pandas index of two parquet files (``Expectations(check_parquet_schema=True)``).

Example::

    with ExpectedOutputs(Expectations(ignore_columns=["run_id"])) as outputs:
        staging.main()
        outputs.check(raw_config, "STAGED_FOO")
        outputs.check(processed_config, "FINAL_BAR", list_columns_to_sort=["tags"], rtol=1e-3)
"""

__all__ = ["EnvironmentSwitch", "ExpectedOutputs", "Expectations", "FrameAssertion", "FrameComparison", "OutputFiles", "OutputMismatchError", "OutputSandbox", "ParquetSchemaComparison"]

from dynamicio.testharness.environment_switch import EnvironmentSwitch
from dynamicio.testharness.expectations import Expectations, FrameAssertion
from dynamicio.testharness.expected_outputs import ExpectedOutputs
from dynamicio.testharness.frame_comparison import FrameComparison
from dynamicio.testharness.output_files import OutputFiles
from dynamicio.testharness.output_mismatch_error import OutputMismatchError
from dynamicio.testharness.output_sandbox import OutputSandbox
from dynamicio.testharness.parquet_schema_comparison import ParquetSchemaComparison
