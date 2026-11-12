# Testing pipelines locally

## Testing Locally

After following the above documentation, at this point it should be clear that `dynamic(i/o)` is optimised for enabling
seamless local testing for your pipelines.

Simply by selecting the `LOCAL` environment (`ENVIRONMENT = "LOCAL"`) and provided that you have the required test data in the
necessary directories, it becomes very simple to test your pipelines end-to-end in seconds, eliminating the need to deploy your dags and
wait for their tasks to be provided access to processing resources.

This is the fast feedback loop that the seam between logic and I/O gives you (see [Why dynamicio](../getting-started/why-dynamicio.md)).
It is as useful to a coding agent as to a person: the agent runs the real tasks against sample files, with no credentials and no deploy,
and the harness below returns a verdict it can read, with every mismatch listed.

All you need to do is mimic the order of execution of your tasks, running them in procedural order, then compare what they wrote with what you expect.

### Declaring the expected output

An output that a test should verify declares, in its `LOCAL` environment, an optional `expected` key next to the data it writes:

```yaml
FINAL_BAR:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/processed/final_bar.parquet"
      file_type: "parquet"
    expected:
      path: "[[ TEST_RESOURCES ]]/data/processed/expected/final_bar.parquet"   # file_type defaults to the one above
  DEV:
    type: "kafka"
    kafka:
      kafka_server: "[[ DEV_KAFKA_BROKER ]]"
      kafka_topic: "[[ KAFKA_TOPIC ]]"
  PROD:
    type: "kafka"
    kafka:
      kafka_server: "[[ PROD_KAFKA_BROKER ]]"
      kafka_topic: "[[ KAFKA_TOPIC ]]"
```

`expected` is optional and per environment: reading and writing ignore it, so DEV and PROD stay as they are, and the test harness reads it from
whichever environment the test runs in (`expected.file_type` is optional and defaults to the `local` file type).
JSON files are read with the `orient` and `lines` the resource's `options` declare (so JSON Lines outputs work); `expected.options` (e.g. `options: {orient: index}`) sets reader options for both files when they differ. Update mode refuses to copy an output over a reference declared in another file type.

### Asserting lazily with `dynamicio.testharness`

The `dynamicio.testharness` package replaces the hand-written read-compare-delete code around a pipeline test. Its `ExpectedOutputs` session collects *every*
mismatch instead of stopping at the first, raises a single `OutputMismatchError` (an `AssertionError`) listing them all, removes the generated file of every
output that matched, and keeps the ones that did not so you can inspect them:

```python
import asyncio

import pytest

from demo.src import staging_output_config, transform_output_config
from demo.src.runners import staging, transform
from dynamicio.testharness import ExpectedOutputs


class TestPipeline:
    @pytest.mark.end_to_end
    def test_dag_with_mock_sample_input_data(self):
        with ExpectedOutputs() as outputs:
            staging.main()
            asyncio.run(transform.main())

            outputs.check_all(staging_output_config)  # STAGED_FOO and STAGED_BAR: every output that declares `expected`
            outputs.check_all(transform_output_config)  # FINAL_FOO and FINAL_BAR
```

`check` reads the generated and the expected file with plain pandas (no schema or cast can hide a difference) and compares them under a set of
`Expectations`. An exception raised by the task itself is never masked by the report, and a missing generated or expected file is recorded as a failure
like any other, so one broken output never hides the others.

#### Making expectations stricter or more relaxed

`Expectations` is an immutable set of rules. The defaults suit a pipeline test: same data, rows and columns in any order, dtypes included, floats within
`rtol=1e-5`. Pass your own to a session (applies to every check), to one `check` (`expectations=...`), or change single rules for one `check` (`**rules`):

| Rule | Default | Effect |
|---|---|---|
| `check_row_order` | `False` | Require rows in the same order. When ignored, rows are aligned by index label, or sorted by `sort_by` |
| `check_column_order` | `False` | Require columns in the same order |
| `check_dtype` | `True` | Require identical dtypes (`int64` is not `float64`) |
| `check_exact`, `rtol`, `atol` | `False`, `1e-5`, `1e-8` | Exact floats, or the tolerance to compare them with |
| `sort_by=["id"]` | `()` | Sort both frames by these columns first: for outputs whose index means nothing |
| `only_columns=[...]` | all | Compare just these columns |
| `ignore_columns=["run_id"]` | `()` | Leave volatile columns (timestamps, run ids) out |
| `list_columns_to_sort=["tags"]` | `()` | List-valued columns whose element order does not matter |
| `assertions=[fn, ...]` | `()` | Your own checks `fn(actual, expected)`, run after the frames matched; raise `AssertionError` to fail |
| `check_parquet_schema` | `False` | For parquet, also require the same arrow schema and stored pandas index as the expected file, which loaded frames can hide |

```python
from dynamicio.testharness import ExpectedOutputs, Expectations

def prices_are_positive(actual, expected):
    assert (actual["price"] > 0).all(), "non-positive price"

session = Expectations(ignore_columns=["run_id"]).also_assert(prices_are_positive)

with ExpectedOutputs(session) as outputs:
    staging.main()
    outputs.check(staging_output_config, "STAGED_FOO")                                   # session rules
    outputs.check(transform_output_config, "FINAL_BAR", sort_by=["id"], rtol=1e-3)   # relaxed for this output only
    outputs.check(transform_output_config, "FINAL_FOO", expectations=Expectations.strict())   # strict for this output
```

`Expectations.strict()` requires row and column order and exact floats; `Expectations.lenient()` ignores dtypes and loosens the tolerance to `1e-3`;
`expectations.with_(...)` derives a variant. Dict, list and set cells (which pandas cannot hash) are compared by their canonical JSON.

#### What a run verified

`check_all` decides at runtime which outputs it verifies (those whose resource definition declares `expected`), so the test file alone does not list
them. The harness therefore says it, for passing runs as well as failing ones. At the end of the session it prints:

```
2 matched, 0 failed  [demo/tests/runners/test_staging.py::TestStaging::test_staging_writes_the_staged_foo_and_bar_datasets]
  ✓ STAGED_FOO  10 rows x 4 columns, as staged_foo.parquet
  ✓ STAGED_BAR  10 rows x 4 columns, as staged_bar.parquet
```

and pytest repeats every session in its terminal summary (green `✓` matched, red `✗` failed, yellow `↻` updated and `-` skipped), because pytest hides the
output of passing tests otherwise. Use `-s` to see the live print, `-p no:dynamicio` to turn the summary off, and `outputs.report` (a `SessionReport`)
to build your own. A failure's `OutputMismatchError` also lists the outputs that matched. Nothing is printed when a session checked nothing.

Why `check_all` stays the recommended default, with that report: a list of `check` calls drifts (an output added to the definition is silently
unverified, the failure mode that matters most in a regression test), while the definition is already the single source of truth of which outputs have a
reference. The report restores the visibility a hand-written list gave. If you prefer the list, keep writing `check` calls; both work together.

#### Other session features

| Feature | Effect |
|---|---|
| `outputs.check(..., params={"day": "2024-01-01"})` | Fills `{day}` in the generated and expected paths, for outputs whose file name carries a runtime parameter |
| `outputs.check_all(config, skip=[...], rules_for={"KEY": {...}})` | `check` for every resource of the config that declares `expected`, so a new output is covered without touching the test; those with unfilled `{placeholders}` are listed in `outputs.skipped` |
| `outputs.check_each(config, "KEY", {"table": [...], "shard": range(4)})` | `check` for every combination of the placeholder values, each reported by its own params |
| `ExpectedOutputs.clear_each(config, "KEY", grid)` | `clear` for every combination of the placeholder values |
| `ExpectedOutputs.clear_all(config)` | `clear` every output of the config that declares `expected`, so a stale file of an earlier run can not pass for a fresh one |
| `ExpectedOutputs(environment="NAME")`, `outputs.check(..., environment="NAME")` | Run and check in that environment (see [Choosing the environment](#choosing-the-environment)); an unknown name is reported as a failure listing the defined ones |
| `outputs.check(..., label="...")` | The name used in the report (default: the resource name, with `params`) |
| `ExpectedOutputs(keep_files=True)` | Keep the generated files even when they match |
| `ExpectedOutputs.clear(config, "KEY", ...)` | Remove leftovers of an earlier, interrupted run at the start of a test |
| `ExpectedOutputs.read_generated(...)` / `read_expected(...)` | Get the frames yourself |
| `outputs.compare(actual_df, expected_df, "name")` | Record a comparison of frames you built, without any files |
| `outputs.errors`, `outputs.passed`, `outputs.updated`, `outputs.skipped`, `outputs.report` | Inspect the outcome without raising; `report` is the structured account described in [What a run verified](#what-a-run-verified) |

Tests that write the same local outputs (the same task run in different ways) overwrite each other's files, and with `pytest-xdist` they race. Wrap such a
test in `OutputSandbox(tmp_path, output_config)`: the outputs of that config go below `tmp_path` and the checks follow them (the `expected` files stay put).
Pass the task's *output* configs only, an input config would be redirected too. Open it inside the `ExpectedOutputs(environment=...)` block (or an `EnvironmentSwitch`) when the test runs in another environment, so it redirects that environment's resources.

### Choosing the environment

A test should say which environment it runs in, rather than rely on whatever `ENVIRONMENT` the process happens to have. Resource definitions list their
environments in order, and **the first one listed is the default** (list the local one first, as in the examples above). `ExpectedOutputs` picks the
environment like this:

- `ExpectedOutputs()` uses, for every `IOConfig`, the first environment its resource definitions list, whatever the process runs in.
- `ExpectedOutputs(environment="LOCAL_E2E")` makes every `IOConfig` that defines `LOCAL_E2E` select it for the `with` block, so the task under test runs in it
  too, and the checks compare its outputs. The configs get their own `env_identifier` back on exit, also when the test fails, so no `monkeypatch` is needed.
  A name that no config defines raises a `ValueError` on entering, so a typo cannot silently run against the wrong files.
- `ExpectedOutputs(..., configs=[input_config, output_config])` switches only those configs.

`IOConfig` objects are normally created once per process (module-level names the tasks import), and the switch changes their `env_identifier`, which is
read on every `get`. `IOConfig.instances()` lists the live ones.

To make the switch visible in the test, or to switch configs without checking outputs, use `EnvironmentSwitch(environment, *configs)`. It validates that every
config defines the environment, and restores them on exit:

```python
with EnvironmentSwitch("LOCAL_E2E", input_config, output_config), ExpectedOutputs(environment="LOCAL_E2E") as outputs:
    Task().run()
    outputs.check(output_config, "FINAL_BAR")
```

The building blocks are public too: `FrameComparison(expectations).compare(a, b)` compares two frames (raising `AssertionError`) and `OutputFiles(config, key, params)`
finds, reads, removes and promotes the files of one resource.

When a change of output is intended, regenerate the reference files instead of editing them by hand:

```shell
DYNAMICIO_UPDATE_EXPECTED=1 uv run pytest demo/tests/test_pipeline.py   # writes the generated outputs over the expected files; review the diff
```

The end-to-end test above is `demo/tests/test_pipeline.py`; the sample data it runs on lives under `demo/tests/data`.

### Recommended testing practices

These are the patterns the harness is built for. They keep tests short, deterministic and the same everywhere, so a test reads like the one next to it.

**Test layers**

| Layer | What it proves | How |
|---|---|---|
| Unit | One transformation does the right thing | Plain pandas frames in, frames out; no I/O. Use `outputs.compare(actual_df, expected_df, "name")` for the comparison report |
| Task integration | A task reads its resources and writes the expected outputs | One test per task, `LOCAL` data, `ExpectedOutputs` as above |
| End to end | The tasks hand data to each other | Run the tasks in DAG order in a `LOCAL_E2E` environment, so each reads the previous task's output, then check all outputs |

The unit tests mock backends; none of this proves that a CLOUD reader or writer works. For that, add a test that runs against a `CLOUD` resource definition
with an in-memory object store (see `_json_object_store` in `tests/test_mixins/test_s3_mixins.py`), and check a change of I/O against a development account
before releasing it.

**Make every test look the same**

```python
ENVIRONMENT = "LOCAL"


class TestStageTask:
    @pytest.mark.integration
    def test_stage_task_with_mock_sample_input_data(self):
        # Given: the configs of the task and the sample data, both in the ENVIRONMENT
        with EnvironmentSwitch(ENVIRONMENT, stage_input_config, stage_output_config), ExpectedOutputs(environment=ENVIRONMENT) as outputs:
            # When
            Stage({}).run()

            # Then
            outputs.check(stage_output_config, "STAGED_FOO")
```

- Name the environment in the test, in one constant, and use the same `with` block in every test. Do not hide it in a fixture or patch `env_identifier`.
- Put the run inside the block: checks are lazy, so the report lists everything that differs, and an exception from the task itself is never masked.
- Test the task through the same entry point the scheduler uses (`Stage({}).run()`, `python -m ...`), not through its internals.

**Expected outputs**

- Declare `expected:` in the resource definition, next to the `local` path, and keep the reference files in version control next to the sample data. A reviewer
  then sees an intended change of output in the diff.
- Regenerate them with `DYNAMICIO_UPDATE_EXPECTED=1` and review the diff; never edit them by hand.
- Keep the defaults strict (dtypes, tolerance `1e-5`) and relax one output with `check(..., sort_by=[...], ignore_columns=[...])` when it is genuinely volatile
  (run ids, timestamps, unordered lists), so the reason sits next to the check.
- Use `Expectations(check_parquet_schema=True)` for parquet outputs that other systems read: a loaded frame can hide a changed arrow schema or index.
- Prefer `outputs.check_all(output_config)` to a list of `check` calls: it covers every output that declares `expected`, so an output added later is verified
  without touching the test. Name an exception with `skip=[...]`, and relax one output with `rules_for={"KEY": {"sort_by": [...]}}`.
- A resource whose path has several `{placeholders}` (tables, shards) is one `check_each(config, "KEY", grid)`, and `ExpectedOutputs.clear_each` removes leftovers first.

**Determinism**

- Fix everything that varies between runs before the task runs: the clock (`time.time`), the number of worker processes if it changes the order of results,
  random seeds, and the `as_of` timestamp the scheduler would pass. Do it with a fixture the test requests by name, not an autouse one.
- Sample data is small, generic and committed. Never test against production data or credentials.

**Parallel runs**

- Tests that write the same local outputs race under `pytest-xdist`. Give each its own directory with `OutputSandbox(tmp_path, output_config)`, or give each
  test its own environment variant. Run the end-to-end test on its own, as it writes the shared files of every stage.
- In CI, run the integration tests in parallel (`pytest -n auto --dist loadfile`) and the end-to-end test separately.

**Where a test belongs**

A test that exercises your own logic (for example that results do not depend on how work is split into shards) is not a dynamicio test, even if it uses the
harness to check files: keep it with the code it tests. Tests of the task wiring (resources in, outputs out) are the ones that belong with the task tests.
