# 0010. A polars engine with one engine for each repo and a file contract that both engines share

- Status: Proposed (dynamicio 12.0.0)
- Date: 2026-11-17

## Context

dynamicio reads and writes pandas frames only. pandas, pyarrow and awswrangler are hard dependencies, and `core.py` calls pandas in the read, write and validation steps.
The mixins carry the transport: local files, local batch, S3 file, S3 prefix, Postgres, Kafka (write only) and Athena (read only).

On one build shard of a consumer repo, a polars build without dynamicio took 29.6 s and 12.1 GB peak memory. The pandas build with dynamicio took 86.0 s and 17.0 GB.
The aim of v12 is to give that gain to dynamicio users and keep the dynamicio design. In that design, the user writes no I/O code. The resource definition and the
Pandera model are the only interface. An eager polars read keeps the pandas memory profile, because it holds the full input and the full output in memory. The gain
needs lazy reads that push the column and row filters down to S3, and streaming writes.

A probe on polars 2.1.0, pandas 2.3.3 and pyarrow 26.0.1 showed that a file from the polars writer gives a different frame to a pandas reader:

| Fact in the file | pandas writer | polars writer | Effect on a pandas reader |
|---|---|---|---|
| Float NaN | Stored as null | Stored as NaN | Null checks do not find the values |
| pandas metadata | Present, with a `RangeIndex` | Absent | The reader guesses the dtypes |
| Nullable `Int64` | `Int64` | `int64` with nulls | Reads as `float64` |
| Nullable `boolean` | `boolean` | `bool` with nulls | Reads as `object` |
| Timestamp unit | Unit of 10.x | polars default | The unit is different (a consumer repo found it) |

The polars `metadata=` argument does not give a block that pandas reads. The pandas metadata must go through `arrow_schema=`.

pandas and polars also disagree on missing values. For a float column `[1.0, NaN, null]`, pandas counts 2 missing values and polars counts 1, because polars keeps NaN
as a value. polars `mean` and `sum` then give NaN, and polars sorts NaN above all numbers.

The resource `options` go to pandas and awswrangler functions. Each reader and writer accepts only the argument names of these functions, for example
`@allow_options(pd.read_csv)` in `with_local.py` and `@allow_options(wr.s3.read_parquet)` in `with_s3.py`. Many of these names do not exist in polars or have a
different meaning there.

Downstream repos read the files and Kafka messages that dynamicio writes. A consumer repo compares Kafka message hashes for its incremental publish, so a change of one
byte in a message causes a full publish.

## Decision

### Rule 1: a repo uses one engine for all its resources

The engine is a setting for the process. The resource definition has no `engine:` key. `read()` returns a frame of the engine, and `write()` raises an error when it
gets a frame from the other engine. A repo moves to polars in one step with `dynamicio migrate --engine polars`. This is a new mode
of the `dynamicio migrate` command, which changes YAML schemas to Pandera models in 11.x. The new mode changes `pandera.pandas` models to `pandera.polars` models.

The engine does not give the user a frame of the other engine. There is one exception inside the engine. The polars engine reads Athena through awswrangler and changes
the result to a polars frame (decision 8).

### Rule 2: a file that one engine writes gives the same frame to the other engine

The reference is the pandas engine of dynamicio 11.1.0. The bug fixes of 11.1.0 changed some outputs, so the expected files come from 11.1.0 and not from 10.x.

- The polars writer always writes files that the 11.1.0 pandas engine reads as it reads its own files. A user cannot turn this off.
- Text files and Kafka messages are equal byte for byte.
- The polars engine reads the files from the pandas engine, from 11.1.0 and from 10.x.
- Each format passes the 4 pairs of writer engine and reader engine against the expected files.

A pandas file can store a named index in its pandas metadata. pandas then gives the index back, and polars reads it as a normal column. The schema column selection
removes the columns that the model does not declare, so the two engines give the same columns.
- Rule 2 compares the columns of the frame. The pandas index is not part of the contract.
- A pandas read keeps a named index, and a polars read removes it unless the model declares it as a column. A polars write then cannot give the index back.
- A repo that needs an index value on both engines writes it as a column (`reset_index()`) and declares it in the model.

### The 5 open decisions

1. How a repo sets the engine. The only way is `dynamicio.configure(engine="polars")`, which accepts `"pandas"` or `"polars"`. There is no environment variable, so
   the engine is in the code of the repo and a deployment cannot change it.
   - Without the call, the engine is pandas. An unknown value raises an error.
   - The setting freezes at the first read or write. A later `configure()` with a different engine raises an error.
   - A process that `spawn` starts does not get the call of its parent. A repo calls `configure()` in its package `__init__.py`, so each process runs it on import.
   - dynamicio logs the engine at the first read or write.
2. pandas becomes optional in v13, with `dynamicio[pandas]` and `dynamicio[polars]` extras.
   - In v12, pandas stays a hard dependency and `dynamicio[polars]` adds `polars>=2.1`.
   - polars 2.1 needs Python 3.11 or later, and Python 3.10 reaches its end of life in October 2026. dynamicio 12.0.0 needs Python 3.11 or later,
     and CI runs Python 3.11 to 3.14. dynamicio 11.x keeps Python 3.10.
   - The polars minimum is always the latest polars release when a dynamicio release ships (2.1.0 on 2026-11-17).
   - awswrangler needs pandas, so it moves to the `[pandas]` extra in v13. A v13 install with `[polars]` only has no Athena reads.
   - Phase 7 records the v13 dependency list and an import test for each extra.
3. The null rule. The polars engine changes float NaN to null when it reads and when it writes. The rule is always on and has no option. The pandas writer already
   stores NaN as null, so validators, metrics and files then agree across engines.
4. The Kafka key for polars. A new resource option `key_column` names the column that gives the message key.
   - The polars engine requires `key_column`, because a polars frame has no index. A polars Kafka resource without it raises an error when the user builds the resource.
     A row number is not a key: it is equal to the pandas key only when the pandas index is a `RangeIndex`.
   - The pandas engine also accepts `key_column`. Without it, the pandas engine uses the index, as in 11.x.
   - The key column is not in the message value. This gives the same bytes as `set_index(key)` on pandas, which a consumer repo uses today.
   - `key_generator` stays on both engines. It gets the value of `key_column`, or the index on pandas.
5. The CSV index. When `index` is not `False`, the polars CSV writer writes the same leading column as pandas. This column has an empty header and the row
   numbers 0 to n-1. The default of `index` is `True` on both engines, as in `DataFrame.to_csv`. The schema column selection removes this column at read time on both engines.
   The migrate command writes `index` into each CSV resource definition.
   - The byte contract covers a pandas frame with the default `RangeIndex` only. A polars frame has no index, so the leading column has the row numbers.
   - A pandas frame with a named index writes the index name in the header. The polars equivalent is a normal first column with that name and `index: false`.
     These 2 writes give equal bytes.
   - A pandas frame with an unnamed index that is not a `RangeIndex`, or with a `MultiIndex`, has no polars equivalent. The contract does not cover these frames.

YAML schemas are not an open decision. dynamicio 11.0 removed them ([0009](0009-pandera-models-as-the-only-schema-declaration.md)), and Pandera models are the only
schema declaration.

### Other decisions

6. Resource options. The 11.x option names are the one option vocabulary of a resource definition on both engines. Each engine has a fixed map from these
   names to the arguments of its own reader and writer, for each format and backend. For example, the CSV option `sep` becomes `separator` inside the polars engine.
   - An option that has no entry for the polars engine raises an error when the user builds the resource. It does not get to the first read or write.
   - The migrate command does not change option names. A resource definition stays the same when a repo changes its engine.
7. User callables on Kafka. The signatures and the call order of 11.x stay the same on both engines (`with_kafka._send_messages`):
   - `key_generator(key, row)` gets the key and the row as a `dict`.
   - `document_transformer(row)` gets the row as a `dict`.
   - `key_serializer(key)` gets the return value of `key_generator`.
   - `value_serializer(value)` gets the return value of `document_transformer`, which can be a value of any type.

   The pandas engine makes the row with `df.to_dict("records")`. On pandas 2.3.3, numbers and strings are `int`, `float` and `str`, and timestamps are
   `pd.Timestamp`. A float null is NaN and an `Int64` null is `None`. polars `to_dicts()` gives `datetime` and `None` for all nulls. The polars engine changes the
   row to the pandas types before it calls `key_generator` and `document_transformer`, so custom callables give the same bytes on both engines. The default
   serializers give the same bytes on both engines.
8. Athena on polars. In v12, the polars engine reads Athena through awswrangler and changes the result to a polars frame inside the engine. The user gets a polars
   frame only. In v13, an install with `[polars]` only has no Athena reads unless a later ADR gives polars its own Athena reader.
9. JSON on polars. The polars engine reads and writes the `records` orient, with and without `lines`. The pandas orients `split`, `index`, `columns`, `values` and
   `table` have no polars reader. A polars JSON resource with one of them raises an error when the user builds the resource, as hdf and pickle do.
10. On the pandas engine, `scan()` and `sink()` raise `NotImplementedError`. The message names the polars engine. pandas has no lazy frame, so these methods do
    not return an eager frame under a lazy name.
11. Tests on both engines. The pytest plugin of `dynamicio.testharness` (the `pytest11` entry point) adds an option `--dynamicio-engine=pandas|polars` that calls `configure()` before the first test. CI runs
    one job for each engine, because one process cannot change its engine.
12. One order of steps (change 12). Both engines select the columns, cast, validate and then compute metrics, on reads and on writes.
    - Reads in 11.x already use this order: `read()` calls `_apply_schema()` before the validations and metrics. Reads do not change.
    - Writes in 11.x validate and compute metrics on the frame of the user, and then call `_apply_schema()` before the writer. In 12.0, writes cast before
      they validate. This is a change in behavior of the pandas engine for writes only, and the CHANGELOG states it.
    - Phase 1 moves the pandas code without a change in behavior. The change of order for writes comes in phase 5.
13. Earlier ADRs apply to the polars engine.
    - [0002](0002-tolerate-missing-columns-in-parquet-prefix-reads.md): a parquet prefix read fills the missing columns with null and logs the same warning. A
      column that is missing from all objects raises an error. polars infers the schema from the first object, so `missing_columns="insert"` alone fails when
      the first object does not have a column. The polars engine gives `scan_parquet` an explicit `schema=` from the model columns and dtypes, so the result does
      not change with the order of the objects. The engine reads the footer of each object to count the objects that do not have each column.
    - [0004](0004-ranged-parquet-reads-with-filters.md): a filtered single-file parquet read reads only the row groups and columns it needs. polars gets this with
      the predicate and projection pushdown of `scan_parquet`.
14. The dtype map (change 5) covers these pairs at least:

    | pandas | polars |
    |---|---|
    | `int8` to `int64`, `Int8` to `Int64` | `Int8` to `Int64` |
    | `float32`, `float64` | `Float32`, `Float64` |
    | `bool`, `boolean` | `Boolean` |
    | `object` with strings, `string` | `String` |
    | `category` | `Categorical`, or `Enum` when the model gives the values |
    | `datetime64[unit]`, `datetime64[unit, tz]` | `Datetime(unit)`, `Datetime(unit, time_zone)` |
    | `timedelta64[unit]` | `Duration(unit)` |

    A dtype that has no pair raises an error when the polars engine compiles the model.

    The map is many to one: `int64` and `Int64` both become `Int64`, and `object` and `string` both become `String`. The pandas metadata of a polars file
    (change 17) needs the pandas dtype of each column, so the polars model keeps it.
    - The migrate command writes the pandas dtype of each field into the field metadata of the polars model, for example `pa.Field(metadata={"pandas_dtype": "int64"})`.
    - A polars model without this metadata uses a fixed reverse map: `Int64` to `int64` when the column has no null and to `Int64` when it has a null,
      `String` to `object`, `Boolean` to `bool`, and `Float64` to `float64`. The contract covers the pandas dtypes of this reverse map only.
15. Integration tests on both engines run against [floci](https://github.com/floci-io/floci), a local AWS emulator. One container gives S3, RDS with
    `postgres:16` and MSK with a Redpanda broker.
    - `moto.mock_aws` does not intercept polars S3 calls. SQLite, which the unit tests use for Postgres, has no COPY.
    - A session fixture starts floci, pinned by image digest, with the Docker socket. The integration tests skip when Docker is not available. CI runs them in
      one job on one Python version.
    - The unit tests stay on moto and the mocks. floci accepts a DeleteObjects call with more than 1000 keys, and S3 rejects it. Thus the test of the
      batches of 1000 keys stays on moto.
    - Athena stays a mock, because floci does not emulate Athena.
    - A spike on floci 2.5.0 ran 12 checks of the path-prefix writes and the missing-key errors. The results were the same as on S3 in the development
      account. The spike also passed the Postgres COPY of 11.x with the NULL rule, and the Kafka message bytes.
    - The fixture is change 26. RND-25426 adds it with the pandas integration tests before phase 1.

### Success measure

The test is the build shard of the consumer repo in the Context. On it, the 12.0 polars engine takes at most 10% more time and peak memory than the polars build
without dynamicio. The limits are 32.6 s and 13.3 GB.

### The 27 changes, in delivery order

Phases 1 to 7 are dynamicio 12.0.0.

| Phase | Changes | Scope |
|---|---|---|
| 1 | 1 to 4 | Engine seam. An `Engine` abstract base class in `dynamicio.engines` with 16 members (read, scan, cast, validate, write, sink). `PandasEngine` calls the current code without a change in behavior. The pandas I/O code stays in the `dynamicio.mixins.*` modules in 12.x, so a patch of a module binding changes the code that runs. A test patches each binding that the dynamicio tests and a consumer repo patch. Each engine has a registry of validators and metrics with the same names. `ModelCompiler` accepts `pandera.polars` models and raises if the model type is not the engine type. `configure()`, the freeze and the wrong-frame error (decision 1). |
| 2 | 5 to 9 | Schema and null rule. The dtype map (decision 14). The null rule. polars validators and metrics that give the same results as pandas. Each is an expression, and all run in one `collect_all` with the Pandera data checks. |
| 3 | 17, 18, 20, 26 | Eager codecs and the parquet contract. The pandas metadata through `arrow_schema=`. Footer details equal to 11.1.0: timestamp unit, row group size, snappy compression, coerced timestamps and INT96 reads. JSON (decision 9) and CSV (decision 5) bytes equal to 11.1.0. The option maps (decision 6). The polars S3 tests run against floci (decision 15). |
| 4 | 10, 14, 16 | `scan()` returns a `LazyFrame`. It checks names and types with `collect_schema()` and adds the casts to the plan. polars reads S3 with a `credential_provider` from the boto3 session. A missing key raises the dynamicio error, so `missing_ok` applies. A prefix read lists the keys with boto3 and makes one `scan_parquet` call that keeps ADR 0002 and ADR 0004 (decision 13). |
| 5 | 11, 12, 13, 15 | `sink()` runs all sinks in one `collect_all`. Writes go through a boto3 multipart upload (64 MiB parts, 4 in flight, ACL) that completes only on success and aborts on error. Both engines select columns, cast, validate and then compute metrics, in that order (decision 12). Both engines raise the same dynamicio errors. |
| 6 | 19, 21, 22, 23 | A footer-only read and a sink from several scans that merges the pandas metadata of all parts (a consumer repo needs it). Postgres COPY in chunks of 100k rows with the same NULL rule, tested on floci RDS. Kafka keys from decision 4 and row types from decision 7, with batches, `poll()` and a flush timeout, tested on the floci broker. Athena through awswrangler (decision 8). hdf and pickle resources raise on polars when the user builds the resource. |
| 7 | 24, 25, 27 | No fork on polars, and `POLARS_MAX_THREADS` applies. `dynamicio.testharness` comparisons for each engine and the `--dynamicio-engine` option (decision 11). The `dynamicio[polars]` extra. CI runs all tests on both engines. The migrate command, one demo for each engine and the docs. |

Phase 1 as built:

- An engine takes the IO object as an argument and has no state of its own. The pandas cast stays in `DynamicDataIO._has_valid_dtypes` in 12.x, so a test
  that patches `dynamicio.core.logger` still sees the cast warnings. `_apply_schema`, `validate_from_schema` and `log_metrics_from_schema` stay on
  `DynamicDataIO` and call the engine, so a patch of these methods still applies.
- The base class of a polars model is `dynamicio.schema.polars.DataModel`. `dynamicio.schema` does not import it, so dynamicio imports without polars.
- The model check runs when the user creates an IO object, and again at each read and write, because `configure()` can change the engine between them.
  The checks run before the freeze, so a refused read or write does not fix the engine. `ModelEngineMismatchError` is also a `ValueError`, as the
  11.x error of a polars model.
- The frame check runs at `write()` and `sink()`, before any I/O. It compares the top-level module name of each class in the MRO of
  the frame, so a subclass of a polars or pandas frame counts as one, and it imports no polars. `configure()` checks the freeze before it looks for polars.
- The dtype check of `DataframeSchema` knows only the pandas dtype names. In phase 1, only the integer and float columns of a polars model compile. Phase 2
  (the dtype map of decision 14) adds the other polars dtypes.
- The polars engine checks models and frames. Its I/O, cast, validations and metrics raise `NotImplementedError` until phases 2 to 6.

Change 26 is the floci fixture of decision 15. RND-25426 delivers it before phase 1, with the pandas integration tests. The polars S3 tests of phase 3 are its first polars users.

Change 5 replaces the neutral dtype vocabulary of the first proposal. That vocabulary was for YAML schemas, which dynamicio 11.0 removed. A pandas repo declares
`pandera.pandas` models and a polars repo declares `pandera.polars` models. The polars writer uses the dtype map to build the pandas metadata (change 17). The tests
of the 4 pairs use it to compare frames.

## Consequences

- A repo that does not call `configure()` gets the pandas engine. Its only change in behavior in 12.0 is the order of steps (decision 12).
- A repo cannot use pandas for one resource and polars for another. A repo that needs both engines must split into two processes that share files.
- The migrate command changes the resource definitions, the options and the models. It does not change the transformation code of the repo. A repo must change its
  own pandas code to polars before it moves.
- The polars engine cannot store a float NaN that is different from null. The pandas engine cannot do this today either.
- The polars writer costs more than plain polars: it builds the pandas metadata, writes the CSV index column and keeps the 11.1.0 byte format. Rule 2 requires
  this cost. The success measure limits it.
- The polars engine changes Kafka rows to pandas Python types (decision 7). This costs time on each message, and keeps custom serializers correct.
- hdf, pickle and the JSON orients of decision 9 have no polars reader. A polars repo must move these resources to parquet or to `records` JSON first.
- Tests that patch internals, such as a consumer repo that patches `dynamicio.mixins.with_s3.wr.s3.read_json`, keep working in 12.x. The pandas I/O code stays in
  the `dynamicio.mixins.*` modules, so the code looks up `wr` in the module that the test patches. A re-export only does not give this, because a moved function
  looks up `wr` in its new module.

## References

- Jira: the epic is RND-25348, the phases 1 to 7 are RND-25354, the floci integration tests are RND-25426 and the move of the first consumer repo is RND-25361.
- dynamicio 11.1.0 bug fixes: PR #139.
