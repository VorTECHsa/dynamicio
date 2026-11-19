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

The polars reader differs from the 11.1.0 pandas reader for 4 kinds of file. In each case the polars engine keeps a value that the pandas engine loses, or reads
a file that the pandas engine refuses. Rule 2 does not include these cases, and the tests of the 4 pairs do not use them. Each case has its own test.

1. A CSV or JSON string column without a string `dtype` option. The pandas reader infers the type, so `007` becomes the integer 7, and the cast gives `"7"`.
   The polars reader keeps the text `007`. A repo that needs the same value on both engines sets a string `dtype` for the column on the pandas engine, for
   CSV and for JSON.
2. A JSON file without records, as `DataFrame.to_json` writes a frame without rows. The pandas engine raises `ColumnsDataTypeError` (`KeyError` in 11.x),
   and the polars engine gives a frame without rows with the columns of the model.
3. A JSON epoch under a `Datetime` or a `Duration` column. `pandas.read_json` converts only the columns with a date name, such as `created_at`, and gives a
   naive `datetime64[ns]`. The cast of a column with a time zone then fails. Another column stays an integer, and the cast reads it in nanoseconds, so 1 ms
   becomes 1 ns. The polars engine reads each epoch in `date_unit` and keeps the value. A column with a time zone gives a UTC instant.
4. A CSV resource with `header: false` and `names`. The pandas reader raises `TypeError` for `header: false`, and the polars engine reads the file that the
   writer gives.

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
   - Phase 7 adds an import test of an install without the `polars` extra. The v13 dependency list and an import test for each v13 extra are v13 work.
3. The null rule. The polars engine changes float NaN to null when it reads and when it writes. The rule is always on and has no option. The pandas writer already
   stores NaN as null, so validators, metrics and files then agree across engines.
   - The cast of the polars engine applies the rule. Each float column of the model gets `fill_nan(None)`, so the validators and the metrics count a NaN as a null.
     Reads and writes cast, so the rule applies to both.
   - A float column that casts to an integer gets `fill_nan(None)` before the cast. pandas casts a NaN to `<NA>` of a nullable integer.
     A strict polars cast refuses a NaN.
4. The Kafka key for polars. A new resource option `key_column` names the column that gives the message key.
   - The polars engine requires `key_column`, because a polars frame has no index. A polars Kafka resource without it raises an error when the user builds the resource.
     A row number is not a key: it is equal to the pandas key only when the pandas index is a `RangeIndex`.
   - The pandas engine also accepts `key_column`. Without it, the pandas engine uses the index, as in 11.x.
   - The key column is not in the message value. This gives the same bytes as `set_index(key)` on pandas, which a consumer repo uses today.
   - `key_generator` stays on both engines. It gets the value of `key_column`, or the index on pandas.
5. The CSV index. When `index` is not `False`, the polars CSV writer writes the same leading column as pandas. This column has an empty header and the row
   numbers 0 to n-1. The default of `index` is `True` on both engines, as in `DataFrame.to_csv`. The schema column selection removes this column at read time on both engines.
   The report of the migrate command lists each CSV resource definition without an `index` option.
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
   - A Python `datetime` and `timedelta` keep microseconds only, so `to_dicts()` loses the last 3 digits of a `Datetime("ns")` value. The engine makes the
     `pd.Timestamp` and `pd.Timedelta` values from the columns, with `Series.to_pandas()`, and not from the values of `to_dicts()`.
   - A test sends a value that ends in `.123456789` through a custom serializer on both engines and compares the bytes.
8. Athena on polars. In v12, the polars engine reads Athena through awswrangler and changes the result to a polars frame inside the engine. The user gets a polars
   frame only. In v13, an install with `[polars]` only has no Athena reads unless a later ADR gives polars its own Athena reader.
9. JSON on polars. The polars engine reads and writes the `records` orient, with and without `lines`. The pandas orients `split`, `index`, `columns`, `values` and
   `table` have no polars reader. A polars JSON resource with one of them raises an error when the user builds the resource, as hdf and pickle do.
10. On the pandas engine, `scan()` and `sink()` raise `NotImplementedError`. The message names the polars engine. pandas has no lazy frame, so these methods do
    not return an eager frame under a lazy name.
11. Tests on both engines. The pytest plugin of `dynamicio.testharness` (the `pytest11` entry point) adds an option `--dynamicio-engine=pandas|polars`.
    - The option calls `configure()` before the first test, so it sets one engine for the whole pytest process. A consumer repo runs one pytest process
      for each engine.
    - The tests of dynamicio do not use the option, except the polars demo. Each test sets its engine, and a fixture resets the engine after the test.
      One CI test job thus runs the tests of the two engines ("Phase 7 as built").
12. One order of steps (change 12). Both engines select the columns, cast, validate and then compute metrics, on reads and on writes.
    - Reads in 11.x already use this order: `read()` calls `_apply_schema()` before the validations and metrics. Reads do not change.
    - Writes in 11.x validate and compute metrics on the frame of the user, and then call `_apply_schema()` before the writer. In 12.0, writes cast before
      they validate. This is a change in behavior of the pandas engine for writes only, and the CHANGELOG states it.
    - Phase 1 moves the pandas code without a change in behavior. The change of order for writes comes in phase 5.
    - `sink()` on polars is the one exception. Its metric queries run in the same pass as its validations ("Phase 5 as built").
13. Earlier ADRs apply to the polars engine.
    - [0002](0002-tolerate-missing-columns-in-parquet-prefix-reads.md): a parquet prefix read fills the missing columns with null and logs the same warning. A
      column that is missing from all objects raises an error. polars infers the schema from the first object, so `missing_columns="insert"` alone fails when
      the first object does not have a column. The polars engine gives `scan_parquet` an explicit `schema=` of the model columns, so the result does
      not change with the order of the objects. The engine reads the footer of each object to count the objects that do not have each column.
    - The `schema=` of `scan_parquet` gives the types of the files, and it does not cast. An `Int32` column under an `Int64` type raises `SchemaError`
      at scan time (probe on polars 2.1.0). The engine therefore takes the type of each column from the footers, and not from the model.
    - When the footers give different types for a column, the engine uses the widest type. `ScanCastOptions(integer_cast="upcast", float_cast="upcast")`
      then reads the narrower objects. Footers with types that do not upcast, such as `String` and `Int64`, raise `ColumnsDataTypeError` before the scan.
    - The lazy cast of the model then runs in the plan, as for a single file. A file type that the pandas engine casts to the model type thus also casts on
      polars, for example an `Int32` file under an `Int64` model.
    - [0004](0004-ranged-parquet-reads-with-filters.md): a filtered single-file parquet read reads only the row groups and columns it needs. polars gets this with
      the predicate and projection pushdown of `scan_parquet`.
14. The dtype map (change 5) covers these pairs at least:

    | pandas | polars |
    |---|---|
    | `int8` to `int64`, `Int8` to `Int64` | `Int8` to `Int64` |
    | `uint8` to `uint64`, `UInt8` to `UInt64` | `UInt8` to `UInt64` |
    | `float32`, `float64` | `Float32`, `Float64` |
    | `bool`, `boolean` | `Boolean` |
    | `object` with strings, `string` | `String` |
    | `category` | `Categorical`, or `Enum` when the model gives the values |
    | `datetime64[unit]`, `datetime64[unit, tz]` | `Datetime(unit)`, `Datetime(unit, time_zone)` |
    | `timedelta64[unit]` | `Duration(unit)` |

    The unit is `ms`, `us` or `ns`. polars has no second unit, so `datetime64[s]` and `timedelta64[s]` have no pair. A dtype that has no pair raises
    `ValueError` when the polars engine compiles the model. Examples are `Date`, `Time`, `List`, `Struct` and `Decimal`.

    The map is many to one: `int64` and `Int64` both become `Int64`, and `object` and `string` both become `String`. The pandas metadata of a polars file
    (change 17) needs the pandas dtype of each column, so the polars model keeps it.
    - The polars model keeps the pandas dtype in the `pandas_dtype` key of the field metadata. `Field(pandas_dtype="Int64")` writes it, and a
      `pa.Field(metadata={"pandas_dtype": "Int64"})` also works. The migrate command writes it for each field.
    - The pandas dtype of the metadata must be a pair of the polars dtype of the column, else the model raises `ValueError` when it compiles. Any
      `category` pairs with any `Categorical` or `Enum`. A datetime pairs only with the same unit and time zone.
    - A pandas model with `pandas_dtype` raises `ValueError`, because the column type of a pandas model is its pandas dtype.
    - A polars model without this metadata uses a fixed reverse map. An integer gives its pandas name (`Int64` to `int64`). `String` gives `object`,
      `Boolean` gives `bool`, and `Categorical` and `Enum` give `category`. `Datetime` and `Duration` give `datetime64` and `timedelta64`, with the
      same unit and time zone. With a null, an integer gives its nullable pandas dtype (`Int64`), and `Boolean` gives `boolean`.
    - The null rule of the reverse map also applies to a numpy dtype of the metadata. A column with `pandas_dtype="int64"` or `"bool"` and a null
      thus gives `Int64` or `boolean`, as `DataFrame.to_parquet` gives for a pandas column with a null. The rule applies to `write()`, to `sink()`
      and to the pandas frames of the CSV, JSON, Postgres and Kafka writers. pandas then reads the exact integers, also above 2**53.
    - The contract covers the pandas dtypes of this reverse map only.
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
| 2 | 5 to 9 | Schema and null rule. The dtype map (decision 14). The null rule. polars validators and metrics that give the same results as pandas. The validators and the Pandera checks run in one `collect_all`. The metrics run in one `select` after the validations. |
| 3 | 17, 18, 20, 26 | Eager codecs and the parquet contract. The pandas metadata through `arrow_schema=`. Footer details equal to 11.1.0: timestamp unit, row group size, snappy compression, coerced timestamps and INT96 reads. JSON (decision 9) and CSV (decision 5) bytes equal to 11.1.0. The option maps (decision 6). The polars S3 tests run against floci (decision 15). |
| 4 | 10, 14, 16 | `scan()` returns a `LazyFrame`. It checks names and types with `collect_schema()` and adds the casts to the plan. polars reads S3 with a `credential_provider` from the boto3 session. A missing key raises the dynamicio error, so `missing_ok` applies. A prefix read lists the keys with boto3 and makes one `scan_parquet` call that keeps ADR 0002 and ADR 0004 (decision 13). |
| 5 | 11, 12, 13, 15 | `sink()` runs all sinks in one `collect_all`. Writes go through a boto3 multipart upload (64 MiB parts, 4 in flight, ACL) that completes only on success and aborts on error. Both engines select columns, cast, validate and then compute metrics, in that order (decision 12). Both engines raise the same dynamicio errors. |
| 6 | 19, 21, 22, 23 | A footer-only read and a sink from several scans that merges the pandas metadata of all parts (a consumer repo needs it). Postgres COPY in chunks of 100k rows with the same NULL rule, tested on floci RDS. Kafka keys from decision 4 and row types from decision 7, with batches, `poll()` and a flush timeout, tested on the floci broker. Athena through awswrangler (decision 8). |
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
- The dtype check of `DataframeSchema` knows only the pandas dtype names. In phase 1, only the integer and float columns of a polars model compiled.
  Phase 2 compiles all the dtypes of the dtype map (decision 14).
- The polars engine checks models and frames. Its I/O raises `NotImplementedError` until phases 3 to 6.

Phase 2 as built:

- `SchemaColumn` does not change. The `data_type` of a compiled polars column is a pandas dtype name, from the `pandas_dtype` metadata or the reverse
  map of decision 14. The engine takes the polars dtypes from the model, with `DtypeMap.of_model()`.
- The cast is strict. A `DataFrame` casts one column at a time, so a failed cast names its column. A `LazyFrame` gets the casts in its plan, and a failed
  cast raises when the frame is collected. polars 2 does not cast a string to a datetime, so a string column parses with `str.to_datetime`.
- A failed cast raises `ColumnsDataTypeError`, as on the pandas engine. A schema column that is not in the frame also raises it, and so does a frame column
  that is not in the schema.
  The polars engine reads `dynamicio.core.logger` at each message, so a test that patches it sees the messages of the two engines. The polars engine ignores
  `strict_dtype_check`.
- The validators and the Pandera column and dataframe checks run in one `pl.collect_all`. The metrics run in a second `select`, after the
  validations. Change 2 first had one `collect_all` for all of them. Two queries keep the order of decision 12: a failed validation stops the step
  before the metrics.
- A polars validator returns its lazy queries and a function that makes the `ValidationResult` from the collected frames. The 9 validators give the
  names, messages and values of the pandas validators. Tests compare the two engines on the same data, with 200 columns of random floats.
- `is_in` ignores nulls when it decides the result, as in pandas, so a null never fails the validation. The set of values comes from
  `to_pandas().unique()` without the nulls. After a failure, the count of cells that are not acceptable includes a null when the pandas `isin`
  does not accept it for the pandas dtype of the column. A float null matches NaN only. An integer or Boolean null matches no value. A string,
  categorical or temporal null matches None or NaN.
- The polars engine runs `Config.unique`, `Config.ordered` and `@pa.dataframe_check` as lazy queries, with the keys and the message template of the
  pandas engine. The counts and the examples come from the polars failure cases, so they are not always the pandas values. For a dataframe check, the
  count is the number of failing rows. A check that returns one bool has no failure cases, so its count is 1 when it fails. Each dataframe check is
  kept, and the engine joins the messages of checks with one name. `Config.unique` with column groups has one query for each group. As on the pandas
  engine, the result is the first group with repeated rows.
- The 8 metrics give the `METRIC` lines of the pandas engine. For an empty or an all-null column, `Min`, `Max`, `Mean`, `Std` and `Variance` give NaN.
  `Counts` and `UniqueCounts` give the counts of the pandas engine, for example 0 for an empty column and 2 and 1 for `[None, None]`. `CountsPerLabel` does
  not count nulls. It logs 0 for each category of an `Enum` that is not in the column, as `value_counts` does for a pandas `category`.

Phase 3 as built:

- The polars engine reads and writes parquet, CSV and JSON on `local` and `s3_file`, and reads `local_batch`. The other resources, `scan()` and `sink()`
  raise `NotImplementedError` until phases 4 to 6.
- The CSV and JSON writers change the frame to pandas with the pandas dtypes of the model, and call the pandas writers of 11.1.0. Thus the bytes are equal by
  construction, and the tests compare them for each case on a local disk and on floci S3.
  - The option `compression` goes to the pandas writer without a change. A gzip header has the time of the write and the file name.
    The option `{"method": "gzip", "mtime": 0}` fixes the time. Without it, two gzip writes differ on one engine too.
  - The byte contract thus covers the data after decompression. It covers the gzip bytes only for a fixed `mtime` and the same file name.
- The parquet writer calls `pyarrow.parquet.write_table`, not the polars `write_parquet` with `arrow_schema=`. pyarrow makes the arrow types and the pandas
  metadata from an empty pandas frame with the dtypes of the model, as `DataFrame.to_parquet` does. The footer schema, the metadata and the SNAPPY compression
  are then equal to 11.1.0. A `category` column has the code width of pandas: `int8` below 127 categories, `int16` below 32767 and `int32` after that.
- The readers are polars readers, and the cast of phase 2 then gives the dtypes of the model. A CSV column that is not a number or a Boolean reads as a string,
  so a value such as `007` keeps its text (exception 1 of rule 2). The cast parses the strings of a `Duration` with `pd.to_timedelta`, and the strings `True` and `False` of a
  `Boolean`. A `DataFrame` with a bad string raises `ColumnsDataTypeError`. A `LazyFrame` raises the polars `InvalidOperationError` when the caller collects it.
- A JSON integer under a `Datetime` or `Duration` column of the model is an epoch in `date_unit`. The pandas engine of 11.1.0 cannot cast a date name
  column to a time zone, and casts the epoch of another column in nanoseconds. The polars engine keeps the value, and reads a UTC instant for a time zone.
  A JSON file without records, as pandas writes a frame without rows, reads as a frame without rows with the columns of the model. The pandas engine raises
  `ColumnsDataTypeError` for this file. These are exceptions 2 and 3 of rule 2.
- The option maps of decision 6 are in `PolarsOptions`. A resource has one set of options for its reads and its writes, so a file type accepts the options of
  its pandas reader and of its pandas writer. The `options` of a resource go beside its `local` or `s3` section. Options inside the section have no effect on
  either engine.
- `EngineResourceError` is the error of decision 6 and decision 9. It is also a `ValueError`. The polars engine raises it when the user builds the resource,
  and again at each read and write, so a `configure()` or a change of the options after the build cannot bypass it. The refusal of hdf and pickle also comes in
  this phase, and not in phase 6.
- The CSV options that polars cannot apply raise `EngineResourceError`: a `na_values` map for each column, and a `decimal` mark other than `.` and `,`.
  `decimal: ","` becomes `decimal_comma`. A read of a CSV resource without a header line and without `names` also raises it, because the 11.1.0 pandas engine
  cannot read that file either. `names` becomes `new_columns`. Without `header`, the first line is data, as on pandas. A write with `header: false` is accepted.
- A file with another number of columns than `names` raises the polars `SchemaError`. pandas then makes an index of the first columns, or adds columns of
  nulls. The polars engine refuses these files, and rule 2 does not include them.
- The CSV and JSON writers change an integer column to a pandas nullable integer, so an integer above 2**53 next to a null keeps its value. A parquet write
  with `index: false` has the pandas metadata of `to_parquet(index=False)`. A write changes float NaN to null before its validations and metrics.
- A missing S3 key raises the error of the pandas engine: `S3KeyNotFoundError` for parquet and `ObjectNotFoundError` for CSV and JSON. `missing_ok` applies.
  An `s3_file` key that ends in `/` raises `NotImplementedError` at read, and a parquet write to it raises the `ValueError` of the pandas engine.

Phase 4 as built:

- `scan()` gives a `LazyFrame` of `local`, `local_batch`, `s3_file` and `s3_path_prefix`. `DynamicDataIO.scan()` adds the casts of the model to its
  plan, so a failed cast raises when the caller collects the frame. A schema column that no file has raises `ColumnsDataTypeError` at the scan.
- A scan does not validate or log metrics, because a lazy frame has no rows before the caller collects it. The step order of decision 12 applies to
  `read()` only. A scan with validations or metrics logs a warning. The caller collects the frame and calls `validate_from_schema`, or uses `read()`.
- Parquet and CSV go to `scan_parquet` and `scan_csv`, so a filter or a selection reaches the scan (ADR 0004). polars has no lazy reader of a JSON array, so
  a JSON scan reads the file with the eager reader and then calls `lazy()`. A CSV scan reads all rows for the types (`infer_schema_length=None`), as the
  eager read does. Thus it reads the file twice. A CSV scan gives the columns in the order of the file, as the eager read and `usecols` on pandas do.
- polars reads S3 with a `credential_provider` that gives the frozen credentials of the boto3 session of dynamicio, with their expiry. polars calls it again
  after the expiry. The region comes from the boto3 client. Each boto3 endpoint other than the public endpoint of a region (`s3.amazonaws.com` and
  `s3.<region>.amazonaws.com`) goes to polars. Examples are floci, a VPC interface endpoint, a FIPS endpoint and a dual-stack endpoint.
- A scan of one S3 object first does a ranged `GetObject` of 1 byte. A missing key then raises the dynamicio error at the scan, and `missing_ok` applies.
  A missing bucket raises the botocore `ClientError`. An `s3_file` key that ends in `/` raises `NotImplementedError`.
- A prefix lists its keys with boto3, with the filter of the pandas engine: no directory keys, and the pattern of `dynamic_file_path`. A prefix without
  objects raises `NoObjectsFoundError`. The engine reads the parquet footer of each object, and the scan then reads the footers again. The type of a column is the
  widest type of the footers, as `pd.concat` gives it, and the scan upcasts the narrower objects. A pair that does not upcast, for example `String` and
  `Int64`, or `Int64` and `UInt64`, raises `ColumnsDataTypeError`. An object that lacks a column gets nulls, with the warning of the pandas engine.
- `read()` of a prefix is `scan().collect()`, so the two reads give the same frame. A CSV or JSON prefix reads each object with the eager reader, as the
  pandas engine does, and the engine calls `lazy()` on the concatenated frame.
- A prefix write raised `NotImplementedError` in phase 4. The options `no_disk_space`, `partition_cols`, `mode` and `allow_delete` are accepted on a
  prefix, and refused on the other resources.

Phase 5 as built:

- `sink()` writes a `LazyFrame` to `local`, `s3_file` and `s3_path_prefix`. One `pl.collect_all` runs the sink, the validations and the metrics of the
  model. The sink goes to a temporary file, so a failed validation leaves the old file or object. The engine logs the metrics after the validations pass.
- `sink()` is the one exception to the order of decision 12. Its metric queries run in the same pass as the validations, so they also run on rows that
  fail a validation. A second pass after the validations would read a lazy source two times. The results keep the order of decision 12:
  - A failed validation raises `SchemaValidationError`. The engine does not log the metrics, and the destination does not change.
  - A metric query that fails makes the `collect_all` fail. Its polars error then comes before the result of the validations, and the destination does not change.
- A parquet sink to one file streams the rows to the temporary file with lz4. pyarrow then copies its row groups to a second file with the arrow types,
  the pandas metadata and the options of `write()`. The same `collect_all` counts the rows and the categories of each `Categorical` column for that metadata.
  The file has the footer and the row groups of `write()`. Only one row group is in memory at a time.
- A local file goes into place with `os.replace`, from a temporary directory in the directory of the file. An S3 file goes up with the upload of change 13.
- A CSV or JSON sink and a prefix sink collect the frame and call the writer of `write()`. The bytes stay equal to 11.1.0, and the prefix writer of the
  pandas engine gives the same objects and deletes the same old objects for each `mode`. A prefix write thus no longer raises `NotImplementedError`.
- Change 13 applies to the parquet writes of the polars engine to S3. `S3Upload` uses the boto3 transfer manager with parts of 64 MiB, 4 in flight and
  the `bucket-owner-full-control` ACL. A failed part aborts the multipart upload. The S3 uploads of the pandas engine do not change.
- A prefix read on the two engines does not read the `col=value` directories back as a column. A read of a prefix with `partition_cols` thus raises
  the `ValueError` of a column that no object has, on the two engines.
- The pandas `write()` now casts before its validations and metrics (decision 12). A schema column that is not in the frame raises `ColumnsDataTypeError`
  on the two engines, on reads and on writes (change 15). The pandas engine of 11.x raised `KeyError`. The CHANGELOG states the two changes as breaking.
- The checks of the polars engine are in `PolarsChecks`, and the parquet metadata code is in `PolarsParquet`. `PolarsFiles` and `PolarsSinks` share them.

Phase 6 as built:

- `footer()` reads the arrow schema, the key-value metadata and the row count of a `local` or `s3_file` parquet file, without its rows. An S3 read
  takes the last 64 KiB with one ranged `GetObject`, and a second ranged `GetObject` only for a larger footer. The read is the same on the two engines.
  It does not fix the engine, and `missing_ok` gives None for a missing file.
- `sink()` takes a list of lazy frames as the parts of one frame. Each part gets the schema columns and the casts of the model, so the parts can have
  other types of a column. The pandas metadata comes from the model and has one `RangeIndex` for all the rows.
- A polars Postgres read runs the statement as `pd.read_sql` runs it. A `str` query goes to the driver with `params` in the style of the driver, so a
  `sql_query` and its `params` mean the same on the two engines. The rows become a polars frame, and the cast of the model gives the types.
  Some `pd.read_sql` options change the pandas frame, for example `parse_dates` and `dtype`. On polars, they raise `EngineResourceError` when the user builds the resource.
- A polars Postgres write sends chunks of 100,000 rows to the COPY of the pandas engine. The DELETE or TRUNCATE, all the chunks and the commit are in one
  transaction, so a failed chunk keeps the old rows. Each chunk changes to pandas with the dtypes of the model first. The NULL rule and the text of
  each value are thus those of the pandas engine. The Postgres type map of the two engines also accepts `datetime64[us]` and `datetime64[ms]` columns.
- `key_column` (decision 4) is an option of the two engines. The pandas engine without it keeps the keys of the index. A polars Kafka resource without it, or
  with a name that is not a column of the model, raises `EngineResourceError` when the user builds the resource.
- A polars Kafka write sends chunks of 100,000 rows to the producer of the pandas engine, and the producer flushes once, after the last chunk. Each
  chunk changes to pandas with the dtypes of the model first, so the user callables get the types of decision 7. A test sends a `Datetime("ns")` and a
  `Duration("ns")` value through a custom serializer on the two engines and compares the bytes. The batches, `poll()` and `flush_timeout` are those of 11.1.0.
- A polars Athena read runs the query through awswrangler with the options of the pandas engine, and `pl.from_pandas` gives the polars frame
  (decision 8). The tests use a mock of awswrangler.
- A polars Kafka read and a polars Athena write raise the `NotImplementedError` of the pandas engine.

Phase 7 as built:

- polars can deadlock in a fork of a process that imported polars. A fork of such a process thus refuses the polars engine, also when the parent
  has the pandas engine or no engine yet. Its first polars read or write, and `dynamicio.configure(engine="polars")`, raise `EngineConfigurationError`.
  A fork of a process that did not import polars does not change.
- dynamicio does not set `POLARS_MAX_THREADS`. polars reads it at its first import, and `import dynamicio` and `configure()` do not import polars.
- `configure(engine="polars")` raises `ImportError` if polars is missing or older than 2.1, the minimum of the `polars` extra. The check reads the
  version of the installed package, so it does not import polars. `dynamicio migrate --engine polars` runs the same check.
- `FrameComparison` of `dynamicio.testharness` sends two polars frames to `PolarsFrameComparison`, with the same rules. A polars frame has no index, so
  `check_row_order=False` without `sort_by` compares the rows as a multiset. The pytest option `--dynamicio-engine` calls `configure()` before the first test.
- The tests of dynamicio set the engine for each test, and a fixture resets it after each test. The CI test job thus runs the tests of the two
  engines in one run. A CI job runs the polars demo with `--dynamicio-engine=polars`.
- An import test blocks the polars import in a new process. It checks the modules of dynamicio, the pandas engine and the CLI without polars. The
  dependency-ranges CI jobs install dynamicio without the `polars` extra and check that polars is not installed.
- `dynamicio migrate --engine polars` writes a polars model for each pandas model and a `POLARS_MIGRATION.md` report. The report lists the checks to port,
  the refused resources and the CSV definitions without `index`. The command does not change the resource definitions, because PyYAML drops their comments.
- `demo/polars_pipeline` is the pandas demo on the polars engine, with models from the migrate command. Its outputs are equal to the expected outputs
  of the pandas demo, so the demo tests rule 2.
- The review of phases 4 to 7 changes these items:
  - `scan()` and `sink()` check the operation before the freeze. A pandas process that calls them thus keeps no engine, and `configure(engine="polars")` still works.
  - A parquet prefix with an empty object and a type conflict leaves the empty object out of the types and the scan, as `pd.concat` does. If all objects are
    empty, the conflict raises `ColumnsDataTypeError`.
  - `Duration` columns of a prefix give the finest time unit. The objects with the same units are one scan, and a cast gives the finest unit.
  - The CSV options `escapechar` and `doublequote` raise `EngineResourceError` on polars, and `quoting=csv.QUOTE_NONE` reads the quotes as text.
  - The parts of a sink take the column order of the first part. The footer read repeats a ranged `GetObject` that breaks while it reads the body.

Change 26 is the floci fixture of decision 15. RND-25426 delivers it before phase 1, with the pandas integration tests. The polars S3 tests of phase 3 are its first polars users.

Change 5 replaces the neutral dtype vocabulary of the first proposal. That vocabulary was for YAML schemas, which dynamicio 11.0 removed. A pandas repo declares
`pandera.pandas` models and a polars repo declares `pandera.polars` models. The polars writer uses the dtype map to build the pandas metadata (change 17). The tests
of the 4 pairs use it to compare frames.

## Consequences

- A repo that does not call `configure()` gets the pandas engine. The pandas engine of 12.0 has 2 changes in behavior:
  - A write casts before it validates and computes metrics (decision 12).
  - A schema column that is not in the frame raises `ColumnsDataTypeError` on reads and on writes (change 15). The pandas engine of 11.x raised `KeyError`,
    so a caller that catches `KeyError` must catch `ColumnsDataTypeError`. A CSV or parquet read still raises the error of its reader first.
- A repo cannot use pandas for one resource and polars for another. A repo that needs both engines must split into two processes that share files.
- The migrate command writes the polars models. Its report lists the resource definitions and the options to change, and the user changes them.
  It does not change the transformation code of the repo. A repo must change its own pandas code to polars before it moves.
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
