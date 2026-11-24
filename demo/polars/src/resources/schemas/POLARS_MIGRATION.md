# Polars migration

`dynamicio migrate --engine polars` wrote 7 polars models to `demo/polars/src/resources/schemas`. Each model has the dtypes, validations, metrics and names of its pandas model.

1. Replace the imports of the pandas models with the imports of the polars models.
2. Call `dynamicio.configure(engine="polars")` at the start of each process, before the first read or write.
3. Change the transformation code from the pandas API to the polars API.
4. Run the tests with `pytest --dynamicio-engine=polars`.

## Checks to port

These Pandera checks are pandas code. Write them again for polars.

None.

## Resource definitions that the polars engine refuses

Change these definitions, or keep these resources on the pandas engine. The options that the code gives to an IO object are not in this list.

- definitions/staging_input.yaml: FOO.DEV: the polars engine reads and writes parquet, csv, json files, not hdf files. Use the pandas engine for hdf.
- definitions/staging_input.yaml: FOO.PROD: the polars engine reads and writes parquet, csv, json files, not hdf files. Use the pandas engine for hdf.
- definitions/transform_output.yaml: FINAL_BAR.DEV: the polars engine needs the option key_column: a polars frame has no index to give the message keys.
- definitions/transform_output.yaml: FINAL_BAR.PROD: the polars engine needs the option key_column: a polars frame has no index to give the message keys.

## CSV definitions without an index option

The polars engine writes the leading index column with the row numbers 0 to n-1. Set `index: false` if the file has no index column.

- definitions/staging_input.yaml: FOO.LOCAL
