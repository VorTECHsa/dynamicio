# API reference

Generated from the docstrings of the `dynamicio` package by [`mkdocstrings`](https://mkdocstrings.github.io/). The
reference documents the public classes and functions; names beginning with an underscore are left out.

## Core

::: dynamicio
    options:
      members:
        - UnifiedIO

::: dynamicio.core

## Resource definitions

::: dynamicio.config.io_config

::: dynamicio.parquet_footer

## Backends

### S3

::: dynamicio.mixins.with_s3

### Postgres

::: dynamicio.mixins.with_postgres

### Athena

::: dynamicio.mixins.with_athena

### Kafka

::: dynamicio.mixins.with_kafka

### Local filesystem

::: dynamicio.mixins.with_local

### Utilities

::: dynamicio.mixins.utils

## Engines

::: dynamicio.engines.engine

::: dynamicio.engines.engine_setting

::: dynamicio.engines.registry

::: dynamicio.engines.pandas_engine

::: dynamicio.engines.polars_engine

::: dynamicio.engines.polars_sinks

::: dynamicio.engines.polars_postgres

::: dynamicio.engines.polars_kafka

::: dynamicio.engines.polars_athena

::: dynamicio.engines.s3_upload

## Schemas

::: dynamicio.schema.data_model

::: dynamicio.schema.field

::: dynamicio.schema.metadata

::: dynamicio.schema.models

::: dynamicio.schema.validations

::: dynamicio.schema.metrics

## Polars schemas

::: dynamicio.schema.polars

::: dynamicio.schema.polars_dtypes

::: dynamicio.schema.polars_validations

::: dynamicio.schema.polars_metrics

## Errors

::: dynamicio.errors

## Command line

::: dynamicio.cli

## Test harness

::: dynamicio.testharness

::: dynamicio.testharness.polars_frame_comparison

::: dynamicio.testharness.pytest_plugin
