# flake8: noqa: T201
"""Demo for PostgresResource.

This demo shows how to read and write data from a postgres database. 
It will read/write from your local postgres database and requires you to run the sql provided first.

SQL Statement to create the table & Seed some data:

CREATE TABLE public.new_table (
	test_col int8,
	column1 text
);
INSERT INTO public.new_table (test_col, column1) VALUES (1, 'test1');

SQL Statement to delete the table
DROP TABLE public.new_table;
"""

from pandera import SchemaModel
from pandera.typing import Series

from dynamicio import PostgresConfig, PostgresResource

config = PostgresConfig(
    db_user="",
    db_host="localhost",
    db_port=5432,
    db_name="",
    table_name="new_table",
    truncate_and_append=True,
)

df = PostgresResource(config).read()
print(df)

df["test_col"] = 123


PostgresResource(config).write(df)


class PGSchema(SchemaModel):
    test_col: Series[str]

    class Config:
        strict = "filter"


df2 = PostgresResource(config, pa_schema=PGSchema).read()

print(df2)

# Output (first time):
#    test_col column1
# 0         1   test1
#    test_col
# 0       123
