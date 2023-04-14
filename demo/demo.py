# flake8: noqa: T201

from pathlib import Path

from pandera import SchemaModel
from pandera.typing import Series

from dynamicio import ParquetConfig, ParquetHandler

DEMO_DIR = Path(__file__).parent

config = ParquetConfig(path=DEMO_DIR / "data/[[directory]]/{filename}.parquet")
config = config.inject(directory="input", filename="bar")
handler_without_schema = ParquetHandler(config)
df_without_schema = handler_without_schema.read()

print(df_without_schema)


class OneFilteredColumnSchema(SchemaModel):
    column_a: Series[str]
    column_b: Series[str]
    column_c: Series[int]

    class Config:
        coerce = True  # this will coerce column_c to int
        strict = "filter"  # this will filter out column_d from the raw data


handler_with_schema = ParquetHandler(config, pa_schema=OneFilteredColumnSchema)
df_with_schema = handler_with_schema.read()

print(df_with_schema)

# Output:
#   column_a column_b  column_c  column_d
# 0      id1  Label_A    1001.0     999.0
# 1      id2  Label_A    1002.0     998.0
# 2      id3  Label_B    1003.0     997.0
# 3      id4  Label_C    1004.0     996.0
# 4      id5  Label_A    1005.0     995.0
# 5      id6  Label_B    1006.0     994.0
# 6      id7  Label_C    1007.0     993.0
# 7      id8  Label_A    1008.0     992.0
# 8      id9  Label_A    1009.0     991.0
# 9     id10  Label_B    1010.0     990.0
#   column_a column_b  column_c
# 0      id1  Label_A      1001
# 1      id2  Label_A      1002
# 2      id3  Label_B      1003
# 3      id4  Label_C      1004
# 4      id5  Label_A      1005
# 5      id6  Label_B      1006
# 6      id7  Label_C      1007
# 7      id8  Label_A      1008
# 8      id9  Label_A      1009
# 9     id10  Label_B      1010
