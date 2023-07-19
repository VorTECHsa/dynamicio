from datetime import datetime

import pandera as pa
from pandera import SchemaModel
from pandera.typing import Series


class CargoMovementsColoadsMappingSchema(SchemaModel):
    id: Series[str] = pa.Field(nullable=True)
    tonnes: Series[float] = pa.Field(nullable=True)
    ratio: Series[float] = pa.Field(nullable=True)
    col_1: Series[int] = pa.Field(isin=["class_a","class_b","class_c"],nullable=True)
    col_2: Series[int] = pa.Field(gt=1000,nullable=True)
    col_3: Series[int] = pa.Field(in_range={"min_value":0, "max_value":1000, "include_min":False, "include_max":True},nullable=True)
    col_4: Series[int] = pa.Field(unique=True,nullable=False)
    col_5: Series[datetime] = pa.Field(nullable=True)
    col_6: Series[int] = pa.Field(nullable=True)
    col_7: Series[float] = pa.Field(ge=0,nullable=True)
    col_8: Series[float] = pa.Field(le=0,nullable=True)
    col_9: Series[float] = pa.Field(gt=0,nullable=True)
    col_10: Series[float] = pa.Field(lt=0,nullable=True)
    col_11: Series[float] = pa.Field(gt=0,nullable=True)

    class Config:
        coerce = True
        strict = "filter"
    