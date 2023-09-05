from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Type

import boto3
import pandas as pd

from dynamicio.io.s3_contexts import s3_named_file_reader, s3_writer, s3_reader
from dynamicio.io.resource import BaseResource
from dynamicio.io.serde import BaseSerde, CsvSerde, HdfSerde, JsonSerde, ParquetSerde, PickleSerde


class S3Resource(BaseResource):
    bucket: str
    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    injectables: List[str] = ["path"]
    file_type: Optional[Literal["parquet", "hdf", "csv", "json", "pickle"]] = None
    force_read_to_memory: bool = False

    @property
    def _s3_path(self) -> str:
        """For logging purposes only."""
        return f"s3://{self.bucket}/{self.path}"

    def _read(self) -> pd.DataFrame:
        if self.force_read_to_memory:
            with s3_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as fobj:  # type: ignore
                df = self.get_serde()._read(fobj, **self.read_kwargs)  # type: ignore
                if df is not None:
                    return df

        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as target_file:
            return self.get_serde()._read(target_file.name, **self.read_kwargs)  # type: ignore

    def _write(self, df: pd.DataFrame) -> None:
        with s3_writer(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as fobj:
            return self.get_serde()._write(fobj, df)

    @property
    def serde_class(self) -> Type[BaseSerde]:
        file_type = self.file_type or (self.path.suffix[1:] if self.path.suffix else None)

        if file_type == "parquet":
            serde_class = partial(ParquetSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "hdf" or file_type == "h5":
            serde_class = partial(HdfSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "csv":
            serde_class = partial(CsvSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "json":
            serde_class = partial(JsonSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "pickle":
            serde_class = partial(PickleSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type is None:
            raise ValueError(f"File type not specified for {self.path}")
        else:
            raise ValueError(f"Unknown file type {file_type}")

        return serde_class

    # TODO: hdf serde .... lock.
    def cache_key(self) -> Path:
        if self.test_path is not None:
            return self.test_path
        else:
            return Path("s3") / self.bucket / self.path


if __name__ == "__main__":
    # df = S3Resource(path=Path("tests/fixtures/sample.parquet"), file_type="parquet").read()
    # print(df)
    # foo = io.BytesIO()
    # boto3.client("s3").download_fileobj(
    #     "vortexa-develop-pmmt-freight-pricing",
    #     str("reference_data/routes/00b1edbed41feeaccb638b3c6ed906eddec9f9cc0a954e42e7ad7f89ead926da.parquet"),
    #     foo,
    # )
    # foo.seek(0)
    # df = pd.read_parquet(foo)

    df = S3Resource(
        bucket="vortexa-develop-pmmt-freight-pricing",
        path=Path("reference_data/routes/00b1edbed41feeaccb638b3c6ed906eddec9f9cc0a954e42e7ad7f89ead926da.parquet"),
        file_type="parquet",
    ).read()
    print(df)
    S3Resource(bucket="vortexa-develop-pmmt-freight-pricing", path="test.parquet").write(df)
    print("yay")
