# pylint: disable=missing-class-docstring, missing-function-docstring, redefined-outer-name
"""One IO object used for more than one read or write.

Every sequence of calls on one object has to behave like the same calls on new objects, and the object's options
have to stay as they were. Local files run on a temp dir. S3 runs on moto's in-memory S3 through the real read and write code.
"""

import io
import itertools
import os
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
import yaml

import dynamicio.mixins.with_s3
from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from tests import constants

BUCKET = "reuse-bucket"
REGION = "us-east-1"

SEQUENCES = [pytest.param(sequence, id="-".join(sequence)) for sequence in itertools.product(("read", "write"), repeat=2)]


class FileIO(UnifiedIO):
    schema = {"id": "int64", "name": "object"}


class PartitionedIO(UnifiedIO):
    schema = {"grp": "object", "val": "int64"}


FRAME = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
PARTITIONED_FRAME = pd.DataFrame({"grp": ["a", "a", "b"], "val": [1, 2, 3]})


@pytest.fixture
def config_for(tmp_path):
    local_dir = tmp_path / "local"
    resources = {
        "LOCAL_FILE": {"LOCAL": {"type": "local", "local": {"file_path": f"{local_dir}/{{name}}.parquet", "file_type": "parquet"}}},
        "LOCAL_BATCH": {"LOCAL": {"type": "local_batch", "local": {"path_prefix": f"{local_dir}/{{name}}/", "file_type": "parquet"}}},
        "LOCAL_BATCH_DYNAMIC": {"LOCAL": {"type": "local_batch", "local": {"dynamic_file_path": f"{local_dir}/{{name}}/**/part_{{part}}.parquet", "file_type": "parquet"}}},
        "S3_FILE": {"CLOUD": {"type": "s3_file", "s3": {"bucket": BUCKET, "file_path": "data/{name}.parquet", "file_type": "parquet"}}},
        "S3_PREFIX": {"CLOUD": {"type": "s3_path_prefix", "s3": {"bucket": BUCKET, "path_prefix": "data/{name}/", "file_type": "parquet"}}},
        "S3_PREFIX_DYNAMIC": {"CLOUD": {"type": "s3_path_prefix", "s3": {"bucket": BUCKET, "dynamic_file_path": "data/{name}/**/part_{part}.parquet", "file_type": "parquet"}}},
    }
    path = tmp_path / "resources.yaml"
    path.write_text(yaml.safe_dump(resources))

    def get(source_key, env):
        return IOConfig(path_to_source_yaml=str(path), env_identifier=env, dynamic_vars=constants).get(source_key=source_key)

    local_dir.mkdir()
    return get


@pytest.fixture
def s3_client(monkeypatch):
    moto = pytest.importorskip("moto")  # dev dependency only; the dependency-ranges CI jobs do not install it
    for name, value in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing", "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(name, value)
    with moto.mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket=BUCKET)
        with patch.object(dynamicio.mixins.with_s3, "_shared_s3_client", return_value=client):
            yield client


def _keys(client, prefix=""):
    return sorted(obj["Key"] for obj in client.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []))


def _run(io_object, sequence, frame):
    """Run the calls on one object and return the frame of every read."""
    reads = []
    for step in sequence:
        if step == "read":
            reads.append(io_object.read())
        else:
            io_object.write(frame)
    return reads


@pytest.mark.unit
@pytest.mark.parametrize("sequence", SEQUENCES)
class TestOneObjectForSeveralCalls:
    def test_local_file(self, config_for, sequence, tmp_path):
        # Given
        config = config_for("LOCAL_FILE", "LOCAL")
        FileIO(source_config=config, name="seed").write(FRAME)
        io_object = FileIO(source_config=config, name="seed")
        options_before = dict(io_object.options)

        # When
        reads = _run(io_object, sequence, FRAME)

        # Then
        for df in reads:
            pd.testing.assert_frame_equal(df, FRAME)
        pd.testing.assert_frame_equal(FileIO(source_config=config, name="seed").read(), FRAME)
        assert os.listdir(tmp_path / "local") == ["seed.parquet"]
        assert io_object.options == options_before

    def test_s3_file(self, config_for, s3_client, sequence):
        # Given
        config = config_for("S3_FILE", "CLOUD")
        FileIO(source_config=config, name="seed").write(FRAME)
        io_object = FileIO(source_config=config, name="seed")
        options_before = dict(io_object.options)

        # When
        reads = _run(io_object, sequence, FRAME)

        # Then
        for df in reads:
            pd.testing.assert_frame_equal(df, FRAME)
        assert _keys(s3_client) == ["data/seed.parquet"]
        assert io_object.options == options_before

    def test_s3_path_prefix(self, config_for, s3_client, sequence):
        # Given
        config = config_for("S3_PREFIX", "CLOUD")
        PartitionedIO(source_config=config, name="seed", partition_cols="grp").write(PARTITIONED_FRAME)
        # A partition column is not stored in its objects, and a read fails if a schema column is in none of them, so one plain object holds it
        buffer = io.BytesIO()
        PARTITIONED_FRAME.to_parquet(buffer)
        s3_client.put_object(Bucket=BUCKET, Key="data/seed/plain.parquet", Body=buffer.getvalue())
        io_object = PartitionedIO(source_config=config, name="seed", partition_cols="grp", no_disk_space=True)
        options_before = dict(io_object.options)

        # When
        reads = _run(io_object, sequence, PARTITIONED_FRAME)

        # Then each write adds new objects under the prefix (their names are unique), and a read returns all of them
        writes_before_each_read = [sequence[:index].count("write") for index, step in enumerate(sequence) if step == "read"]
        for df, writes in zip(reads, writes_before_each_read):
            assert sorted(df["val"]) == sorted([1, 2, 3] * (2 + writes))
        keys = _keys(s3_client)
        assert len(keys) == 1 + 2 * (1 + sequence.count("write")) and sum(key.startswith("data/seed/grp=") for key in keys) == len(keys) - 1
        assert io_object.options == options_before


@pytest.mark.unit
class TestOneObjectForRepeatedReads:
    """Local batch reads and reads with a dynamic file path have no writer, so only the read sequence applies."""

    def test_local_batch(self, config_for, tmp_path):
        # Given
        config = config_for("LOCAL_BATCH", "LOCAL")
        batch_dir = tmp_path / "local" / "seed"
        batch_dir.mkdir()
        FRAME.to_parquet(batch_dir / "one.parquet")
        io_object = FileIO(source_config=config, name="seed")
        options_before = dict(io_object.options)

        # When
        first, second = io_object.read(), io_object.read()

        # Then
        pd.testing.assert_frame_equal(first, FRAME)
        pd.testing.assert_frame_equal(second, FRAME)
        assert io_object.options == options_before

    def test_local_batch_with_a_dynamic_file_path(self, config_for, tmp_path):
        # Given
        config = config_for("LOCAL_BATCH_DYNAMIC", "LOCAL")
        part_dir = tmp_path / "local" / "seed" / "x"
        part_dir.mkdir(parents=True)
        FRAME.to_parquet(part_dir / "part_01.parquet")
        io_object = FileIO(source_config=config, name="seed", part="01")
        options_before = dict(io_object.options)

        # When
        first, second = io_object.read(), io_object.read()

        # Then
        pd.testing.assert_frame_equal(first, FRAME)
        pd.testing.assert_frame_equal(second, FRAME)
        assert io_object.options == options_before

    def test_s3_path_prefix_with_a_dynamic_file_path(self, config_for, s3_client):
        # Given
        config = config_for("S3_PREFIX_DYNAMIC", "CLOUD")
        FileIO(source_config=config_for("S3_FILE", "CLOUD"), name="seed").write(FRAME)
        s3_client.copy_object(Bucket=BUCKET, Key="data/seed/x/part_01.parquet", CopySource={"Bucket": BUCKET, "Key": "data/seed.parquet"})
        io_object = FileIO(source_config=config, name="seed", part="01", no_disk_space=True)
        options_before = dict(io_object.options)

        # When
        first, second = io_object.read(), io_object.read()

        # Then
        pd.testing.assert_frame_equal(first, FRAME)
        pd.testing.assert_frame_equal(second, FRAME)
        assert io_object.options == options_before


@pytest.mark.unit
class TestTemplateFieldNamedColumns:
    """`columns` is also the option that selects columns to read, so a template field of that name must not be taken for a selection."""

    def test_local_file(self, tmp_path):
        # Given
        resources = {"LOCAL_FILE": {"LOCAL": {"type": "local", "local": {"file_path": f"{tmp_path}/{{columns}}.parquet", "file_type": "parquet"}}}}
        path = tmp_path / "resources.yaml"
        path.write_text(yaml.safe_dump(resources))
        config = IOConfig(path_to_source_yaml=str(path), env_identifier="LOCAL", dynamic_vars=constants).get(source_key="LOCAL_FILE")
        io_object = FileIO(source_config=config, columns="daily")
        options_before = dict(io_object.options)

        # When
        io_object.write(FRAME)
        first, second = io_object.read(), io_object.read()

        # Then
        pd.testing.assert_frame_equal(first, FRAME)
        pd.testing.assert_frame_equal(second, FRAME)
        assert io_object.options == options_before

    def test_s3_file(self, config_for, s3_client, tmp_path):
        # Given
        resources = {"S3_FILE": {"CLOUD": {"type": "s3_file", "s3": {"bucket": BUCKET, "file_path": "data/{columns}.parquet", "file_type": "parquet"}}}}
        path = tmp_path / "columns_resources.yaml"
        path.write_text(yaml.safe_dump(resources))
        config = IOConfig(path_to_source_yaml=str(path), env_identifier="CLOUD", dynamic_vars=constants).get(source_key="S3_FILE")
        io_object = FileIO(source_config=config, columns="daily")

        # When
        io_object.write(FRAME)
        first = io_object.read()

        # Then
        pd.testing.assert_frame_equal(first, FRAME)
        assert _keys(s3_client) == ["data/daily.parquet"]
