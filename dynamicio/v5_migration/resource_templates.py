# pylint: skip-file
# noqa
# type: ignore

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

# @dataclass
# class KeyedResourceTemplate:
#     resources: list
#     resource_name: str
#     template: str = """
# {resource_name} = KeyedResource(
#     {{
#         {resources}
#     }}
# )
# """
#
#     def render_template(self) -> str:
#         return self.template.format(
#             resource_name=self.resource_name,
#             resources="\n".join([resource.render_own_resource() for resource in self.resources]),
#         )


class ReadyTemplate(ABC):
    @abstractmethod
    def render_template(self) -> str:
        raise NotImplementedError


s3_file_type_class_map = {
    "parquet": "S3ParquetResource",
    "csv": "S3CsvResource",
    "json": "S3JsonResource",
    "hdf": "S3HdfResource",
}


@dataclass
class S3Template(ReadyTemplate):
    resource_name: str
    bucket: str
    file_path: str
    test_path: Optional[str]
    class_name: str
    template: str = """
{resource_name} = {class_name}(
    bucket="{bucket}",
    path="{file_path}"{test_path_str}
)
"""

    @classmethod
    def from_dict(cls, resource_dict: dict[str, ...], resource_name: str) -> "S3Template":
        file_type = resource_dict["cloud"]["s3"]["file_type"]
        test_path = resource_dict.get("local", {}).get("local", {}).get("file_path", None)
        # Warning: if local filetype does not match cloud filetype. This will not work.
        return cls(
            resource_name=resource_name,
            bucket=resource_dict["cloud"]["s3"]["bucket"],
            file_path=resource_dict["cloud"]["s3"]["file_path"],
            class_name=s3_file_type_class_map[file_type],
            test_path=test_path,
        )

    @staticmethod
    def is_dict_parseable(resource_dict: dict[str, ...]):
        return resource_dict["cloud"]["type"] == "s3_file" and resource_dict["cloud"]["s3"]["file_type"] in list(
            s3_file_type_class_map.keys()
        )

    def render_template(self) -> str:
        test_path_str = f',\n    test_path="{self.test_path}"' if self.test_path else ""
        return self.template.format(
            resource_name=self.resource_name,
            class_name=self.class_name,
            bucket=self.bucket,
            file_path=self.file_path,
            test_path_str=test_path_str,
        )


file_types = ["parquet", "csv", "json", "hdf"]


def replace_double_brackets(string: Optional[str]) -> Optional[str]:
    if not string:
        return None
    return string.replace("[[", "{").replace("]]", "}")


@dataclass
class LocalTemplate(ReadyTemplate):
    resource_name: str
    file_path: str
    file_type: Optional[str]
    test_path: Optional[str]
    template: str = """
{resource_name} = FileResource(
    path="{file_path}"{test_path_str}{file_type_str}
)
"""

    @classmethod
    def from_dict(cls, resource_dict: dict[str, ...], resource_name: str) -> "LocalTemplate":
        test_path = resource_dict.get("local", {}).get("local", {}).get("file_path", None)
        file_path = resource_dict["cloud"]["local"]["file_path"]
        file_type = resource_dict["cloud"]["local"]["file_type"]

        test_path = replace_double_brackets(test_path)
        file_path = replace_double_brackets(file_path)
        # these can be inferred from the file_path
        if any([file_path.endswith("." + ext) for ext in file_types]):
            file_type = None
        return cls(
            resource_name=resource_name,
            file_path=file_path,
            file_type=file_type,
            test_path=test_path,
        )

    @staticmethod
    def is_dict_parseable(resource_dict: dict[str, ...]) -> bool:
        file_type = resource_dict["cloud"]["local"]["file_type"]
        return resource_dict["cloud"]["type"] == "local" and file_type in file_types

    def render_template(self) -> str:
        test_path_str = f',\n    test_path="{self.test_path}"' if self.test_path else ""
        file_type_str = f',\n    file_type="{self.file_type}"' if self.file_type else ""
        return self.template.format(
            resource_name=self.resource_name,
            file_type_str=file_type_str,
            file_path=self.file_path,
            test_path_str=test_path_str,
        )


@dataclass
class KafkaTemplate(ReadyTemplate):
    resource_name: str
    topic: str
    server: str
    test_path: Optional[str]
    template: str = """
{resource_name} = KafkaResource(
    server="{server}",
    topic="{topic}"{test_path_str}
)
"""

    @classmethod
    def from_dict(cls, resource_dict: dict[str, ...], resource_name: str) -> "KafkaTemplate":
        test_path = resource_dict.get("local", {}).get("local", {}).get("file_path", None)
        return cls(
            resource_name=resource_name,
            topic=resource_dict["cloud"]["kafka"]["kafka_topic"],
            server=resource_dict["cloud"]["kafka"]["kafka_server"],
            test_path=test_path,
        )

    @staticmethod
    def is_dict_parseable(resource_dict: dict[str, ...]):
        return resource_dict["cloud"]["type"] == "kafka"

    def render_template(self) -> str:
        test_path_str = f',\n    test_path="{self.test_path}"' if self.test_path else ""
        return self.template.format(
            resource_name=self.resource_name,
            server=self.server,
            topic=self.topic,
            test_path_str=test_path_str,
        )


@dataclass
class PostgresTemplate(ReadyTemplate):
    resource_name: str
    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str
    class_name: str = "PostgresResource"
    test_path: Optional[str] = None
    template: str = """
{resource_name} = {class_name}(
    db_host="{db_host}",
    db_port="{db_port}",
    db_name="{db_name}",
    db_user="{db_user}",
    db_password="{db_password}",
    table_name=None,
    sql_query=...{test_path_str}
)
"""

    @classmethod
    def from_dict(cls, resource_dict: dict[str, dict[str, str]], resource_name: str) -> "PostgresTemplate":
        test_path = resource_dict.get("local", {}).get("local", {}).get("file_path", None)
        return cls(
            resource_name=resource_name,
            db_host=resource_dict["cloud"]["postgres"]["db_host"],
            db_port=resource_dict["cloud"]["postgres"]["db_port"],
            db_name=resource_dict["cloud"]["postgres"]["db_name"],
            db_user=resource_dict["cloud"]["postgres"]["db_user"],
            db_password=resource_dict["cloud"]["postgres"]["db_password"],
            test_path=test_path,
        )

    @staticmethod
    def is_dict_parseable(resource_dict: dict[str, ...]):
        return resource_dict["cloud"]["type"] == "postgres"

    def render_template(self) -> str:
        test_path_str = f',\n    test_path="{self.test_path}"' if self.test_path else ""
        return self.template.format(
            resource_name=self.resource_name,
            class_name=self.class_name,
            db_host=self.db_host,
            db_port=self.db_port,
            db_name=self.db_name,
            db_user=self.db_user,
            db_password=self.db_password,
            test_path_str=test_path_str,
        )
