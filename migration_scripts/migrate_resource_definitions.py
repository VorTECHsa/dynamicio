from dataclasses import dataclass

import yaml


@dataclass
class KeyedResourceTemplate:
    resources: list
    resource_name: str
    template: str = """
{resource_name} = KeyedResource(
    {{
        {resources}
    }}
)
"""

    def render_template(self) -> str:
        return self.template.format(resource_name=self.resource_name, resources="\n".join([resource.render_own_resource() for resource in self.resources]))


@dataclass
class S3ParquetFileType:
    environment: str
    file_path: str
    bucket: str
    class_name: str = "S3ParquetResource"

    @classmethod
    def from_dict(cls, candidate_dict: dict[str, str], environment: str) -> "S3ParquetFileType":
        return cls(
            environment=environment,
            file_path=candidate_dict["s3"]["file_path"],
            bucket=candidate_dict["s3"]["bucket"],
        )

    @staticmethod
    def is_dict_parseable(candidate_dict: dict[str, ...]):
        return candidate_dict["type"] == "s3_file" and candidate_dict["s3"]["file_type"] == "parquet"

    def render_own_resource(self) -> str:
        return f'"{self.environment}": {self.class_name}(path="{self.file_path}", bucket="{self.bucket}"),'


@dataclass
class LocalParquetFileType:
    environment: str
    file_path: str
    class_name: str = "ParquetResource"

    @classmethod
    def from_dict(cls, candidate_dict: dict[str, str], environment: str) -> "LocalParquetFileType":
        return cls(
            environment=environment,
            file_path=candidate_dict["local"]["file_path"],
        )

    @staticmethod
    def is_dict_parseable(candidate_dict: dict[str, ...]):
        return candidate_dict["type"] == "local" and candidate_dict["local"]["file_type"] == "parquet"

    def render_own_resource(self) -> str:
        return f'"{self.environment}": {self.class_name}(path="{self.file_path}"),'


@dataclass
class Kafka:
    environment: str
    topic: str
    server: str
    class_name: str = "KafkaResource"

    @classmethod
    def from_dict(cls, candidate_dict: dict[str, dict[str, str]], environment: str) -> "Kafka":
        return cls(
            environment=environment,
            topic=candidate_dict["kafka"]["kafka_topic"],
            server=candidate_dict["kafka"]["kafka_server"],
        )

    @staticmethod
    def is_dict_parseable(candidate_dict: dict[str, ...]):
        return candidate_dict["type"] == "kafka"

    def render_own_resource(self) -> str:
        return f'"{self.environment}": {self.class_name}(topic="{self.topic}", server="{self.server}"),'


@dataclass
class Postgres:
    environment: str
    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str
    class_name: str = "PostgresResource"

    @classmethod
    def from_dict(cls, candidate_dict: dict[str, dict[str, str]], environment: str) -> "Postgres":
        return cls(
            environment=environment,
            db_host=candidate_dict["postgres"]["db_host"],
            db_port=candidate_dict["postgres"]["db_port"],
            db_name=candidate_dict["postgres"]["db_name"],
            db_user=candidate_dict["postgres"]["db_user"],
            db_password=candidate_dict["postgres"]["db_password"],
        )

    @staticmethod
    def is_dict_parseable(candidate_dict: dict[str, ...]):
        return candidate_dict["type"] == "postgres"

    def render_own_resource(self) -> str:
        return (
            f'"{self.environment}": {self.class_name}( \n'
            f'\t\t\tdb_user="{self.db_user}", \n'
            f'\t\t\tdb_password="{self.db_password}", \n'
            f'\t\t\tdb_host="{self.db_host}", \n'
            f'\t\t\tdb_port="{self.db_port}", \n'
            f'\t\t\tdb_name="{self.db_name}", \n'
            f'\t\t\tdb_schema="public", \n'
            f"\t\t\ttable_name=None, \n"
            f"\t\t\tsql_query=..., \n"
            f"\t\t\ttruncate_and_append=False, \n"
            f"\t\t\tkwargs={{}}, \n"
            f"\t\t\tpa_schema=None, \n"
            f"\t\t),"
        )


def parse_resource_configs(parsed_yaml_entry: dict[str, str]) -> list:
    resource_configs = []

    for key, val in parsed_yaml_entry.items():
        if key == "schema":
            continue

        for resource_type in resource_types:
            if resource_type.is_dict_parseable(val):
                resource_configs.append(resource_type.from_dict(val, key.lower()))

    return resource_configs


# Execute script
resource_types = [S3ParquetFileType, LocalParquetFileType, Kafka, Postgres]

# Point to resource definitions
parsed_yaml = yaml.safe_load("./test.yaml")

# Write this output to file
keyed_templates = []

for resource_key, resource_value in parsed_yaml.items():
    resource_name = f"{resource_key.lower()}_resource"

    keyed_template = KeyedResourceTemplate(
        resources=parse_resource_configs(parsed_yaml[resource_key]),
        resource_name=f"{resource_name}",
    )

    keyed_templates.append(keyed_template)
