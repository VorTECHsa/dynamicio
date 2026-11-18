# pylint: disable=missing-module-docstring, missing-function-docstring
import os
import shutil
import subprocess
import time
import uuid
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Set

import boto3
import psycopg2
import pytest
from confluent_kafka.admin import AdminClient, NewTopic

# Application Imports
from dynamicio.mixins import with_s3
from tests.integration.resources import Resources


@dataclass(frozen=True)
class FlociStack:
    """The endpoints of one floci container: S3, an RDS Postgres instance and an MSK (Redpanda) cluster."""

    endpoint_url: str
    bucket: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    kafka_bootstrap: str
    kafka_container: str

    def s3_client(self):
        return boto3.client("s3", endpoint_url=self.endpoint_url)

    def pg_connection(self):
        return psycopg2.connect(host=self.db_host, port=self.db_port, user=self.db_user, password=self.db_password, dbname=self.db_name)


class Floci:
    """Starts and stops floci (https://github.com/floci-io/floci), a local AWS emulator, for the integration tests.

    floci starts RDS and MSK as their own containers through the Docker socket. The images are pinned, so a new
    release of floci, Postgres or Redpanda cannot change the results of the tests.
    """

    IMAGE = "floci/floci@sha256:a0fbf944d7f338cc0856f1ef83831ea749ff2be71956eb7a7fce08fc21458e19"  # floci 2.5.0, linux/amd64 and linux/arm64
    POSTGRES_IMAGE = "postgres:16.3-alpine"
    REDPANDA_IMAGE = "redpandadata/redpanda:v26.2.7"
    PORT = 4566
    RDS_PROXY_PORTS = "7001-7010"
    REGION = "eu-west-1"
    ENVIRONMENT = {
        "AWS_ENDPOINT_URL": f"http://localhost:{PORT}",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_DEFAULT_REGION": REGION,
        "AWS_REGION": REGION,
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
    }
    DB_USER, DB_PASSWORD, DB_NAME = "dynamicio", "dynamicio", "postgres"

    def __init__(self):
        self.name = f"dynamicio-floci-{uuid.uuid4().hex[:8]}"
        self.others: Set[str] = set()

    @staticmethod
    def floci_children() -> Set[str]:
        """Return the names of the RDS and MSK containers of all floci servers on this Docker host."""
        return set(subprocess.run(["docker", "ps", "-a", "--filter", "name=floci-aws-", "--format", "{{.Names}}"], capture_output=True, text=True, check=True).stdout.split())

    @staticmethod
    def docker_is_available() -> bool:
        if shutil.which("docker") is None:
            return False
        return subprocess.run(["docker", "info"], capture_output=True, check=False).returncode == 0

    @staticmethod
    def wait_for(condition: Callable[[], Any], what: str, timeout: float = 180) -> Any:
        deadline = time.monotonic() + timeout
        while True:
            try:
                result = condition()
                if result:
                    return result
            except Exception:  # pylint: disable=broad-except
                if time.monotonic() > deadline:
                    raise
            if time.monotonic() > deadline:
                raise TimeoutError(f"floci: {what} was not ready after {timeout}s")
            time.sleep(1)

    def start(self) -> FlociStack:
        self.others = self.floci_children()
        subprocess.run(
            [
                "docker", "run", "-d", "--name", self.name, "-u", "root",
                "-p", f"{self.PORT}:{self.PORT}", "-p", f"{self.RDS_PROXY_PORTS}:{self.RDS_PROXY_PORTS}",
                "-v", "/var/run/docker.sock:/var/run/docker.sock",
                "-e", "FLOCI_SERVICES_MSK_BOOTSTRAP_HOSTNAME=localhost",
                "-e", f"FLOCI_SERVICES_RDS_DEFAULT_POSTGRES_IMAGE={self.POSTGRES_IMAGE}",
                "-e", f"FLOCI_SERVICES_MSK_DEFAULT_IMAGE={self.REDPANDA_IMAGE}",
                self.IMAGE,
            ],
            check=True,
            capture_output=True,
        )  # fmt: skip
        endpoint_url = self.ENVIRONMENT["AWS_ENDPOINT_URL"]
        session = boto3.Session(aws_access_key_id="test", aws_secret_access_key="test", region_name=self.REGION)
        s3, rds, kafka = (session.client(service, endpoint_url=endpoint_url) for service in ("s3", "rds", "kafka"))
        self.wait_for(lambda: s3.list_buckets() is not None, "S3")

        bucket = f"dynamicio-it-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": self.REGION})
        rds.create_db_instance(
            DBInstanceIdentifier="dynamicio", DBInstanceClass="db.t3.micro", Engine="postgres", MasterUsername=self.DB_USER, MasterUserPassword=self.DB_PASSWORD, AllocatedStorage=20
        )
        cluster_arn = kafka.create_cluster(
            ClusterName="dynamicio", KafkaVersion="3.6.1", NumberOfBrokerNodes=1, BrokerNodeGroupInfo={"InstanceType": "kafka.m5.large", "ClientSubnets": ["subnet-1"]}
        )["ClusterArn"]

        endpoint = self.wait_for(lambda: rds.describe_db_instances(DBInstanceIdentifier="dynamicio")["DBInstances"][0].get("Endpoint"), "RDS")
        self.wait_for(lambda: kafka.describe_cluster(ClusterArn=cluster_arn)["ClusterInfo"]["State"] == "ACTIVE", "MSK")
        # The bootstrap string lists the address inside the Docker network first and the address on the host last
        bootstrap = kafka.get_bootstrap_brokers(ClusterArn=cluster_arn)["BootstrapBrokerString"].split(",")[-1]
        stack = FlociStack(
            endpoint_url=endpoint_url,
            bucket=bucket,
            db_host="localhost",  # the RDS endpoint address is the container address; the floci proxy publishes the port on the host
            db_port=int(endpoint["Port"]),
            db_user=self.DB_USER,
            db_password=self.DB_PASSWORD,
            db_name=self.DB_NAME,
            kafka_bootstrap=bootstrap,
            kafka_container=self.wait_for(lambda: next((name for name in self.children() if name.startswith("floci-aws-msk")), ""), "the MSK container"),
        )
        self.wait_for(lambda: stack.pg_connection().close() is None, "Postgres")
        self.wait_for(lambda: AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5).brokers, "the Kafka broker")
        return stack

    def children(self) -> Set[str]:
        """Return the names of the RDS and MSK containers that this floci started."""
        return self.floci_children() - self.others

    def stop(self) -> None:
        """Remove floci and the RDS and MSK containers that it started."""
        subprocess.run(["docker", "rm", "-f", self.name, *sorted(self.children())], capture_output=True, check=False)


class FlociCaches:
    """The per-process AWS caches of dynamicio."""

    @staticmethod
    def clear() -> None:
        with with_s3._SESSION_LOCK:  # pylint: disable=protected-access
            with_s3._SESSION.clear()  # pylint: disable=protected-access
            with_s3._CLIENT.clear()  # pylint: disable=protected-access
        with_s3._FILESYSTEM.clear()  # pylint: disable=protected-access


@pytest.fixture(scope="package")
def floci() -> Iterator[FlociStack]:
    """A running floci, with the AWS environment of the process pointed at it while the integration tests run.

    The shared boto3 session, S3 client and pyarrow file systems of dynamicio are cached for each process. They are
    dropped before and after, so that no cached client of the unit tests reaches floci, and no client of floci stays.
    """
    if not Floci.docker_is_available():
        pytest.skip("the integration tests need Docker")
    server = Floci()
    with pytest.MonkeyPatch.context() as environment:
        for name, value in Floci.ENVIRONMENT.items():
            environment.setenv(name, value)
        for name in ("AWS_PROFILE", "AWS_SESSION_TOKEN"):
            environment.delenv(name, raising=False)
        try:
            stack = server.start()
            FlociCaches.clear()
            yield stack
        finally:
            FlociCaches.clear()
            server.stop()


@pytest.fixture
def prefix() -> str:
    """A unique key prefix in the bucket of floci, so that the tests do not see each other's objects."""
    return f"it/{uuid.uuid4().hex[:8]}"


@pytest.fixture
def pg_config(floci, tmp_path):  # pylint: disable=redefined-outer-name
    """The `pg` table of `tests.mocking.models.PgRows` on the floci RDS instance, dropped after each test."""
    yield Resources.postgres(tmp_path, floci)
    with closing(floci.pg_connection()) as connection:
        with connection, connection.cursor() as cursor:  # `with connection` commits but does not close
            cursor.execute("DROP TABLE IF EXISTS pg")


@pytest.fixture
def topic(floci) -> str:  # pylint: disable=redefined-outer-name
    """A new Kafka topic with 1 partition, so that the messages keep their order."""
    name = f"it-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": floci.kafka_bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(30)
    return name
