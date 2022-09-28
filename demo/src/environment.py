"""A module for configuring all environment variables."""
import os

ENVIRONMENT = "sample"
CLOUD_ENV = "DEV"
RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../resources")
TEST_RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../tests")
S3_YOUR_INPUT_BUCKET = None
S3_YOUR_OUTPUT_BUCKET = None
KAFKA_SERVER = None
KAFKA_TOPIC = None
DB_HOST = None
DB_PORT = None
DB_NAME = None
DB_USER = None
DB_PASS = None
REFERENCE_DATA_STATE_KEY = None
LOWER_THAN_LIMIT = 1000
