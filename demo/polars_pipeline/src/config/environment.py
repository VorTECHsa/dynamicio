"""The variables of the resource definitions of the polars demo pipeline."""
import os

ENVIRONMENT = "LOCAL"
RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../resources")
# The inputs and the expected outputs of the pandas demo: the polars pipeline reads the same files and must write the same frames
DEMO_DATA = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../tests/data")
TEST_RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../tests")
DEV_S3_INPUT_BUCKET = None
DEV_S3_OUTPUT_BUCKET = None
PROD_S3_INPUT_BUCKET = None
PROD_S3_OUTPUT_BUCKET = None
DEV_KAFKA_BROKER = None
PROD_KAFKA_BROKER = None
KAFKA_TOPIC = None
DEV_DB_HOST = None
PROD_DB_HOST = None
DB_PORT = None
DB_NAME = None
DB_USER = None
DB_PASS = None
