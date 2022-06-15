"""A module for configuring all environment variables."""
import os

# Let's keep type checkers happy
ENVIRONMENT: str
CLOUD_ENV: str
RESOURCES: str
S3_YOUR_INPUT_BUCKET: str
S3_YOUR_OUTPUT_BUCKET: str
DB_PASS: str
DB_HOST: str
DB_PORT: str
DB_NAME: str

# Keys are environment variable names, values are default values. Pass None for no default.
__REQUIRED_ENVIRONMENT_VARIABLES__ = {
    "ENVIRONMENT": "sample",
    "CLOUD_ENV": "DEV",
    "RESOURCES": os.path.join(os.path.dirname(os.path.realpath(__file__)), "../resources"),
    "TEST_RESOURCES": os.path.join(os.path.dirname(os.path.realpath(__file__)), "../tests"),
    "S3_YOUR_INPUT_BUCKET": None,
    "S3_YOUR_OUTPUT_BUCKET": None,
    "KAFKA_SERVER": None,
    "KAFKA_TOPIC": None,
    "DB_HOST": None,
    "DB_PORT": None,
    "DB_NAME": None,
    "DB_USER": None,
    "DB_PASS": None,
    "REFERENCE_DATA_STATE_KEY": None,
}

# Let's dynamically fetch those values from the environment and add them to the local scope
for required_variable, default_value in __REQUIRED_ENVIRONMENT_VARIABLES__.items():
    locals()[required_variable] = os.getenv(required_variable, default_value)
