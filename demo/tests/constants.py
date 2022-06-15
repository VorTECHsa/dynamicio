"""A module for configuring all dynamic environment variables for testing purposes"""
import os

E2E_TEST_RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)))

# Dynamic Vars
S3_INPUT_BUCKET = "mock-foo-input-bucket"
S3_OUTPUT_BUCKET = "mock-bar-output-bucket"
DB_HOST = "10.0.2.217"
DB_PORT = "5432"
DB_NAME = "backend"
DB_USER = "user"
DB_PASS = "pass"
