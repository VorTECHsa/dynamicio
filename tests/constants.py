"""A module for configuring all dynamic environment variables for testing purposes"""

import os

TEST_RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "resources")

# Dynamic Vars
MOCK_BUCKET = "mock-bucket"
MOCK_KEY = "mock-key"
KAFKA_SERVER = "mock-kafka-server"
KAFKA_TOPIC = "mock-kafka-topic"
DB_HOST = "127.0.0.1"
DB_PORT = "17039"
DB_NAME = "backend"
DB_USER = "user"
DB_PASS = "pass"
LOWER_THAN_LIMIT = 1000
