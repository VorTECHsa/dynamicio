from dynamicio import ParquetResource, CsvResource, JsonResource, HdfResource,S3ParquetResource, S3CsvResource, S3JsonResource, S3HdfResource, KafkaResource, PostgresResource

resource_b_resource = S3ParquetResource(
    bucket="[[ PROJECT_BUCKET]]",
    path="evaluation/test_predictions/[[ RUN_ID ]]/test_predictions_{route}_{outlook}d.parquet",
    test_path="[[ TEST_RESOURCES_PATH ]]/data/evaluation/test_predictions_{route}_{outlook}d.parquet"
)


hybrid_model_configuration_resource = JsonResource(
    path="[[ RESOURCES_DIR_PATH ]]/hybrid_model_configurations/{days_in_advance}d.json",
    test_path="[[ TEST_RESOURCES_DIR_PATH ]]/data/modelling/hybrid_model_configuration.json"
)


voyage_messages_resource = KafkaResource(
    server="[[ KAFKA_SERVER ]]",
    topic="[[ KAFKA_TOPIC ]]",
    test_path="[[ LOCAL_DATA ]]/sink/voyage_messages.json"
)


freight_rates_resource = PostgresResource(
    db_host="[[ DB_HOST ]]",
    db_port="[[ DB_PORT ]]",
    db_name="[[ DB_NAME ]]",
    db_user="[[ DB_USER ]]",
    db_password="[[ DB_PASS ]]",
    table_name=None,
    sql_query=...,
    test_path="[[ TEST_RESOURCES_PATH ]]/data/input/freight_rates.parquet"
)
