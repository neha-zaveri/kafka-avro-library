from kafka_avro_library import utils

EVENT_SCHEMA_FILE_PATH = (
    "./tests/resources/schema/value/user_schema.avsc"
)


def test_load_schema_from_file() -> None:
    schema_str = utils.load_avro_schema_from_file(EVENT_SCHEMA_FILE_PATH)
    with open(EVENT_SCHEMA_FILE_PATH) as fh:
        result = fh.read()
    assert result == schema_str
