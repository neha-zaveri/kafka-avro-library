from typing import Dict

from confluent_kafka.schema_registry import Schema

from kafka_avro_library.consumer import AvroConsumer
from kafka_avro_library.message import (
    create_producer_message,
)
from kafka_avro_library.producer import AvroProducer
from kafka_avro_library.utils import (
    build_schema_registry_client,
    load_avro_schema_from_file,
)

ENCODING_UTF_8 = "utf-8"
TOPIC = "user"

TEST_MESSAGE = create_producer_message(
    value={
        "id": "",
        "name": ""
    },
    trace_id="trace_id",
    name="",
)

EVENT_SCHEMA_FILE_PATH = (
    "./tests/resources/schema/value/user_schema.avsc"
)


def register_schema_for_topic(config: Dict[str, str]) -> None:
    schema_str = load_avro_schema_from_file(EVENT_SCHEMA_FILE_PATH)
    schema_client = build_schema_registry_client(config)
    schema_client.register_schema(
        "{}-value".format(TOPIC), Schema(schema_str, "AVRO")
    )


def test_end_to_end_avro(kafka_cluster_confluent_avro: Dict[str, str]) -> None:
    register_schema_for_topic(kafka_cluster_confluent_avro)
    producer = AvroProducer(kafka_cluster_confluent_avro, TOPIC)
    producer.send_message(TOPIC, TEST_MESSAGE)
    consumer_config = {
        **kafka_cluster_confluent_avro,
        "group.id": "test-group",
        "auto.offset.reset": "earliest",
    }
    consumer = AvroConsumer(consumer_config, TOPIC)
    with consumer as it:
        message = next(it())
        assert message.key() == TEST_MESSAGE.key
        assert message.headers() == TEST_MESSAGE.header
        assert message.value() == TEST_MESSAGE.value
