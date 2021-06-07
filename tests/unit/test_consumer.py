from unittest.mock import Mock

from confluent_kafka.error import ValueDeserializationError

from kafka_avro_library.consumer import BaseConsumer
from kafka_avro_library.message import KafkaMessage


def print_message(message: KafkaMessage) -> None:
    raise Exception


def test_consumer_deserialisation_error_handler_is_called() -> None:
    mock_consumer = Mock()
    message = Mock()
    message.value.return_value = bytes("I m message", "utf-8")
    message.headers.return_value = [
        ("id", b"ca506843-d17e-4c5c-bbdc-cf708e0c270b"),
        ("eventName", b""),
        ("traceId", b"trace_id"),
    ]

    mock_consumer.poll.side_effect = ValueDeserializationError(
        exception="Error occured", kafka_message=message
    )
    consumer = BaseConsumer(mock_consumer, "", 5)
    deserialization_mock = Mock()
    consumer.subscribe(
        on_next=print_message, on_deserialization_error=deserialization_mock,
    )
    deserialization_mock.assert_called()
