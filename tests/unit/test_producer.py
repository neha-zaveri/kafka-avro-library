from unittest import mock

import pytest

from kafka_avro_library.exceptions import MessageDeliveryFailure
from kafka_avro_library.message import create_producer_message
from kafka_avro_library.producer import AvroProducer, BaseProducer

TOPIC = "normalization"

TEST_MESSAGE = create_producer_message(
    value={"id": "blah"},
    trace_id="trace_id",
    name="WorkingPaper.Harvested",
)


def test_send_message_passes_value_and_schema_to_avro_producer() -> None:
    mock_producer = mock.Mock()

    producer = BaseProducer(_producer=mock_producer, topic=TOPIC)

    producer.send_message(TOPIC, TEST_MESSAGE)

    mock_producer.produce.assert_called_once_with(
        topic=TOPIC,
        key=TEST_MESSAGE.key,
        value=TEST_MESSAGE.value,
        headers=TEST_MESSAGE.header.to_dict(),
        on_delivery=producer.acknowledge,
    )


def test_on_delivery_raises_exception_if_error_message_is_present() -> None:
    with pytest.raises(MessageDeliveryFailure):
        BaseProducer.acknowledge(
            err="Oh no!", msg="This message has not been delivered"
        )


def test_on_delivery_does_not_raise_exception_if_no_error_message_is_present() -> None:
    mock_message = mock.Mock()

    # These values do not matter, they just have to be there to format the log message
    mock_message.msg.return_value = "This message has been delivered"
    mock_message.offset.return_value = 4

    # Does not raise an exception
    AvroProducer.acknowledge(err=None, msg=mock_message)
