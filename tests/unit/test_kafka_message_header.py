from kafka_avro_library.message_header import (
    MessageHeader,
)


def test_kafka_message_header_converts_to_dictionary() -> None:
    assert MessageHeader(
        id="test_id",
        eventName="test_name",
        traceId="the_trace_id",
    ).to_dict() == {
               "id": "test_id",
               "eventName": "test_name",
               "traceId": "the_trace_id",
           }
