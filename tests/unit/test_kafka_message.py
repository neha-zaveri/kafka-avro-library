import uuid

from kafka_avro_library.message import (
    create_producer_message,
)


def test_create_producer_message_generates_valid_key() -> None:
    message = create_producer_message(
        value={"id": "blah"},
        trace_id="trace_id",
        name="User.Created",
    )

    # This will throw unless the key string is a correctly formatted uuid4
    _ = uuid.UUID(message.key, version=4)
