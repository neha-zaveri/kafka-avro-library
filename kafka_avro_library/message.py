import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Union

from confluent_kafka.cimpl import Message

from kafka_avro_library.message_header import (
    MessageHeader,
    create_header,
)

ValueType = Union[Dict[str, Any], str, bytes, Any, None]


class MalformedHeaderException(Exception):
    pass


@dataclass
class ProducerMessage:
    def __init__(
            self, key: str, header: MessageHeader, value: Dict[str, Any]
    ) -> None:
        self.key = key
        self.header = header
        self.value = value


def create_producer_message(
        trace_id: str,
        name: str,
        value: Dict[Any, Any]
) -> ProducerMessage:
    key = str(uuid.uuid4())
    header = create_header(
        id=key,
        name=name,
        traceId=trace_id
    )

    return ProducerMessage(key=key, header=header, value=value)


class KafkaMessage(object):
    def __init__(self, message: Message):
        self._message = message
        self._headers = None
        raw_headers = self._message.headers()
        if raw_headers:
            headers = {k: v.decode("utf-8") for k, v in raw_headers}
            headers.update(
                {
                    "traceId": headers["traceId"],
                }
            )
            self._headers = MessageHeader(**headers)

    def key(self) -> Any:
        return self._message.key()

    def partition(self) -> Any:
        return self._message.partition()

    def offset(self) -> Any:
        return self._message.offset()

    def value(self) -> ValueType:
        return self._message.value()

    def headers(self) -> MessageHeader:
        return self._headers

    def __repr__(self) -> str:
        return json.dumps(
            {
                "partition": self.partition(),
                "offset": self.offset(),
                "key": self.key(),
                "headers": self.headers().to_dict()
                if self.headers() is not None
                else None,
                "value": str(self.value())
                if self.value() is not None
                else None,
            }
        )
