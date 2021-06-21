import copy
import json
import logging
import time
from typing import (
    Dict,
    Callable,
    Iterator,
    Any,
    ContextManager,
)

from confluent_kafka import DeserializingConsumer
from confluent_kafka.cimpl import Consumer
from confluent_kafka.error import ValueDeserializationError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import (
    SerializationError,
    StringDeserializer,
)
from kafka_avro_library import utils
from kafka_avro_library.message import KafkaMessage
from kafka_avro_library.message_header import build_message_header

_default_config = {"auto.offset.reset": "earliest"}


def _default_on_deserialization_error(ex: Any) -> None:
    logging.error(
        f"Error processing message with trace_id {ex.kafkaMessageHeaders().traceId} and value {ex.kafkaMessage()}"
    )


def _default_on_error(error: Exception) -> None:
    logging.error("Failed to process message", exc_info=error)


def _default_on_completed() -> None:
    logging.info("Kafka consumer stopped.")


class _MessageWithError:
    __slots__ = ["_exception", "_message", "_message_headers"]

    def __init__(self, exception: Exception, message: Any):
        self._exception = exception
        self._message_headers = build_message_header(message.headers())
        self._message = message.value().decode("utf-8")

    def eventError(self) -> Any:
        return self._exception

    def kafkaMessage(self) -> Any:
        return self._message

    def kafkaMessageHeaders(self) -> Any:
        logging.info("Message headers", self._message_headers)
        return self._message_headers

    def __repr__(self) -> str:
        return json.dumps(
            {
                "eventError": str(self.eventError()),
                "kafkaMessage": self.kafkaMessage(),
            }
        )


class BaseConsumer(ContextManager[Callable[..., Iterator[KafkaMessage]]]):
    def __init__(
            self, _consumer: Consumer, _topic: str, listen_time: int = 0,
    ) -> None:
        self._consumer = _consumer
        self._topic: str = _topic
        if listen_time == 0:
            self.listen_end_time = None
        else:
            self.listen_end_time = int(time.time() + listen_time)

    def __enter__(self) -> Callable[..., Iterator[KafkaMessage]]:
        self._consumer.subscribe([self._topic])
        logging.info(
            f"Subscribed to topic: {self._topic} until {self.listen_end_time}"
        )
        return self._iterator

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._consumer.close()
        logging.info("Consumer stopped.")

    def _iterator(self) -> Iterator[KafkaMessage]:
        while (
                self.listen_end_time is None or time.time() < self.listen_end_time
        ):
            try:
                message = None
                try:
                    message = self._consumer.poll(5)
                    if message is None:
                        logging.debug(
                            f"No message received on topic: {self._topic}"
                        )
                        continue
                    kafka_message: Any = KafkaMessage(message)
                except ValueDeserializationError as ex:
                    logging.error(
                        "Wrapping DeSerializerError as a _MessageWithError message",
                        exc_info=True,
                    )
                    kafka_message = _MessageWithError(ex, ex.kafka_message)
                logging.info(
                    f"Message received: topic={self._topic}, {kafka_message}"
                )
                yield kafka_message
            finally:
                return None

    def subscribe(
            self,
            on_next: Callable[[KafkaMessage], None],
            on_error: Callable[[Exception], None] = _default_on_error,
            on_deserialization_error: Callable[
                [KafkaMessage], None
            ] = _default_on_deserialization_error,
            on_completed: Callable[[], None] = _default_on_completed,
    ) -> None:
        try:
            with self as subscription:
                for message in subscription():
                    try:
                        if message.eventError():
                            if isinstance(
                                    message.eventError(), SerializationError
                            ):
                                on_deserialization_error(message)
                        else:
                            on_next(message)
                    except StopIteration:
                        logging.exception(
                            "Received StopIteration, Stopping Kafka consumer..."
                        )
                        break
                    except Exception as ex:
                        try:
                            on_error(ex)
                        except StopIteration:
                            logging.exception(
                                "Received StopIteration, Stopping Kafka consumer..."
                            )
                            break
                        except Exception:
                            logging.exception("Error occurred while processing")
        finally:
            on_completed()


class AvroConsumer(BaseConsumer):
    def __init__(
            self, _config: Dict[str, Any], topic: str, listen_time: int = 0
    ):
        config = copy.deepcopy(_config)
        schema_registry_conf = {
            "url": config["schema.registry.url"],
            "basic.auth.user.info": config["basic.auth.user.info"],
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        registered_schema = schema_registry_client.get_latest_version(
            "{}-value".format(topic)
        )
        message_schema = registered_schema.schema.schema_str

        message_deserializer = utils.build_avro_deserializer(
            message_schema, schema_registry_client
        )

        consumer_conf = utils.pop_schema_registry_params_from_config(config)
        consumer_conf["value.deserializer"] = message_deserializer
        consumer_conf["key.deserializer"] = StringDeserializer()
        super().__init__(
            DeserializingConsumer(consumer_conf), topic, listen_time
        )
