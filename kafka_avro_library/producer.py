import copy
import logging
from typing import Any, Dict

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from kafka_avro_library.exceptions import MessageDeliveryFailure
from kafka_avro_library import utils
from kafka_avro_library.utils import (
    build_schema_registry_client,
    build_avro_serializer,
)
from kafka_avro_library.message import ProducerMessage


class BaseProducer:
    def __init__(self, _producer: Any, topic: str):
        self._default_topic = topic
        self._producer = _producer

    @staticmethod
    def acknowledge(err: Any, msg: Any) -> None:
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            logging.error("Failed to deliver message: {}".format(err))
            raise MessageDeliveryFailure(
                f"Message delivery failed due to : {err}"
            )
        else:
            logging.info(
                "Produced record to topic {} partition [{}] @ offset {}".format(
                    msg.topic(), msg.partition(), msg.offset()
                )
            )

    def send_message(self, topic: str, message: ProducerMessage) -> None:
        logging.info(
            "Producing Avro record: {}\t{}".format(message.key, message.value)
        )

        self._producer.produce(
            topic=topic,
            key=message.key,
            value=message.value,
            headers=message.header.to_dict(),
            on_delivery=self.acknowledge,
        )

        self._producer.flush()

        logging.info("Messages were produced to topic {}!".format(topic))


class AvroProducer(BaseProducer):
    def __init__(self, _config: Dict[Any, Any], topic: str) -> None:
        config = copy.deepcopy(_config)
        schema_registry_client = build_schema_registry_client(config)

        registered_schema = schema_registry_client.get_latest_version(
            "{}-value".format(topic)
        )
        message_schema = registered_schema.schema.schema_str

        message_avro_serializer = build_avro_serializer(
            message_schema, schema_registry_client
        )

        producer_conf = utils.pop_schema_registry_params_from_config(config)
        producer_conf["value.serializer"] = message_avro_serializer
        producer_conf["key.serializer"] = StringSerializer()
        _producer: SerializingProducer = SerializingProducer(producer_conf)

        super().__init__(_producer=_producer, topic=topic)
