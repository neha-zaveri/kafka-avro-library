from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AvroSerializer,
    AvroDeserializer,
)


def read_ccloud_config(config_file: str) -> Any:
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf


def pop_schema_registry_params_from_config(conf: Any) -> Any:
    """Remove potential Schema Registry related configurations from dictionary"""
    conf.pop("schema.registry.url", None)
    conf.pop("basic.auth.user.info", None)
    conf.pop("basic.auth.credentials.source", None)

    return conf


def load_avro_schema_from_file(file_path: str) -> str:
    with open(file_path) as fh:
        return fh.read()


def build_avro_serializer(
    schema_str: str, schema_registry_client: SchemaRegistryClient
) -> AvroSerializer:
    avro_serializer = AvroSerializer(schema_str, schema_registry_client)
    return avro_serializer


def build_avro_deserializer(
    schema_str: str, schema_registry_client: SchemaRegistryClient
) -> AvroDeserializer:
    avro_serializer = AvroDeserializer(schema_str, schema_registry_client)
    return avro_serializer


def build_schema_registry_client(conf: Any) -> SchemaRegistryClient:
    schema_registry_conf = {
        "url": conf["schema.registry.url"],
        "basic.auth.user.info": conf["basic.auth.user.info"],
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    return schema_registry_client
