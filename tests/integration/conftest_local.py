from typing import Any

import pytest

from kafka_avro_library import utils
from tests.integration.docker_compose_operations import (
    wait_for_kafka_to_start,
)


@pytest.fixture(
    name="kafka_cluster_confluent_avro", scope="session", autouse=True
)
def kafka_cluster_confluent_avro(session_scoped_container_getter: Any, ) -> Any:
    wait_for_kafka_to_start(session_scoped_container_getter)
    config_file = "./tests/resources/avro.config"
    config = utils.read_ccloud_config(config_file)
    return config
