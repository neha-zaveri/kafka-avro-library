from typing import Any, Dict, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def wait_to_start(
    docker_service_name: str,
    port: Union[int, None],
    session_scoped_container_getter: Any,
) -> str:
    request_session = requests.Session()
    retries = Retry(total=25, backoff_factor=0.1)
    request_session.mount("http://", HTTPAdapter(max_retries=retries))
    service = session_scoped_container_getter.get(
        docker_service_name
    ).network_info[0]
    api_url = "http://%s:%s/" % (
        service.hostname,
        service.host_port if port is None else port,
    )
    assert request_session.get(api_url)
    request_session.close()
    return api_url


def wait_for_kafka_to_start(session_scoped_container_getter: Any) -> str:
    api_url = wait_to_start(
        "rest-proxy", None, session_scoped_container_getter,
    )
    return api_url


def cluster_info(session_scoped_container_getter: Any) -> Dict[str, str]:
    config = {}
    for service in ["zookeeper", "broker", "schema-registry"]:
        config.update(
            _service_host_and_port(session_scoped_container_getter, service)
        )
    return config


def _service_host_and_port(
    session_scoped_container_getter: Any, service_name: str
) -> Dict[str, str]:
    service = session_scoped_container_getter.get(service_name).network_info[0]
    return {
        f"{service_name}.host": service.hostname,
        f"{service_name}.port": service.host_port,
    }


def wait_for_schema_registry_to_start(
    session_scoped_container_getter: Any,
) -> str:
    api_url = wait_to_start(
        "schema-registry", None, session_scoped_container_getter,
    )
    return api_url
