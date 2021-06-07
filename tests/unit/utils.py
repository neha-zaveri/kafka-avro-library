from typing import Any, List, Tuple
from unittest import mock
import json

from confluent_kafka.cimpl import Message, KafkaError

from kafka_avro_library.message_header import (
    Provenance,
    CreatedWith,
    DerivedFrom,
)

TEST_PROVENANCE = Provenance(
    createdOn="2020-06-18T12:53:30.706Z",
    createdBy="org:elsevier-rt",
    createdWith=[
        CreatedWith(
            type="Service",
            id="service:research/working-paper.normalization-service",
            version="1.0.0",
        ),
        CreatedWith(
            type="Model",
            id="entity:working-paper.header-footer-model",
            version="1.2.3",
        ),
        CreatedWith(
            type="Model",
            id="entity:working-paper.superscript-subscript-model",
            version="1.2.3",
        ),
    ],
    derivedFrom=[
        DerivedFrom(
            type="Event",
            eventName="WorkingPaper.Harvested",
            id="93126e00-6bf0-4a61-a16a-5a6d1d402338",
        ),
        DerivedFrom(
            type="Data",
            id="s3://com-elsevier-dp-caps-working-paper-harvested-dev/4e/a4/4ea41346-0a9d-4915-960b-ad5a9a30dae6/harvested.zip",
        ),
    ],
)

msg_headers: List[Tuple[str, Any]] = [
    ("id", "b60743ea-b162-11ea-b9af-62599e40c283"),
    ("name", "WorkingPaper.Normalized"),
    ("type", "Normalized"),
    ("subject", "WorkingPaper.Normalized"),
    ("traceId", "traceid"),
    ("provenance", json.dumps(TEST_PROVENANCE.to_dict())),
    ("newheader", "WorkingPaper.Normalized1"),
    ("version", "1"),
    (
        "conformsTo",
        "service:research/working-paper.normalization-service/schema/working-paper.normalized",
    ),
    ("createdOn", "2020-06-18T12:53:30.706Z"),
    ("sender", "service:research/working-paper.normalization-service"),
    ("mediaType", "application/json"),
]


def create_mock_kafka_message(
    headers: List[Tuple[str, bytes]] = None, error_msg: str = None
) -> Message:
    mock_message = mock.Mock(Message)
    mock_message.key.return_value = 1
    mock_message.partition.return_value = 0
    mock_message.offset.return_value = 1
    mock_message.value.return_value = {}
    mock_message.headers.return_value = (
        [(k, bytes(v, encoding="utf-8")) for k, v in msg_headers]
        if headers is None
        else headers
    )

    mock_error = None
    if error_msg:

        def mock_error_repr(self: KafkaError) -> str:
            return error_msg

        mock_error = mock.Mock(KafkaError)
        mock_error.__repr__ = mock_error_repr
    mock_message.error.return_value = mock_error

    return mock_message
