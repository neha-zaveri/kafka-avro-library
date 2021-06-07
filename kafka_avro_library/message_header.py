import json
from dataclasses import dataclass, asdict, field
from typing import Dict, Any, List

from dp_caps_library_common_utils import datetime_utils


@dataclass
class MessageHeader:
    id: str
    eventName: str
    traceId: str

    def __init__(
            self,
            id: str,
            eventName: str,
            traceId: str
    ) -> None:
        self.id = id
        self.eventName = eventName
        self.traceId = traceId

    def to_dict(self) -> Dict[Any, Any]:
        response = {
            "id": self.id,
            "eventName": self.eventName,
            "traceId": str(self.traceId),
        }
        return response


def create_header(
        id: str,
        name: str,
        traceId: str = None,
) -> MessageHeader:
    return MessageHeader(
        id=id,
        eventName=name,
        traceId=traceId,
    )


def build_message_header(raw_headers: Any) -> MessageHeader:
    headers = {k: v.decode("utf-8") for k, v in raw_headers}
    headers.update({"traceId": headers["traceId"]})
    return MessageHeader(**headers)
