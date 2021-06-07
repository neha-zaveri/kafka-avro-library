from typing import Any


class InvalidMessageValue(Exception):
    pass


class InvalidMessageKey(Exception):
    pass


class InvalidSchemaPath(Exception):
    pass


class MessageDeliveryFailure(Exception):
    def __init__(self, message: Any) -> None:
        super().__init__(message)


class InvalidTopic(Exception):
    def __init__(self, message: Any) -> None:
        super().__init__(message)
