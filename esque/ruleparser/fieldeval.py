import datetime
import re
from typing import Any

from confluent_kafka.cimpl import Message

from esque.messages.message import KafkaMessage, decode_message
from esque.ruleparser.expressionelement import Operator


class FieldEval:

    __message = None
    __header_pattern = None
    __time_format: str = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, message: Any = None):
        self.__message = message
        self.__header_pattern = re.compile(Operator.FIELDS["MESSAGE_HEADER"], flags=re.IGNORECASE)

    def evaluate_field(self, field_name: str):
        """This method contains the logic to evaluate the field values. New fields are added as follows:
        (1) add a new element to the Operator.FIELDS set. The key is arbitrary, and the field contains a regex for the parser,
        (2) add an implementation to this method
        """
        if field_name == Operator.FIELDS["SYSTEM_TIMESTAMP"].replace("\\", ""):
            return datetime.datetime.strftime(datetime.datetime.now(), self.__time_format)
        elif self.__message is None:
            return -1
        elif isinstance(self.__message, Message):
            return self.__evaluate_message_field(field_name=field_name)
        elif isinstance(self.__message, KafkaMessage):
            return self.__evaluate_kafka_message_field(field_name=field_name)

    def __evaluate_message_field(self, field_name: str):
        if field_name == Operator.FIELDS["MESSAGE_OFFSET"].replace("\\", ""):
            return self.__message.offset()
        elif field_name == Operator.FIELDS["MESSAGE_PARTITION"].replace("\\", ""):
            partition = self.__message.partition()
            return -1 if not partition else partition
        elif field_name == Operator.FIELDS["MESSAGE_TIMESTAMP"].replace("\\", ""):
            return datetime.datetime.strftime(
                datetime.datetime.fromtimestamp(self.__message.timestamp()[1] / 1000.0), self.__time_format
            )
        elif field_name == Operator.FIELDS["MESSAGE_LENGTH"].replace("\\", ""):
            return len(self.__message)
        elif field_name == Operator.FIELDS["MESSAGE_KEY"].replace("\\", ""):
            return self.__message.key().decode("UTF-8")
        elif self.__header_pattern.fullmatch(field_name):
            header_name = field_name.split(".")[-1]
            if self.__message.headers() is not None:
                message_headers = decode_message(message=self.__message).headers
                return next(
                    (message_header.value for message_header in message_headers if message_header.key == header_name),
                    "no_header",
                )
            return "no_header"
        return -1

    def __evaluate_kafka_message_field(self, field_name: str):
        if field_name == Operator.FIELDS["MESSAGE_OFFSET"].replace("\\", ""):
            return -1
        elif field_name == Operator.FIELDS["MESSAGE_PARTITION"].replace("\\", ""):
            partition = self.__message.partition
            return -1 if not partition else partition
        elif field_name == Operator.FIELDS["MESSAGE_LENGTH"].replace("\\", ""):
            return len(self.__message.key) + len(self.__message.value)
        elif field_name == Operator.FIELDS["MESSAGE_KEY"].replace("\\", ""):
            return self.__message.key
        elif self.__header_pattern.fullmatch(field_name):
            header_name = field_name.split(".")[-1]
            message_headers = self.__message.headers
            return next(
                (message_header.value for message_header in message_headers if message_header.key == header_name),
                "no_header",
            )
        return -1
