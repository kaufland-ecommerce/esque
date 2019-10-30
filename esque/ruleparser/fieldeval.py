import datetime
import re
from typing import Any

from confluent_kafka.cimpl import Message

from esque.messages.message import decode_message, KafkaMessage
from esque.ruleparser.expressionelement import Operator


class FieldEval:

    __message = None
    __header_pattern = None
    __time_format: str = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, message: Any = None):
        self.__message = message
        self.__header_pattern = re.compile(Operator.FIELDS["MESSAGE_HEADER"], flags=re.IGNORECASE)

    def evaluate_field(self, field_name: str):
        """ This method contains the logic to evaluate the field values. New fields are added as follows:
            (1) add a new element to the Operator.FIELDS set. The key is arbitrary, and the field contains a regex for the parser,
            (2) add an implementation to this method
        """
        if field_name == Operator.FIELDS["SYSTEM_TIMESTAMP"].replace("\\", ""):
            return datetime.datetime.strftime(datetime.datetime.now(), self.__time_format)
        else:
            if self.__message is None:
                return -1
            else:
                if field_name == Operator.FIELDS["MESSAGE_OFFSET"].replace("\\", ""):
                    if isinstance(self.__message, Message):
                        return self.__message.offset()
                    elif isinstance(self.__message, KafkaMessage):
                        return -1
                elif field_name == Operator.FIELDS["MESSAGE_PARTITION"].replace("\\", ""):
                    if isinstance(self.__message, Message):
                        partition = self.__message.partition()
                    elif isinstance(self.__message, KafkaMessage):
                        partition = self.__message.partition
                    return -1 if not partition else partition
                elif field_name == Operator.FIELDS["MESSAGE_TIMESTAMP"].replace("\\", "") and isinstance(
                    self.__message, Message
                ):
                    return datetime.datetime.strftime(
                        datetime.datetime.fromtimestamp(self.__message.timestamp()[1] / 1000.0), self.__time_format
                    )
                elif field_name == Operator.FIELDS["MESSAGE_LENGTH"].replace("\\", ""):
                    if isinstance(self.__message, Message):
                        return len(self.__message)
                    elif isinstance(self.__message, KafkaMessage):
                        return len(self.__message.key) + len(self.__message.value)
                elif field_name == Operator.FIELDS["MESSAGE_KEY"].replace("\\", ""):
                    if isinstance(self.__message, Message):
                        return self.__message.key().decode("UTF-8")
                    elif isinstance(self.__message, KafkaMessage):
                        return self.__message.key
                elif self.__header_pattern.fullmatch(field_name) and isinstance(self.__message, Message):
                    header_name = field_name.split(".")[-1]
                    if isinstance(self.__message, Message):
                        if self.__message.headers() is None:
                            return ""
                        decoded_message = decode_message(message=self.__message)
                        for message_header in decoded_message.headers:
                            if message_header.key == header_name:
                                return message_header.value if message_header.value else ""
                    elif isinstance(self.__message, KafkaMessage):
                        for message_header in self.__message.headers:
                            if message_header.key == header_name and message_header.value:
                                return message_header.value
                            elif message_header.key == header_name:
                                return ""
                        return ""
                return -1
