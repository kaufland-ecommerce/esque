import datetime
import re

from confluent_kafka.cimpl import Message

from esque.ruleparser.expressionelement import Operator


class FieldEval:

    __message: Message = None
    __header_pattern = None
    __time_format: str = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, message: Message = None):
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
                    return self.__message.offset()
                elif field_name == Operator.FIELDS["MESSAGE_PARTITION"].replace("\\", ""):
                    partition = self.__message.partition()
                    if partition:
                        return partition
                    else:
                        return -1
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
                    header_value = self.__message.headers()[header_name]
                    if header_value is None:
                        return ""
                    else:
                        return header_value
