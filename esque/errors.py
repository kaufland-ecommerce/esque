from typing import Dict, Type

from confluent_kafka import KafkaError, Message

import pykafka.exceptions


def raise_for_kafka_error(err: KafkaError):
    if not err:
        return None

    if err.code() in ERROR_LOOKUP.keys():
        raise ERROR_LOOKUP[err.code()](err.str(), err.code())
    else:
        raise KafkaException(err.str(), err.code())


def raise_for_message(message: Message):
    if message is None:
        raise MessageEmptyException
    elif message.error() is not None:
        raise_for_kafka_error(message.error())


class EsqueException(Exception):
    pass


class ExceptionWithMessage(EsqueException):
    def __init__(self, message):
        self.message = message

    def format_message(self):
        return f"{type(self).__name__}: {self.message}"


class EditCanceled(ExceptionWithMessage):
    def __init__(self):
        super().__init__("Edit canceled.")


class ConfigException(ExceptionWithMessage):
    pass


class MissingSaslParameter(ConfigException):
    pass


class UnsupportedSaslMechanism(ConfigException):
    pass


class KafkaException(ExceptionWithMessage):
    def __init__(self, message: str, code: int):
        super().__init__(f"{message} with code {code}")
        self.code = code


class ConsumerGroupDoesNotExistException(ExceptionWithMessage):
    def __init__(self, consumer_id: str):
        super().__init__(f"Consumer Group does not exist for consumer id '{self.consumer_id}'")
        self.consumer_id = consumer_id


class ConfigNotExistsException(ExceptionWithMessage):
    def __init__(self):
        super().__init__("Config does not exist.")


class NoConfirmationPossibleException(ExceptionWithMessage):
    def __init__(self):
        super().__init__(
            "You are running this command in a non-interactive mode. To do this you must use the --no-verify option."
        )


class ContextNotDefinedException(ExceptionWithMessage):
    def __init__(self, message: str = None):
        if message is None:
            message = "Context cannot be found."
        super().__init__(message)


class FutureTimeoutException(ExceptionWithMessage):
    pass


class MessageEmptyException(KafkaException):
    def __init__(self):
        super().__init__("Consumed Message is empty.", -185)


class TopicAlreadyExistsException(KafkaException):
    pass


class EndOfPartitionReachedException(KafkaException):
    pass


class TopicDoesNotExistException(KafkaException):
    pass


class InvalidReplicationFactorException(KafkaException):
    pass


class ConnectionFailedException(ExceptionWithMessage):
    def __init__(self, pykafka_exception: pykafka.exceptions.KafkaException):
        self.pykafka_exception = pykafka_exception

        if isinstance(self.pykafka_exception.args, str):
            msg = self.pykafka_exception.args
        else:
            msg = f"Connection to brokers failed."
        super().__init__(msg)


class ValidationException(ExceptionWithMessage):
    pass


class YamaleValidationException(ValidationException):
    def __init__(self, validation_error: ValueError):
        complete_message = validation_error.args[0]
        messages = complete_message.split("\n")[2:]
        stripped_messages = list(map(lambda x: x.strip("\t"), messages))
        joined_message = "\n".join(stripped_messages)
        super().__init__(joined_message)


class TopicConfigNotValidException(YamaleValidationException):
    pass


ERROR_LOOKUP: Dict[int, Type[KafkaException]] = {
    3: TopicDoesNotExistException,
    36: TopicAlreadyExistsException,
    -191: EndOfPartitionReachedException,
    38: InvalidReplicationFactorException,
}
