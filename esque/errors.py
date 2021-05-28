from typing import Dict, Optional, Type

from confluent_kafka import KafkaError, Message


def raise_for_kafka_error(err: KafkaError, msg: Optional[Message] = None):
    if not err:
        return None

    if err.code() in CONFLUENT_ERROR_LOOKUP.keys():
        raise CONFLUENT_ERROR_LOOKUP[err.code()](err.str(), err.code())
    else:
        raise KafkaException(err.str(), err.code())


def raise_for_message(message: Message):
    if message is None:
        raise MessageEmptyException
    elif message.error() is not None:
        raise_for_kafka_error(message.error(), message)


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


class ConfigVersionException(ExceptionWithMessage):
    def __init__(self, current, expected):
        super().__init__(f"Expected config version {expected}, got {current}.")


class ConfigTooOld(ConfigVersionException):
    def __init__(self, current, expected):
        super().__init__(current, expected)
        self.message += "\nRun `esque config migrate` to migrate to new version."


class ConfigTooNew(ConfigVersionException):
    def __init__(self, current, expected):
        super().__init__(current, expected)
        self.message += "\nYou might find a backup from last config migration next to your current config."


class ConfigException(ExceptionWithMessage):
    pass


class MissingSaslParameter(ConfigException):
    pass


class UnsupportedSaslMechanism(ConfigException):
    pass


class KafkaException(ExceptionWithMessage):
    def __init__(self, message: str, code: int):
        super().__init__(f"{message} with code {code}.")
        self.code = code


class ConsumerGroupDoesNotExistException(ExceptionWithMessage):
    def __init__(self, consumer_id: str):
        self.consumer_id = consumer_id
        super().__init__(f"Consumer Group does not exist for consumer id '{self.consumer_id}'.")


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


class TopicDeletionException(ExceptionWithMessage):
    pass


class ValidationException(ExceptionWithMessage):
    pass


class YamaleValidationException(ValidationException):
    def __init__(self, validation_error: ValueError):
        complete_message = validation_error.args[0]
        messages = complete_message.split("\n")[2:]
        stripped_messages = list(map(lambda x: x.strip("\t"), messages))
        joined_message = "\n".join(stripped_messages)
        super().__init__(joined_message)


CONFLUENT_ERROR_LOOKUP: Dict[int, Type[KafkaException]] = {
    KafkaError.UNKNOWN_TOPIC_OR_PART: TopicDoesNotExistException,
    KafkaError.TOPIC_ALREADY_EXISTS: TopicAlreadyExistsException,
    KafkaError._PARTITION_EOF: EndOfPartitionReachedException,
    KafkaError.INVALID_REPLICATION_FACTOR: InvalidReplicationFactorException,
}
