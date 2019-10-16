import functools
from enum import Enum
from typing import Dict, Type

import confluent_kafka
import pykafka.exceptions
from click import ClickException
from confluent_kafka import KafkaError, Message


def translate_third_party_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except confluent_kafka.KafkaException as ex:
            raise_for_kafka_error(ex.args[0])
        except pykafka.exceptions.NoBrokersAvailableError as exception:
            raise ConnectionFailedException(exception)
        except pykafka.exceptions.SocketDisconnectedError as exception:
            raise ConnectionFailedException(exception)

    return wrapper


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


class ExceptionWithMessage(ClickException):
    def format_message(self):
        return f"{type(self).__name__}: {self.message}"


class ConfigException(ExceptionWithMessage):
    pass


class MissingSaslParameter(ConfigException):
    pass


class UnsupportedSaslMechanism(ConfigException):
    pass


class KafkaException(ClickException):
    def __init__(self, message: str, code: int):
        super().__init__(f"{message} with code {code}")
        self.code = code
        self.exit_code = code


class ConsumerGroupDoesNotExistException(ExceptionWithMessage):
    exit_code = 4

    def __init__(self, consumer_id: str):
        super().__init__(f"Consumer Group does not exist for consumer id '{self.consumer_id}'")
        self.consumer_id = consumer_id


class ConfigNotExistsException(ExceptionWithMessage):
    exit_code = 5

    def __init__(self):
        super().__init__("Config does not exist.")


class NoConfirmationPossibleException(ExceptionWithMessage):
    exit_code = 6

    def __init__(self):
        super().__init__(
            "You are running this command in a non-interactive mode. To do this you must use the --no-verify option."
        )


class ContextNotDefinedException(ExceptionWithMessage):
    exit_code = 7

    def __init__(self, message: str = None):
        if message is None:
            message = "Context cannot be found."
        super().__init__(message)


class FutureTimeoutException(ExceptionWithMessage):
    exit_code = 8
    pass


class MessageEmptyException(KafkaException):
    def __init__(self):
        super().__init__("Consumed Message is empty.", -185)


class TopicAlreadyExistsException(KafkaException):
    exit_code = 36


class EndOfPartitionReachedException(KafkaException):
    exit_code = -191


class TopicDoesNotExistException(KafkaException):
    exit_code = 3


class ConnectionFailedException(ExceptionWithMessage):
    exit_code = 9

    def __init__(self, pykafka_exception: pykafka.exceptions.KafkaException):
        self.pykafka_exception = pykafka_exception

        if isinstance(self.pykafka_exception.args, str):
            msg = self.pykafka_exception.args
        else:
            msg = f"Connection to brokers failed."
        super().__init__(msg)


ERROR_LOOKUP: Dict[int, Type[KafkaException]] = {
    3: TopicDoesNotExistException,
    36: TopicAlreadyExistsException,
    -191: EndOfPartitionReachedException,
}


class ErrorCode(Enum):
    BROKER_NOT_AVAILABLE = 8
    CLUSTER_AUTHORIZATION_FAILED = 31
    CONCURRENT_TRANSACTIONS = 51
    DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65
    DELEGATION_TOKEN_AUTH_DISABLED = 61
    DELEGATION_TOKEN_EXPIRED = 66
    DELEGATION_TOKEN_NOT_FOUND = 62
    DELEGATION_TOKEN_OWNER_MISMATCH = 63
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64
    DUPLICATE_SEQUENCE_NUMBER = 46
    FETCH_SESSION_ID_NOT_FOUND = 70
    GROUP_AUTHORIZATION_FAILED = 30
    GROUP_COORDINATOR_NOT_AVAILABLE = 15
    GROUP_ID_NOT_FOUND = 69
    GROUP_LOAD_IN_PROGRESS = 14
    ILLEGAL_GENERATION = 22
    ILLEGAL_SASL_STATE = 34
    INCONSISTENT_GROUP_PROTOCOL = 23
    INVALID_COMMIT_OFFSET_SIZE = 28
    INVALID_CONFIG = 40
    INVALID_FETCH_SESSION_EPOCH = 71
    INVALID_GROUP_ID = 24
    INVALID_MSG = 2
    INVALID_MSG_SIZE = 4
    INVALID_PARTITIONS = 37
    INVALID_PRINCIPAL_TYPE = 67
    INVALID_PRODUCER_EPOCH = 47
    INVALID_PRODUCER_ID_MAPPING = 49
    INVALID_REPLICATION_FACTOR = 38
    INVALID_REPLICA_ASSIGNMENT = 39
    INVALID_REQUEST = 42
    INVALID_REQUIRED_ACKS = 21
    INVALID_SESSION_TIMEOUT = 26
    INVALID_TIMESTAMP = 32
    INVALID_TRANSACTION_TIMEOUT = 50
    INVALID_TXN_STATE = 48
    KAFKA_STORAGE_ERROR = 56
    LEADER_NOT_AVAILABLE = 5
    LISTENER_NOT_FOUND = 72
    LOG_DIR_NOT_FOUND = 57
    MSG_SIZE_TOO_LARGE = 10
    NETWORK_EXCEPTION = 13
    NON_EMPTY_GROUP = 68
    NOT_CONTROLLER = 41
    NOT_COORDINATOR_FOR_GROUP = 16
    NOT_ENOUGH_REPLICAS = 19
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
    NOT_LEADER_FOR_PARTITION = 6
    NO_ERROR = 0
    OFFSET_METADATA_TOO_LARGE = 12
    OFFSET_OUT_OF_RANGE = 1
    OPERATION_NOT_ATTEMPTED = 55
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45
    POLICY_VIOLATION = 44
    REASSIGNMENT_IN_PROGRESS = 60
    REBALANCE_IN_PROGRESS = 27
    RECORD_LIST_TOO_LARGE = 18
    REPLICA_NOT_AVAILABLE = 9
    REQUEST_TIMED_OUT = 7
    SASL_AUTHENTICATION_FAILED = 58
    SECURITY_DISABLED = 54
    STALE_CTRL_EPOCH = 11
    TOPIC_ALREADY_EXISTS = 36
    TOPIC_AUTHORIZATION_FAILED = 29
    TOPIC_DELETION_DISABLED = 73
    TOPIC_EXCEPTION = 17
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
    TRANSACTION_COORDINATOR_FENCED = 52
    UNKNOWN = -1
    UNKNOWN_MEMBER_ID = 25
    UNKNOWN_PRODUCER_ID = 59
    UNKNOWN_TOPIC_OR_PART = 3
    UNSUPPORTED_COMPRESSION_TYPE = 74
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43
    UNSUPPORTED_SASL_MECHANISM = 33
    UNSUPPORTED_VERSION = 35
    _ALL_BROKERS_DOWN = -187
    _ASSIGN_PARTITIONS = -175
    _AUTHENTICATION = -169
    _BAD_COMPRESSION = -198
    _BAD_MSG = -199
    _CONFLICT = -173
    _CRIT_SYS_RESOURCE = -194
    _DESTROY = -197
    _EXISTING_SUBSCRIPTION = -176
    _FAIL = -196
    _FATAL = -150
    _FS = -189
    _GAPLESS_GUARANTEE = -148
    _INCONSISTENT = -149
    _INTR = -163
    _INVALID_ARG = -186
    _INVALID_TYPE = -154
    _IN_PROGRESS = -178
    _ISR_INSUFF = -183
    _KEY_DESERIALIZATION = -160
    _KEY_SERIALIZATION = -162
    _MAX_POLL_EXCEEDED = -147
    _MSG_TIMED_OUT = -192
    _NODE_UPDATE = -182
    _NOENT = -156
    _NOT_IMPLEMENTED = -170
    _NO_OFFSET = -168
    _OUTDATED = -167
    _PARTIAL = -158
    _PARTITION_EOF = -191
    _PREV_IN_PROGRESS = -177
    _PURGE_INFLIGHT = -151
    _PURGE_QUEUE = -152
    _QUEUE_FULL = -184
    _READ_ONLY = -157
    _RESOLVE = -193
    _RETRY = -153
    _REVOKE_PARTITIONS = -174
    _SSL = -181
    _STATE = -172
    _TIMED_OUT = -185
    _TIMED_OUT_QUEUE = -166
    _TRANSPORT = -195
    _UNDERFLOW = -155
    _UNKNOWN_GROUP = -179
    _UNKNOWN_PARTITION = -190
    _UNKNOWN_PROTOCOL = -171
    _UNKNOWN_TOPIC = -188
    _UNSUPPORTED_FEATURE = -165
    _VALUE_DESERIALIZATION = -159
    _VALUE_SERIALIZATION = -161
    _WAIT_CACHE = -164
    _WAIT_COORD = -180
