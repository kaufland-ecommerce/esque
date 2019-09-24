from enum import Enum
from typing import Dict, Tuple

_ERROR_METADATA: Dict[int, Tuple[bool, str]] = {
    -1: (False, "The server experienced an unexpected error when processing the request."),
    0: (False, "No Error"),
    1: (False, "The requested offset is not within the range of offsets maintained by the server."),
    2: (
        True,
        "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, "
        "or is otherwise corrupt.",
    ),
    3: (True, "This server does not host this topic-partition."),
    4: (False, "The requested fetch size is invalid."),
    5: (True, "There is no leader for this topic-partition as we are in the middle of a leadership election."),
    6: (True, "This server is not the leader for that topic-partition."),
    7: (True, "The request timed out."),
    8: (False, "The broker is not available."),
    9: (False, "The replica is not available for the requested topic-partition."),
    10: (False, "The request included a message larger than the max message size the server will accept."),
    11: (False, "The controller moved to another broker."),
    12: (False, "The metadata field of the offset request was too large."),
    13: (True, "The server disconnected before a response was received."),
    14: (True, "The coordinator is loading and hence can't process requests."),
    15: (True, "The coordinator is not available."),
    16: (True, "This is not the correct coordinator."),
    17: (False, "The request attempted to perform an operation on an invalid topic."),
    18: (False, "The request included message batch larger than the configured segment size on the server."),
    19: (True, "Messages are rejected since there are fewer in-sync replicas than required."),
    20: (True, "Messages are written to the log, but to fewer in-sync replicas than required."),
    21: (False, "Produce request specified an invalid value for required acks."),
    22: (False, "Specified group generation id is not valid."),
    23: (
        False,
        "The group member's supported protocols are incompatible with those of existing members or first group "
        "member tried to join with empty protocol type or empty protocol list.",
    ),
    24: (False, "The configured groupId is invalid."),
    25: (False, "The coordinator is not aware of this member."),
    26: (
        False,
        "The session timeout is not within the range allowed by the broker (as configured by "
        "group.min.session.timeout.ms and group.max.session.timeout.ms).",
    ),
    27: (False, "The group is rebalancing, so a rejoin is needed."),
    28: (False, "The committing offset data size is not valid."),
    29: (False, "Not authorized to access topics: [Topic authorization failed.]"),
    30: (False, "Not authorized to access group: Group authorization failed."),
    31: (False, "Cluster authorization failed."),
    32: (False, "The timestamp of the message is out of acceptable range."),
    33: (False, "The broker does not support the requested SASL mechanism."),
    34: (False, "Request is not valid given the current SASL state."),
    35: (False, "The version of API is not supported."),
    36: (False, "Topic with this name already exists."),
    37: (False, "Number of partitions is below 1."),
    38: (False, "Replication factor is below 1 or larger than the number of available brokers."),
    39: (False, "Replica assignment is invalid."),
    40: (False, "Configuration is invalid."),
    41: (True, "This is not the correct controller for this cluster."),
    42: (
        False,
        "This most likely occurs because of a request being malformed by the client library or the message was "
        "sent to an incompatible broker. See the broker logs for more details.",
    ),
    43: (False, "The message format version on the broker does not support the request."),
    44: (False, "Request parameters do not satisfy the configured policy."),
    45: (False, "The broker received an out of order sequence number."),
    46: (False, "The broker received a duplicate sequence number."),
    47: (
        False,
        "Producer attempted an operation with an old epoch. Either there is a newer producer with the same "
        "transactionalId, or the producer's transaction has been expired by the broker.",
    ),
    48: (False, "The producer attempted a transactional operation in an invalid state."),
    49: (
        False,
        "The producer attempted to use a producer id which is not currently assigned to its transactional id.",
    ),
    50: (
        False,
        "The transaction timeout is larger than the maximum value allowed by the broker (as configured by "
        "transaction.max.timeout.ms).",
    ),
    51: (
        False,
        "The producer attempted to update a transaction while another concurrent operation on the same "
        "transaction was ongoing.",
    ),
    52: (
        False,
        "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator "
        "for a given producer.",
    ),
    53: (False, "Transactional Id authorization failed."),
    54: (False, "Security features are disabled."),
    55: (
        False,
        "The broker did not attempt to execute this operation. This may happen for batched RPCs where some "
        "operations in the batch failed, causing the broker to respond without trying the rest.",
    ),
    56: (True, "Disk error when trying to access log file on the disk."),
    57: (False, "The user-specified log directory is not found in the broker config."),
    58: (False, "SASL Authentication failed."),
    59: (
        False,
        "This exception is raised by the broker if it could not locate the producer metadata associated with the "
        "producerId in question. This could happen if, for instance, the producer's records were deleted because "
        "their retention time had elapsed. Once the last records of the producerId are removed, the producer's "
        "metadata is removed from the broker, and future appends by the producer will return this exception.",
    ),
    60: (False, "A partition reassignment is in progress."),
    61: (False, "Delegation Token feature is not enabled."),
    62: (False, "Delegation Token is not found on server."),
    63: (False, "Specified Principal is not valid Owner/Renewer."),
    64: (
        False,
        "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token "
        "authenticated channels.",
    ),
    65: (False, "Delegation Token authorization failed."),
    66: (False, "Delegation Token is expired."),
    67: (False, "Supplied principalType is not supported."),
    68: (False, "The group is not empty."),
    69: (False, "The group id does not exist."),
    70: (True, "The fetch session ID was not found."),
    71: (True, "The fetch session epoch is invalid."),
    72: (
        True,
        "There is no listener on the leader broker that matches the listener on which metadata request was "
        "processed.",
    ),
    73: (False, "Topic deletion is disabled."),
    74: (True, "The leader epoch in the request is older than the epoch on the broker"),
    75: (True, "The leader epoch in the request is newer than the epoch on the broker"),
    76: (False, "The requesting client does not support the compression type of given partition."),
    77: (False, "Broker epoch has changed"),
    78: (
        True,
        "The leader high watermark has not caught up from a recent leader election so the offsets cannot be "
        "guaranteed to be monotonically increasing",
    ),
    79: (False, "The group member needs to have a valid member id before actually entering a consumer group"),
    80: (True, "The preferred leader was not available"),
    81: (
        False,
        "Consumer group The consumer group has reached its max size. already has the configured maximum number of"
        " members.",
    ),
}


class ErrorCode(Enum):
    UNKNOWN_SERVER_ERROR = -1
    NONE = 0
    OFFSET_OUT_OF_RANGE = 1
    CORRUPT_MESSAGE = 2
    UNKNOWN_TOPIC_OR_PARTITION = 3
    INVALID_FETCH_SIZE = 4
    LEADER_NOT_AVAILABLE = 5
    NOT_LEADER_FOR_PARTITION = 6
    REQUEST_TIMED_OUT = 7
    BROKER_NOT_AVAILABLE = 8
    REPLICA_NOT_AVAILABLE = 9
    MESSAGE_TOO_LARGE = 10
    STALE_CONTROLLER_EPOCH = 11
    OFFSET_METADATA_TOO_LARGE = 12
    NETWORK_EXCEPTION = 13
    COORDINATOR_LOAD_IN_PROGRESS = 14
    COORDINATOR_NOT_AVAILABLE = 15
    NOT_COORDINATOR = 16
    INVALID_TOPIC_EXCEPTION = 17
    RECORD_LIST_TOO_LARGE = 18
    NOT_ENOUGH_REPLICAS = 19
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
    INVALID_REQUIRED_ACKS = 21
    ILLEGAL_GENERATION = 22
    INCONSISTENT_GROUP_PROTOCOL = 23
    INVALID_GROUP_ID = 24
    UNKNOWN_MEMBER_ID = 25
    INVALID_SESSION_TIMEOUT = 26
    REBALANCE_IN_PROGRESS = 27
    INVALID_COMMIT_OFFSET_SIZE = 28
    TOPIC_AUTHORIZATION_FAILED = 29
    GROUP_AUTHORIZATION_FAILED = 30
    CLUSTER_AUTHORIZATION_FAILED = 31
    INVALID_TIMESTAMP = 32
    UNSUPPORTED_SASL_MECHANISM = 33
    ILLEGAL_SASL_STATE = 34
    UNSUPPORTED_VERSION = 35
    TOPIC_ALREADY_EXISTS = 36
    INVALID_PARTITIONS = 37
    INVALID_REPLICATION_FACTOR = 38
    INVALID_REPLICA_ASSIGNMENT = 39
    INVALID_CONFIG = 40
    NOT_CONTROLLER = 41
    INVALID_REQUEST = 42
    UNSUPPORTED_FOR_MESSAGE_FORMAT = 43
    POLICY_VIOLATION = 44
    OUT_OF_ORDER_SEQUENCE_NUMBER = 45
    DUPLICATE_SEQUENCE_NUMBER = 46
    INVALID_PRODUCER_EPOCH = 47
    INVALID_TXN_STATE = 48
    INVALID_PRODUCER_ID_MAPPING = 49
    INVALID_TRANSACTION_TIMEOUT = 50
    CONCURRENT_TRANSACTIONS = 51
    TRANSACTION_COORDINATOR_FENCED = 52
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53
    SECURITY_DISABLED = 54
    OPERATION_NOT_ATTEMPTED = 55
    KAFKA_STORAGE_ERROR = 56
    LOG_DIR_NOT_FOUND = 57
    SASL_AUTHENTICATION_FAILED = 58
    UNKNOWN_PRODUCER_ID = 59
    REASSIGNMENT_IN_PROGRESS = 60
    DELEGATION_TOKEN_AUTH_DISABLED = 61
    DELEGATION_TOKEN_NOT_FOUND = 62
    DELEGATION_TOKEN_OWNER_MISMATCH = 63
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64
    DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65
    DELEGATION_TOKEN_EXPIRED = 66
    INVALID_PRINCIPAL_TYPE = 67
    NON_EMPTY_GROUP = 68
    GROUP_ID_NOT_FOUND = 69
    FETCH_SESSION_ID_NOT_FOUND = 70
    INVALID_FETCH_SESSION_EPOCH = 71
    LISTENER_NOT_FOUND = 72
    TOPIC_DELETION_DISABLED = 73
    FENCED_LEADER_EPOCH = 74
    UNKNOWN_LEADER_EPOCH = 75
    UNSUPPORTED_COMPRESSION_TYPE = 76
    STALE_BROKER_EPOCH = 77
    OFFSET_NOT_AVAILABLE = 78
    MEMBER_ID_REQUIRED = 79
    PREFERRED_LEADER_NOT_AVAILABLE = 80
    GROUP_MAX_SIZE_REACHED = 81

    def __init__(self, code: int):
        retryable, description = _ERROR_METADATA[code]
        self.retryable = retryable
        self.description = description
