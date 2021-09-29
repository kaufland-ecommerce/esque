class StreamEvent:
    """
    An event that happened on a stream.
    Check :attr:`StreamEvent.partition` to see which partition this event occurred on.
    If the value is equal to :attr:`StreamEvent.ALL_PARTITIONS` then this is a global event that refers to all
    partitions or it is an event that is not applicable to any specific partition.
    """

    ALL_PARTITIONS: int = -1

    def __init__(self, msg: str, partition: int = ALL_PARTITIONS):
        self._msg = msg
        self.partition = partition

    def __repr__(self) -> str:
        return f"{type(self).__name__}(msg={self._msg!r}, partition={self.partition})"

    def __eq__(self, other):
        if not isinstance(other, StreamEvent):
            return NotImplemented
        return type(self) == type(other) and self._msg == other._msg and self.partition == other.partition


class NthMessageRead(StreamEvent):
    """
    Stream Event indicating that the desired amount of messages has been read.
    """


class EndOfStream(StreamEvent):
    """
    Stream Event indicating that the handler reached a (possibly temporary) end of its message source.
    """


class PermanentEndOfStream(EndOfStream):
    """
    Stream event indicating that the handler's source is at a permanent end which means it cannot
    receive further messages.
    For example the source is a local file and the last record from the file has been read.
    This stream event is always received right before the end of a message stream.
    """


class TemporaryEndOfPartition(EndOfStream):
    """
    Stream event indicating that the handler's source is at a temporary end which means it could
    receive further messages at some point.
    For example the source is a Kafka topic with 10 messages and the source has reached the 10th message.
    At that point it has only reached a temporary end because it could be that after a while some producer puts more
    messages into the topic.
    """
