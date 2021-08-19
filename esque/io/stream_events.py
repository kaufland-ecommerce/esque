class StreamEvent:
    def __init__(self, msg: str):
        self._msg = msg

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._msg!r})"


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


class TemporaryEndOfStream(EndOfStream):
    """
    Stream event indicating that the handler's source is at a temporary end which means it could
    receive further messages at some point.
    For example the source is a Kafka topic with 10 messages and the source has reached the 10th message.
    At that point it has only reached a temporary end because it could be that after a while some producer puts more
    messages into the topic.
    """
