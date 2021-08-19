from esque.errors import ExceptionWithMessage


class EsqueIOException(ExceptionWithMessage):
    """
    Base exception for the Esque IO system
    """

    pass


class EsqueIOEndOfSourceReached(EsqueIOException):
    """
    Exception raised when the handler reaches the current end of its message source.
    """

    pass


class EsqueIOTemporaryEndReached(EsqueIOEndOfSourceReached):
    """
    Exception raised when the handler's source is at a temporary end which means it could
    receive further messages at some point.
    For example the source is a Kafka topic with 10 messages and the source has reached the 10th message.
    At that point it has only reached a temporary end because it could be that after a while some producer puts more
    messages into the topic.
    """

    pass


class EsqueIOPermanentEndReached(EsqueIOEndOfSourceReached):
    """
    Exception raised when the handler's source is at a permanent end which means it cannot
    receive further messages.
    For example the source is a local file and the last record from the file has been read.
    """

    pass


class EsqueIOHandlerReadException(EsqueIOException):
    """
    Exception raised when the handler encounters issues while reading from its source.
    For example when it was reading from a pipe that broke or got closed prematurely.
    """

    pass


class EsqueIOConfigException(EsqueIOException):
    """
    General configuration-related exception
    """

    pass


class EsqueIOHandlerConfigException(EsqueIOConfigException):
    """
    Exception raised when the handler configuration is incomplete or invalid
    """

    pass


class EsqueIOSerializerConfigException(EsqueIOConfigException):
    """
    Exception raised when the serializer configuration is incomplete or invalid
    """

    pass


class EsqueIOSerializerConfigNotSupported(EsqueIOConfigException):
    """
    Exception raised when handler doesn't support persisting the serializer config
    """

    pass


class EsqueIONoSuchSchemaException(EsqueIOException):
    """
    Exception raised when :class:`SchemaRegistryClient` doesn't find the requested schema
    """

    pass


class EsqueIOInvalidPipelineBuilderState(EsqueIOException):
    """
    Exception raised when the pipeline builder configuration is incomplete or invalid when building a pipeline
    """

    pass
