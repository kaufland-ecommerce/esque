from esque.errors import ExceptionWithMessage


class EsqueIOException(ExceptionWithMessage):
    """
    Base exception for the Esque IO system
    """


class EsqueIOHandlerReadException(EsqueIOException):
    """
    Exception raised when the handler encounters issues while reading from its source.
    For example when it was reading from a pipe that broke or got closed prematurely.
    """


class EsqueIOConfigException(EsqueIOException):
    """
    General configuration-related exception
    """


class EsqueIOHandlerConfigException(EsqueIOConfigException):
    """
    Exception raised when the handler configuration is incomplete or invalid
    """


class EsqueIOSerializerConfigException(EsqueIOConfigException):
    """
    Exception raised when the serializer configuration is incomplete or invalid
    """


class EsqueIOSerializerConfigNotSupported(EsqueIOConfigException):
    """
    Exception raised when handler doesn't support persisting the serializer config
    """


class EsqueIONoSuchSchemaException(EsqueIOException):
    """
    Exception raised when :class:`SchemaRegistryClient` doesn't find the requested schema
    """


class EsqueIOInvalidPipelineBuilderState(EsqueIOException):
    """
    Exception raised when the pipeline builder configuration is incomplete or invalid when building a pipeline
    """
