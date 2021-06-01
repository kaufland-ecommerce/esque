from esque.errors import ExceptionWithMessage


class EsqueIOException(ExceptionWithMessage):
    """
    Base exception for the Esque IO system
    """

    pass


class EsqueIONoMessageLeft(EsqueIOException):
    """
    Exception raised when the handler reaches the end of message source
    """

    pass


class EsqueIOHandlerReadException(EsqueIOException):
    """
    Exception raised when the handler encounters issues while reading from its source
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


class EsqueIOSerializerSettingsNotSupported(EsqueIOConfigException):
    """
    Exception raised when handler doesn't support persisting serializer settings
    """

    pass
