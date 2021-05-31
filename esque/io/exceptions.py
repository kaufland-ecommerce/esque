from esque.errors import ExceptionWithMessage


class EsqueIOException(ExceptionWithMessage):
    pass


class EsqueIONoMessageLeft(EsqueIOException):
    pass


class EsqueIOConfigException(EsqueIOException):
    pass


class EsqueIOHandlerConfigException(EsqueIOConfigException):
    pass


class EsqueIOSerializerConfigException(EsqueIOConfigException):
    pass
