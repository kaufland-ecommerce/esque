import functools
import logging
from concurrent.futures import Future, wait
from itertools import islice
from typing import Type, TypeVar

import confluent_kafka
import pendulum
from confluent_kafka.cimpl import KafkaError

from esque.errors import FutureTimeoutException, raise_for_kafka_error

log = logging.getLogger(__name__)

T = TypeVar("T")


class SingletonMeta(type):
    __instance: T

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls.__instance = None

    def get_instance(cls: Type[T]) -> T:
        if cls.__instance is None:
            cls.set_instance(cls())
        return cls.__instance

    def set_instance(cls: Type[T], instance: T):
        cls.__instance = instance


def invalidate_cache_before(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        self._client.poll(timeout=1)
        return func(self, *args, **kwargs)

    return wrapper


def invalidate_cache_after(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        self.cluster.confluent_client.poll(timeout=1)
        return result

    return wrapper


def ensure_kafka_future_done(future: Future, timeout: int = 60 * 5) -> Future:
    # Clients, such as confluents AdminClient, may return a done future with an exception
    done, not_done = wait({future}, timeout=timeout)

    if not_done:
        raise FutureTimeoutException("Future timed out after {} seconds".format(timeout))

    result = next(islice(done, 1))

    exception = result.exception()

    if exception is None:
        return result
    elif isinstance(exception, confluent_kafka.KafkaException):
        raise_for_kafka_error(exception.args[0])
    else:
        raise exception


def unpack_confluent_config(config):
    return {v.name: v.value for k, v in sorted(config.items())}


def log_error(err: KafkaError):
    if err is not None:
        log.error(f"KafkaError occured: {err.str()}")


def delta_t(start: pendulum.DateTime) -> str:
    now = pendulum.now()
    return now.to_datetime_string() + " " + (pendulum.now() - start).in_words()
