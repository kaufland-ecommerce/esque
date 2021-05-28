import logging
from concurrent.futures import Future, wait
from typing import List, Type, TypeVar

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


def ensure_kafka_future_done(future: Future, timeout: int = 60 * 5) -> Future:
    return ensure_kafka_futures_done(futures=[future], timeout=timeout)[0]


def ensure_kafka_futures_done(futures: List[Future], timeout: int = 60 * 5) -> List[Future]:
    # Clients, such as confluents AdminClient, may return a done future with an exception
    done, not_done = wait(futures, timeout=timeout)

    if not_done:
        raise FutureTimeoutException("{} future(s) timed out after {} seconds".format(len(not_done), timeout))
    for result in list(done):
        exception = result.exception()
        if exception is None:
            continue
        if isinstance(exception, confluent_kafka.KafkaException):
            raise_for_kafka_error(exception.args[0])
        elif isinstance(exception, BaseException):
            raise exception
    return list(done)


def unpack_confluent_config(config):
    return {v.name: v.value for k, v in sorted(config.items())}


def log_error(err: KafkaError):
    if err is not None:
        log.error(f"KafkaError occured: {err.str()}")


def delta_t(start: pendulum.DateTime) -> str:
    now = pendulum.now()
    return now.to_datetime_string() + " " + (pendulum.now() - start).in_words()
