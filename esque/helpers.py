import functools
from concurrent.futures import Future, wait
from itertools import islice
from typing import List

import click
import pendulum
from confluent_kafka.cimpl import KafkaError, Message

from esque.errors import raise_for_kafka_error


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


def ensure_kafka_futures_done(future: List[Future]) -> Future:
    # Clients, such as confluents AdminClient, may return a done future with an exception
    done, failed = wait(future, timeout=15)
    assert len(failed) + len(done) == 1

    result = next(islice(done, 1)) if len(done) == 1 else next(islice(failed, 1))

    exception = result.exception()

    if exception is None:
        return result
    elif isinstance(exception, KafkaError):
        raise_for_kafka_error(exception)
    else:
        raise exception


def unpack_confluent_config(config):
    return {v.name: v.value for k, v in sorted(config.items())}


def delivery_callback(err: KafkaError, msg: Message):
    if err is not None:
        click.echo(f"EXCEPTION: When sending {msg.key()}: {err.str()}")


def delta_t(start: pendulum.DateTime) -> str:
    now = pendulum.now()
    return now.to_datetime_string() + " " + (pendulum.now() - start).in_words()
