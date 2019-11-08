import json
import random
import time
from concurrent.futures import Future
from pathlib import Path
from string import ascii_letters
from typing import Callable, Dict, Iterable, Tuple
from unittest import mock

import confluent_kafka
import pytest
import yaml
from click.testing import CliRunner
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Producer, Producer as ConfluenceProducer, TopicPartition
from pykafka.exceptions import NoBrokersAvailableError

from esque.cli.options import State
from esque.cluster import Cluster
from esque.config import Config, sample_config_path
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.errors import raise_for_kafka_error
from esque.messages.message import KafkaMessage, MessageHeader
from esque.resources.broker import Broker
from esque.resources.topic import Topic


def pytest_addoption(parser):
    parser.addoption("--integration", action="store_true", default=False, help="run integration tests")
    parser.addoption(
        "--local",
        action="store_true",
        default=False,
        help="run against the 'local' context of the sample config instead of the default 'docker' context for CI",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        # --run-e2e given in cli: do not skip e2e tests
        return
    integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(integration)


@pytest.fixture()
def interactive_cli_runner(test_config: Config):
    with mock.patch("esque.cli.helpers._isatty", return_value=True):
        yield CliRunner()


@pytest.fixture()
def non_interactive_cli_runner(test_config: Config):
    with mock.patch("esque.cli.helpers._isatty", return_value=False):
        yield CliRunner()


@pytest.fixture()
def test_config_path(mocker, tmpdir_factory):
    fn: Path = tmpdir_factory.mktemp("config").join("dummy.cfg")
    fn.write_text(sample_config_path().read_text(), encoding="UTF-8")
    mocker.patch("esque.config.config_path", return_value=fn)
    yield fn


@pytest.fixture()
def test_config(test_config_path, request):
    esque_config = Config()
    if request.config.getoption("--local"):
        esque_config.context_switch("local")
    yield esque_config


@pytest.fixture()
def topic_id(confluent_admin_client) -> str:
    yield "".join(random.choices(ascii_letters, k=5))
    topics = confluent_admin_client.list_topics(timeout=5).topics.keys()
    if topic_id in topics:
        confluent_admin_client.delete_topics([topic_id]).popitem()


@pytest.fixture()
def broker_id(state: State) -> str:
    brokers = Broker.get_all(state.cluster)
    yield str(brokers[0].broker_id)


@pytest.fixture()
def topic_object(cluster: Cluster, topic: str):
    yield cluster.topic_controller.get_cluster_topic(topic)


@pytest.fixture()
def changed_topic_object(cluster: Cluster, topic: str):
    yield Topic(topic, 1, 3, {"cleanup.policy": "compact"})


@pytest.fixture()
def topic(topic_factory: Callable[[int, str], Tuple[str, int]]) -> Iterable[str]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    for topic, _ in topic_factory(1, topic_id):
        yield topic


@pytest.fixture()
def topic_multiple_partitions(topic_factory: Callable[[int, str], Tuple[str, int]]) -> Iterable[str]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    for topic, _ in topic_factory(10, topic_id):
        yield topic


@pytest.fixture()
def source_topic(
    num_partitions: int, topic_factory: Callable[[int, str], Tuple[str, int]]
) -> Iterable[Tuple[str, int]]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    yield from topic_factory(num_partitions, topic_id)


@pytest.fixture()
def target_topic(
    num_partitions: int, topic_factory: Callable[[int, str], Tuple[str, int]]
) -> Iterable[Tuple[str, int]]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    yield from topic_factory(num_partitions, topic_id)


@pytest.fixture(params=[1, 10], ids=["num_partitions=1", "num_partitions=10"])
def num_partitions(request) -> int:
    return request.param


@pytest.fixture()
def topic_factory(confluent_admin_client: AdminClient) -> Callable[[int, str], Iterable[Tuple[str, int]]]:
    def factory(partitions: int, topic_id: str) -> Iterable[Tuple[str, int]]:
        future: Future = confluent_admin_client.create_topics(
            [NewTopic(topic_id, num_partitions=partitions, replication_factor=1)]
        )[topic_id]
        while not future.done() or future.cancelled():
            if future.result():
                raise RuntimeError
        confluent_admin_client.poll(timeout=1)

        yield (topic_id, partitions)

        confluent_admin_client.delete_topics([topic_id]).popitem()

    return factory


@pytest.fixture()
def topic_controller(cluster: Cluster):
    yield cluster.topic_controller


@pytest.fixture()
def messages_ordered_same_partition() -> Iterable[KafkaMessage]:
    ordered_messages = [
        KafkaMessage(key="j", value="v1", partition=0),
        KafkaMessage(key="i", value="v2", partition=0),
        KafkaMessage(key="h", value="v3", partition=0),
        KafkaMessage(key="g", value="v4", partition=0),
        KafkaMessage(key="f", value="v5", partition=0),
        KafkaMessage(key="e", value="v6", partition=0),
        KafkaMessage(key="d", value="v7", partition=0),
        KafkaMessage(key="c", value="v8", partition=0),
        KafkaMessage(key="b", value="v9", partition=0),
        KafkaMessage(key="a", value="v10", partition=0),
    ]
    yield ordered_messages


@pytest.fixture()
def messages_ordered_same_partition_with_headers() -> Iterable[KafkaMessage]:
    ordered_messages = [
        KafkaMessage(key="j", value="v1", partition=0, headers=[MessageHeader(key="hk1", value="hv1")]),
        KafkaMessage(key="i", value="v2", partition=0, headers=[MessageHeader(key="hk2", value=None)]),
        KafkaMessage(key="h", value="v3", partition=0, headers=[MessageHeader(key="hk3", value="hv3")]),
        KafkaMessage(key="g", value="v4", partition=0, headers=[MessageHeader(key="hk4", value=None)]),
        KafkaMessage(key="f", value="v5", partition=0, headers=[MessageHeader(key="hk5", value="hv5")]),
        KafkaMessage(key="e", value="v6", partition=0, headers=[MessageHeader(key="hk6", value="hv6")]),
        KafkaMessage(key="d", value="v7", partition=0, headers=[MessageHeader(key="hk7", value="hv7")]),
        KafkaMessage(key="c", value="v8", partition=0, headers=[MessageHeader(key="hk8", value="hv8")]),
        KafkaMessage(key="b", value="v9", partition=0, headers=[MessageHeader(key="hk9", value="hv9")]),
        KafkaMessage(key="a", value="v10", partition=0, headers=[MessageHeader(key="hk10", value="hv10")]),
    ]
    yield ordered_messages


@pytest.fixture()
def messages_ordered_different_partitions() -> Iterable[KafkaMessage]:
    ordered_messages = [
        KafkaMessage(key="j", value="v1", partition=0),
        KafkaMessage(key="i", value="v2", partition=1),
        KafkaMessage(key="h", value="v3", partition=2),
        KafkaMessage(key="g", value="v4", partition=3),
        KafkaMessage(key="f", value="v5", partition=2),
        KafkaMessage(key="e", value="v6", partition=1),
        KafkaMessage(key="d", value="v7", partition=0),
        KafkaMessage(key="c", value="v8", partition=2),
        KafkaMessage(key="b", value="v9", partition=3),
        KafkaMessage(key="a", value="v10", partition=1),
    ]
    yield ordered_messages


@pytest.fixture()
def messages_ordered_different_partition_with_headers() -> Iterable[KafkaMessage]:
    ordered_messages = [
        KafkaMessage(key="j", value="v1", partition=0, headers=[MessageHeader(key="hk1", value="hv1")]),
        KafkaMessage(key="i", value="v2", partition=1, headers=[MessageHeader(key="hk2", value=None)]),
        KafkaMessage(key="h", value="v3", partition=2, headers=[MessageHeader(key="hk3", value="hv3")]),
        KafkaMessage(key="g", value="v4", partition=3, headers=[MessageHeader(key="hk4", value=None)]),
        KafkaMessage(key="f", value="v5", partition=2, headers=[MessageHeader(key="hk5", value="hv5")]),
        KafkaMessage(key="e", value="v6", partition=1, headers=[MessageHeader(key="hk6", value="hv6")]),
        KafkaMessage(key="d", value="v7", partition=0, headers=[MessageHeader(key="hk7", value="hv7")]),
        KafkaMessage(key="c", value="v8", partition=2, headers=[MessageHeader(key="hk8", value="hv8")]),
        KafkaMessage(key="b", value="v9", partition=3, headers=[MessageHeader(key="hk9", value="hv9")]),
        KafkaMessage(key="a", value="v10", partition=1, headers=[MessageHeader(key="hk10", value="hv10")]),
    ]
    yield ordered_messages


@pytest.fixture()
def produced_messages_different_partitions(messages_ordered_different_partitions: Iterable[KafkaMessage]):
    def _produce(topic_name: str, producer: ConfluenceProducer):
        for message in messages_ordered_different_partitions:
            producer.produce(topic=topic_name, key=message.key, value=message.value, partition=message.partition)
            time.sleep(0.5)
            producer.flush()

    return _produce


@pytest.fixture()
def produced_messages_different_partitions_with_headers(
    messages_ordered_different_partition_with_headers: Iterable[KafkaMessage]
):
    def _produce(topic_name: str, producer: ConfluenceProducer):
        for message in messages_ordered_different_partition_with_headers:
            producer.produce(
                topic=topic_name,
                key=message.key,
                value=message.value,
                partition=message.partition,
                headers=message.headers,
            )
            time.sleep(0.5)
            producer.flush()

    return _produce


@pytest.fixture()
def produced_messages_same_partition(messages_ordered_same_partition: Iterable[KafkaMessage]):
    def _produce(topic_name: str, producer: ConfluenceProducer):
        for message in messages_ordered_same_partition:
            producer.produce(topic=topic_name, key=message.key, value=message.value, partition=message.partition)
            time.sleep(0.5)
            producer.flush()

    return _produce


@pytest.fixture()
def produced_messages_same_partition_with_headers(
    messages_ordered_same_partition_with_headers: Iterable[KafkaMessage]
):
    def _produce(topic_name: str, producer: ConfluenceProducer):
        for message in messages_ordered_same_partition_with_headers:
            producer.produce(
                topic=topic_name,
                key=message.key,
                value=message.value,
                partition=message.partition,
                headers=message.headers,
            )
            time.sleep(0.5)
            producer.flush()

    return _produce


@pytest.fixture()
def produced_avro_messages_with_headers(messages_ordered_same_partition_with_headers: Iterable[KafkaMessage]):
    def _produce(topic_name: str, producer: AvroProducer):
        for message in messages_ordered_same_partition_with_headers:
            producer.produce(
                topic=topic_name,
                key=message.key,
                value=message.value,
                partition=message.partition,
                headers=message.headers,
            )
            time.sleep(0.5)
            producer.flush()

    return _produce


@pytest.fixture()
def confluent_admin_client(test_config: Config) -> AdminClient:
    admin = AdminClient(test_config.create_confluent_config())
    admin.poll(timeout=5)
    yield admin


@pytest.fixture()
def producer(test_config: Config):
    producer_config = test_config.create_confluent_config()
    yield Producer(producer_config)


@pytest.fixture()
def avro_producer(test_config: Config):
    producer_config = test_config.create_confluent_config()
    producer_config.update({"schema.registry.url": Config().schema_registry})
    yield AvroProducer(producer_config)


@pytest.fixture()
def consumergroup_controller(cluster: Cluster):
    yield ConsumerGroupController(cluster)


@pytest.fixture()
def consumergroup_instance(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    yield consumergroup_controller.get_consumergroup(partly_read_consumer_group)


@pytest.fixture()
def consumer_group():
    yield "".join(random.choices(ascii_letters, k=5))


@pytest.fixture()
def target_consumer_group():
    yield "".join(random.choices(ascii_letters, k=5))


@pytest.fixture()
def consumer(topic_object: Topic, consumer_group: str):
    _config = Config().create_confluent_config()
    _config.update(
        {
            "group.id": consumer_group,
            "error_cb": raise_for_kafka_error,
            # We need to commit offsets manually once we"re sure it got saved
            # to the sink
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            # We need this to start at the last committed offset instead of the
            # latest when subscribing for the first time
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
    )
    _consumer = confluent_kafka.Consumer(_config)
    _consumer.assign([TopicPartition(topic=topic_object.name, partition=0, offset=0)])
    yield _consumer


@pytest.fixture()
def filled_topic(producer, topic_object):
    for _ in range(10):
        random_value = "".join(random.choices(ascii_letters, k=5)).encode("utf-8")
        producer.produce(topic=topic_object.name, key=random_value, value=random_value)
        producer.flush()
    yield topic_object


@pytest.fixture()
def partly_read_consumer_group(consumer: confluent_kafka.Consumer, filled_topic, consumer_group):
    for i in range(5):
        msg = consumer.consume(timeout=10)[0]
        consumer.commit(msg, asynchronous=False)
    yield consumer_group


@pytest.fixture()
def cluster(test_config: Config) -> Iterable[Cluster]:
    try:
        cluster = Cluster()
    except NoBrokersAvailableError as ex:
        print(test_config.bootstrap_servers)
        raise ex

    yield cluster


@pytest.fixture()
def state(test_config: Config) -> Iterable[State]:
    yield State()


def check_and_load_yaml(output: str) -> Dict:
    assert output[0] != "{", "non json output starts with '{'"
    assert output[-2] != "}" and output[-1] != "}", "non json output ends with '}'"
    return yaml.safe_load(output)


FORMATS_AND_LOADERS = [("yaml", check_and_load_yaml), ("json", json.loads)]

parameterized_output_formats = pytest.mark.parametrize(
    "output_format,loader", FORMATS_AND_LOADERS, ids=["yaml", "json"]
)
