import json
import os
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
from _pytest.fixtures import FixtureRequest
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import KafkaError, KafkaException
from confluent_kafka.cimpl import Producer as ConfluentProducer
from confluent_kafka.cimpl import TopicPartition
from kafka.errors import NoBrokersAvailable

import esque.config
from esque.cli.options import State
from esque.cluster import Cluster
from esque.config import Config
from esque.config.migration import CURRENT_VERSION
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.helpers import log_error
from esque.messages.message import KafkaMessage, MessageHeader
from esque.resources.broker import Broker
from esque.resources.topic import Topic

# constants that indicate which config version to load
# see function get_path_for_config_version
LOAD_SAMPLE_CONFIG = -1
LOAD_INTEGRATION_TEST_CONFIG = -2
LOAD_BROKEN_CONFIG = -3
LOAD_CURRENT_VERSION = CURRENT_VERSION


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


# use for typing only
def config_loader(config_version: int = CURRENT_VERSION) -> Tuple[Path, str]:
    ...


@pytest.fixture(scope="function")
def load_config(mocker: mock, tmp_path: Path) -> config_loader:
    """
    Loads config of the given version or key into a temporary directory and set this directory as esque config
    directory.
    Available keys are `LOAD_INTEGRATION_TEST_CONFIG` and `LOAD_SAMPLE_CONFIG`.
    It will delete all other configs found in that directory.

    :param config_version: version or config key to load
    :type config_version: int
    :return: Tuple[NamedTemporaryFile, str] where str is the content of the config and the tempfile
    """
    mocker.patch("esque.config._config_dir", return_value=tmp_path)

    def loader(config_version: int = LOAD_CURRENT_VERSION) -> Tuple[Path, str]:
        for file in tmp_path.glob("*"):
            os.remove(str(file.resolve()))
        original_path = get_path_for_config_version(config_version)
        data = original_path.read_text()
        if config_version == 0:
            config_file = tmp_path / "esque.cfg"
        else:
            config_file = tmp_path / "esque_config.yaml"
        config_file.write_text(data)
        return config_file, data

    return loader


def get_path_for_config_version(config_version: int) -> Path:
    base_path = Path(__file__).parent / "test_configs"
    if config_version == 0:
        return base_path / "v0_sample.cfg"
    if config_version == LOAD_INTEGRATION_TEST_CONFIG:
        return base_path / "integration_test_config.yaml"
    if config_version == LOAD_SAMPLE_CONFIG:
        return esque.config.sample_config_path()
    if config_version == LOAD_BROKEN_CONFIG:
        return base_path / "broken_test_config.yaml"
    return base_path / f"v{config_version}_sample.yaml"


@pytest.fixture()
def broken_test_config(load_config: config_loader) -> Config:
    conffile, _ = load_config(LOAD_BROKEN_CONFIG)
    esque_config = Config.get_instance()
    return esque_config


@pytest.fixture()
def unittest_config(request: FixtureRequest, load_config: config_loader) -> Config:
    conffile, _ = load_config(LOAD_INTEGRATION_TEST_CONFIG)
    esque_config = Config.get_instance()
    if request.config.getoption("--local"):
        esque_config.context_switch("local")
    return esque_config


@pytest.fixture()
def topic_id(confluent_admin_client) -> str:
    return "".join(random.choices(ascii_letters, k=5))


@pytest.fixture()
def broker_id(state: State) -> str:
    brokers = Broker.get_all(state.cluster)
    return str(brokers[0].broker_id)


@pytest.fixture()
def broker_host(state: State) -> str:
    brokers = Broker.get_all(state.cluster)
    return brokers[0].host


@pytest.fixture()
def broker_host_and_port(state: State) -> str:
    brokers = Broker.get_all(state.cluster)
    return "{}:{}".format(brokers[0].host, brokers[0].port)


@pytest.fixture()
def topic_object(cluster: Cluster, topic: str):
    yield cluster.topic_controller.get_cluster_topic(topic)


@pytest.fixture()
def changed_topic_object(cluster: Cluster, topic: str):
    yield Topic(topic, 1, 3, {"cleanup.policy": "compact"})


@pytest.fixture()
def topic(topic_factory: Callable[[int, str], Tuple[str, int]]) -> Iterable[str]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    topic, _ = topic_factory(1, topic_id)
    return topic


@pytest.fixture()
def topic_multiple_partitions(topic_factory: Callable[[int, str], Tuple[str, int]]) -> Iterable[str]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    topic, _ = topic_factory(10, topic_id)
    return topic


@pytest.fixture()
def source_topic(num_partitions: int, topic_factory: Callable[[int, str], Tuple[str, int]]) -> Tuple[str, int]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    return topic_factory(num_partitions, topic_id)


@pytest.fixture()
def target_topic(num_partitions: int, topic_factory: Callable[[int, str], Tuple[str, int]]) -> Tuple[str, int]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    return topic_factory(num_partitions, topic_id)


@pytest.fixture(params=[1, 10], ids=["1_partition", "10_partitions"])
def num_partitions(request) -> int:
    return request.param


@pytest.fixture()
def topic_factory(confluent_admin_client: AdminClient) -> Callable[[int, str], Tuple[str, int]]:
    created_topics = []

    def factory(partitions: int, topic_id: str) -> Tuple[str, int]:
        future: Future = confluent_admin_client.create_topics(
            [NewTopic(topic_id, num_partitions=partitions, replication_factor=1)]
        )[topic_id]
        created_topics.append(topic_id)
        while not future.done() or future.cancelled():
            if future.result():
                raise RuntimeError
        for _ in range(80):
            topic_data = confluent_admin_client.list_topics(topic=topic_id).topics[topic_id]
            if topic_data.error is None:
                break
            time.sleep(0.125)
        else:
            raise RuntimeError(f"Couldn't create topic {topic_id}")
        return topic_id, partitions

    yield factory

    if created_topics:
        for topic, future in confluent_admin_client.delete_topics(created_topics).items():
            while not future.done() or future.cancelled():
                try:
                    future.result()
                except KafkaException as e:
                    kafka_error: KafkaError = e.args[0]
                    if kafka_error.code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                        raise


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
    def _produce(topic_name: str, producer: ConfluentProducer):
        for message in messages_ordered_different_partitions:
            producer.produce(topic=topic_name, key=message.key, value=message.value, partition=message.partition)
            time.sleep(0.5)
            producer.flush()

    return _produce


@pytest.fixture()
def produced_messages_different_partitions_with_headers(
    messages_ordered_different_partition_with_headers: Iterable[KafkaMessage],
):
    def _produce(topic_name: str, producer: ConfluentProducer):
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
    def _produce(topic_name: str, producer: ConfluentProducer, sleep_time: float = 0.5):
        for message in messages_ordered_same_partition:
            producer.produce(topic=topic_name, key=message.key, value=message.value, partition=message.partition)
            time.sleep(sleep_time)
            producer.flush()

    return _produce


@pytest.fixture()
def produced_messages_same_partition_with_headers(
    messages_ordered_same_partition_with_headers: Iterable[KafkaMessage],
):
    def _produce(topic_name: str, producer: ConfluentProducer):
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
def confluent_admin_client(unittest_config) -> AdminClient:
    admin = AdminClient({"topic.metadata.refresh.interval.ms": "250", **unittest_config.create_confluent_config()})
    return admin


@pytest.fixture()
def producer(unittest_config) -> ConfluentProducer:
    producer_config = unittest_config.create_confluent_config()
    yield ConfluentProducer(producer_config)


@pytest.fixture()
def avro_producer(unittest_config) -> AvroProducer:
    producer_config = unittest_config.create_confluent_config(include_schema_registry=True)
    yield AvroProducer(producer_config)


@pytest.fixture()
def consumergroup_controller(cluster: Cluster):
    yield ConsumerGroupController(cluster)


@pytest.fixture()
def consumergroup_instance(partly_read_consumer_group: str, consumergroup_controller: ConsumerGroupController):
    yield consumergroup_controller.get_consumer_group(partly_read_consumer_group)


@pytest.fixture()
def consumer_group():
    yield "".join(random.choices(ascii_letters, k=5))


@pytest.fixture()
def target_consumer_group():
    yield "".join(random.choices(ascii_letters, k=5))


@pytest.fixture()
def consumer(topic_object: Topic, consumer_group: str, unittest_config: Config):
    _config = unittest_config.create_confluent_config()
    _config.update(
        {
            "group.id": consumer_group,
            "error_cb": log_error,
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
    for _ in range(5):
        msg = consumer.consume(timeout=10)[0]
        consumer.commit(msg, asynchronous=False)
    yield consumer_group


@pytest.fixture()
def cluster(unittest_config) -> Iterable[Cluster]:
    try:
        cluster = Cluster()
    except NoBrokersAvailable as ex:
        print(unittest_config.bootstrap_servers)
        raise ex

    yield cluster


@pytest.fixture()
def state(unittest_config) -> State:
    return State()


def check_and_load_yaml(output: str) -> Dict:
    assert output[0] != "{", "non json output starts with '{'"
    assert output[-2] != "}" and output[-1] != "}", "non json output ends with '}'"
    return yaml.safe_load(output)


FORMATS_AND_LOADERS = [("yaml", check_and_load_yaml), ("json", json.loads)]

parameterized_output_formats = pytest.mark.parametrize(
    "output_format,loader", FORMATS_AND_LOADERS, ids=["yaml_output", "json_output"]
)
