import random
from concurrent.futures import Future
from pathlib import Path
from string import ascii_letters
from typing import Iterable, Tuple, Callable

import confluent_kafka
import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import Producer, TopicPartition
from pykafka.exceptions import NoBrokersAvailableError

from esque.cluster import Cluster
from esque.config import Config, sample_config_path
from esque.errors import raise_for_kafka_error
from esque.topic import Topic


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
def topic_object(cluster, topic):
    yield cluster.topic_controller.get_cluster_topic(topic)


@pytest.fixture()
def changed_topic_object(cluster, topic):
    yield Topic(topic, 1, 3, {"cleanup.policy": "compact"})


@pytest.fixture()
def topic(topic_factory: Callable[[int, str], Tuple[str, int]]) -> Iterable[str]:
    topic_id = "".join(random.choices(ascii_letters, k=5))
    for topic, _ in topic_factory(1, topic_id):
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
def topic_controller(cluster):
    yield cluster.topic_controller


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
    yield cluster.consumergroup_controller


@pytest.fixture
def random_id() -> str:
    return "".join(random.choices(ascii_letters, k=5))


@pytest.fixture
def consumed_topic(topic_object: Topic, producer: Producer) -> Tuple[str, str, int, int]:
    total, consumed = 12, 5

    def rand_str(x):
        return "".join(random.choices(ascii_letters, k=x))

    consumer_group_id = rand_str(5)

    for _ in range(total):
        random_value = rand_str(5).encode("utf-8")
        producer.produce(topic=topic_object.name, key=random_value, value=random_value)
        producer.flush()

    consumer = consumer_with_id(consumer_group_id)
    consumer.assign([TopicPartition(topic=topic_object.name, partition=0, offset=0)])

    for i in range(consumed):
        msg = consumer.consume(timeout=10)[0]
        consumer.commit(msg, asynchronous=False)
    return consumer_group_id, topic_object.name, total, consumed


def consumer_with_id(group_id: str) -> confluent_kafka.Consumer:
    config = Config().create_confluent_config()
    config.update(
        {
            "group.id": group_id,
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
    consumer = confluent_kafka.Consumer(config)
    return consumer


@pytest.fixture()
def cluster(test_config):
    try:
        cluster = Cluster()
    except NoBrokersAvailableError as ex:
        print(test_config.bootstrap_servers)
        raise ex

    yield cluster
