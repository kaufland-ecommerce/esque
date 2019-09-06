import operator

import pykafka
from confluent_kafka.admin import AdminClient, ConfigResource
from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

from esque.config import Config
from esque.helpers import ensure_kafka_futures_done


class Cluster:
    def __init__(self):
        self._config = Config()
        self.confluent_client = AdminClient(self._config.create_confluent_config())
        self.pykafka_client = pykafka.client.KafkaClient(
            **self._config.create_pykafka_config(), broker_version="1.0.0"
        )
        self.confluent_client.poll(timeout=1)
        self.zookeeper_client = Zookeeper(config=self._config)

    @property
    def bootstrap_servers(self):
        return self._config.bootstrap_servers

    @property
    def brokers(self):
        metadata = self.confluent_client.list_topics(timeout=1)
        return sorted(
            [{"id": broker.id, "host": broker.host, "port": broker.port} for broker in metadata.brokers.values()],
            key=operator.itemgetter("id"),
        )

    def retrieve_config(self, config_type: ConfigResource.Type, id):
        requested_resources = [ConfigResource(config_type, str(id))]

        futures = self.confluent_client.describe_configs(requested_resources)

        (old_resource, future), = futures.items()

        future = ensure_kafka_futures_done([future])

        return future.result()


class Zookeeper:
    # See https://github.com/Yelp/kafka-utils/blob/master/kafka_utils/util/zookeeper.py
    def __init__(self, config: Config, *, retries: int = 5):
        self.config = config
        self.retries = retries

    def __enter__(self):
        self.zk = KazooClient(
            hosts=",".join(self.config.zookeeper_nodes),
            read_only=True,
            connection_retry=KazooRetry(max_tries=self.retries),
        )
        self.zk.start()
        return self

    def __exit__(self, type, value, traceback):
        self.zk.stop()

    def create(self, path, value, *, makepath):
        self.zk.create(path, value, makepath=makepath)
