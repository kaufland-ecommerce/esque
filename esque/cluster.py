import operator

import pykafka
from confluent_kafka.admin import AdminClient, ConfigResource

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

    @property
    def bootstrap_servers(self):
        return self._config.bootstrap_servers

    @property
    def brokers(self):
        metadata = self.confluent_client.list_topics(timeout=1)
        return sorted(
            [
                {"id": broker.id, "host": broker.host, "port": broker.port}
                for broker in metadata.brokers.values()
            ],
            key=operator.itemgetter("id"),
        )

    def retrieve_config(self, config_type: ConfigResource.Type, id):
        requested_resources = [ConfigResource(config_type, str(id))]

        futures = self.confluent_client.describe_configs(requested_resources)

        (old_resource, future), = futures.items()

        future = ensure_kafka_futures_done([future])

        return future.result()
