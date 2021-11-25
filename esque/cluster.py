import operator
from functools import cached_property

import kafka
from confluent_kafka.admin import AdminClient, ConfigResource

from esque.config import Config
from esque.controller.topic_controller import TopicController
from esque.helpers import ensure_kafka_future_done, unpack_confluent_config


class Cluster:
    def __init__(self):
        self._config = Config.get_instance()
        self.__topic_controller = None

    @cached_property
    def kafka_python_client(self) -> kafka.KafkaAdminClient:
        return kafka.KafkaAdminClient(**self._config.create_kafka_python_config(), api_version_auto_timeout_ms=30000)

    @cached_property
    def confluent_client(self) -> AdminClient:
        return AdminClient({"topic.metadata.refresh.interval.ms": "250", **self._config.create_confluent_config()})

    @property
    def topic_controller(self) -> TopicController:
        if self.__topic_controller is None:
            self.__topic_controller = TopicController(self, self._config)
        return self.__topic_controller

    @property
    def bootstrap_servers(self):
        return self._config.bootstrap_servers

    def get_metadata(self):
        return self.confluent_client.list_topics(timeout=20)

    @property
    def brokers(self):
        metadata = self.confluent_client.list_topics(timeout=20)
        return sorted(
            [{"id": broker.id, "host": broker.host, "port": broker.port} for broker in metadata.brokers.values()],
            key=operator.itemgetter("id"),
        )

    def retrieve_config(self, config_type: ConfigResource.Type, id):
        requested_resources = [ConfigResource(config_type, str(id))]
        futures = self.confluent_client.describe_configs(requested_resources)
        ((old_resource, future),) = futures.items()
        future = ensure_kafka_future_done(future)
        result = future.result()
        return unpack_confluent_config(result)
