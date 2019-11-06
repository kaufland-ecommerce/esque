from typing import Dict, List

import pykafka
from confluent_kafka.cimpl import TopicPartition

from esque.clients.consumer import ConsumerFactory
from esque.cluster import Cluster
from esque.config import Config
from esque.errors import translate_third_party_exceptions
from esque.resources.consumergroup import ConsumerGroup


class ConsumerGroupController:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster

    @translate_third_party_exceptions
    def get_consumergroup(self, consumer_id) -> ConsumerGroup:
        return ConsumerGroup(consumer_id, self.cluster)

    @translate_third_party_exceptions
    def list_consumer_groups(self) -> List[str]:
        brokers: Dict[int, pykafka.broker.Broker] = self.cluster.pykafka_client.cluster.brokers
        return list(
            set(group.decode("UTF-8") for _, broker in brokers.items() for group in broker.list_groups().groups)
        )

    @translate_third_party_exceptions
    def edit_consumer_group_offsets(self, consumer_id: str, partitions: List[TopicPartition]):
        """
        Commit consumergroup offsets to specific values
        :param consumer_id: ID of the consumer group
        :param partitions: List of TopicPartition objects denoting the offsets for each partition in the topic
        :return:
        """
        _config = Config().create_confluent_config()
        _config.update({"group.id": consumer_id, "enable.auto.commit": False})
        consumer = ConsumerFactory().create_custom_consumer(_config)
        consumer.commit(offsets=partitions)
