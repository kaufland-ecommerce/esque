from typing import List, Dict

import pykafka

from esque.cluster import Cluster
from esque.errors import raise_for_kafka_exception
from esque.resources.consumergroup import ConsumerGroup


class ConsumerGroupController:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster

    @raise_for_kafka_exception
    def get_consumergroup(self, consumer_id) -> ConsumerGroup:
        return ConsumerGroup(consumer_id, self.cluster)

    @raise_for_kafka_exception
    def list_consumer_groups(self) -> List[str]:
        brokers: Dict[int, pykafka.broker.Broker] = self.cluster.pykafka_client.cluster.brokers
        return list(
            set(group.decode("UTF-8") for _, broker in brokers.items() for group in broker.list_groups().groups)
        )
