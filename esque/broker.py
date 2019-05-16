from confluent_kafka.admin import ConfigResource

from esque.helpers import unpack_confluent_config


class Broker:
    def __init__(self, cluster, broker_id):
        self.cluster = cluster
        self.broker_id = broker_id

    def describe(self):
        conf = self.cluster.retrieve_config(ConfigResource.Type.BROKER, self.broker_id)
        return unpack_confluent_config(conf)
