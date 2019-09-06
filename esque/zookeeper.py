from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

from esque.config import Config


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
