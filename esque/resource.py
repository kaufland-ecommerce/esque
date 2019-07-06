from abc import abstractmethod


class KafkaResource:
    @abstractmethod
    def as_dict(self):
        raise NotImplementedError("as_dict() not implemented")
