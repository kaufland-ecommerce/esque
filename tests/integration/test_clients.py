import pytest

from esque.clients import FileConsumer


@pytest.mark.integration
def test_consume_to_file(file_consumer: FileConsumer):
    file_consumer.consume(10)
