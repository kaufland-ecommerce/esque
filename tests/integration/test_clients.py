from unittest import mock

import pytest
from confluent_kafka.cimpl import Consumer

from esque.clients import FileConsumer


@pytest.mark.integration
def test_consume_to_file(mocker, file_consumer: FileConsumer, consumer: Consumer):
    messages = consumer.consume(10)
    messages_call_list = [mock.call([message] for message in messages)]
    file_writer = mocker.patch("esque.message.FileWriter")
    file_writer.__enter__ = mocker.Mock(return_value=None)
    file_writer.__exit__ = mocker.Mock(return_value=None)
    file_writer.write_message_to_file.assert_has_calls(messages_call_list)
    file_consumer.file_writer = file_writer
    file_consumer.consume(10)
