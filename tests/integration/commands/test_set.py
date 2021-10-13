import pendulum
import pytest
from confluent_kafka.cimpl import Producer as ConfluenceProducer
from confluent_kafka.cimpl import TopicPartition

from esque.cli.commands import esque
from esque.controller.consumergroup_controller import ConsumerGroupController
from tests.utils import produce_text_test_messages


@pytest.mark.integration
def test_set_offsets_offset_to_absolute_value(
    topic: str,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group: str,
    consumergroup_controller: ConsumerGroupController,
):
    produce_text_test_messages(producer=producer, topic=(topic, 1), amount=10)

    consumergroup_controller.commit_offsets(consumer_group, [TopicPartition(topic=topic, partition=0, offset=10)])

    consumergroup_desc_before = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )

    interactive_cli_runner.invoke(
        esque,
        args=["set", "offsets", consumer_group, "--topic-name", topic, "--offset-to-value", "1"],
        input="y\n",
        catch_exceptions=False,
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )
    assert consumergroup_desc_before["offsets"][topic][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic][0]["consumer_offset"] == 1


@pytest.mark.integration
def test_set_offsets_offset_to_delta(
    topic: str,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group: str,
    consumergroup_controller: ConsumerGroupController,
):
    produce_text_test_messages(producer=producer, topic=(topic, 1), amount=10)

    consumergroup_controller.commit_offsets(consumer_group, [TopicPartition(topic=topic, partition=0, offset=10)])

    consumergroup_desc_before = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )

    interactive_cli_runner.invoke(
        esque,
        args=["set", "offsets", consumer_group, "--topic-name", topic, "--offset-by-delta", "-2"],
        input="y\n",
        catch_exceptions=False,
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )
    assert consumergroup_desc_before["offsets"][topic][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic][0]["consumer_offset"] == 8


@pytest.mark.integration
def test_set_offsets_offset_to_delta_all_topics(
    topic: str,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group: str,
    consumergroup_controller: ConsumerGroupController,
):
    produce_text_test_messages(producer=producer, topic=(topic, 1), amount=10)

    consumergroup_controller.commit_offsets(consumer_group, [TopicPartition(topic=topic, partition=0, offset=10)])

    consumergroup_desc_before = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )

    interactive_cli_runner.invoke(
        esque, args=["set", "offsets", consumer_group, "--offset-by-delta", "-2"], input="y\n", catch_exceptions=False
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )
    assert consumergroup_desc_before["offsets"][topic][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic][0]["consumer_offset"] == 8


@pytest.mark.integration
def test_set_offsets_offset_from_group(
    topic: str,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group: str,
    target_consumer_group: str,
    consumergroup_controller: ConsumerGroupController,
):
    produce_text_test_messages(producer=producer, topic=(topic, 1), amount=10)

    consumergroup_controller.commit_offsets(consumer_group, [TopicPartition(topic=topic, partition=0, offset=10)])

    consumergroup_desc_before = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )

    interactive_cli_runner.invoke(
        esque, args=["set", "offsets", consumer_group, "--offset-by-delta", "-2"], input="y\n", catch_exceptions=False
    )
    consumergroup_desc_after = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )

    # create a new consumer in a separate group and consume just one message
    consumergroup_controller.commit_offsets(
        target_consumer_group, [TopicPartition(topic=topic, partition=0, offset=1)]
    )

    interactive_cli_runner.invoke(
        esque,
        args=["set", "offsets", target_consumer_group, "--offset-from-group", consumer_group],
        input="y\n",
        catch_exceptions=False,
    )
    consumergroup_desc_target = consumergroup_controller.get_consumer_group(
        consumer_id=target_consumer_group
    ).describe(partitions=True)

    assert consumergroup_desc_before["offsets"][topic][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic][0]["consumer_offset"] == 8
    assert consumergroup_desc_target["offsets"][topic][0]["consumer_offset"] == 8


@pytest.mark.integration
def test_set_offsets_offset_to_timestamp_value(
    topic: str,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group: str,
    consumergroup_controller: ConsumerGroupController,
):
    messages = produce_text_test_messages(producer=producer, topic=(topic, 1), amount=10)

    consumergroup_controller.commit_offsets(consumer_group, [TopicPartition(topic=topic, partition=0, offset=10)])

    consumergroup_desc_before = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )

    fifth_message = messages[4]
    timestamp = fifth_message.timestamp
    dt = pendulum.from_timestamp(round(timestamp / 1000) - 1)

    interactive_cli_runner.invoke(
        esque,
        args=[
            "set",
            "offsets",
            consumer_group,
            "--topic-name",
            topic,
            "--offset-to-timestamp",
            dt.format("YYYY-MM-DDTHH:mm:ss"),
        ],
        input="y\n",
        catch_exceptions=False,
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumer_group(consumer_id=consumer_group).describe(
        partitions=True
    )
    assert consumergroup_desc_before["offsets"][topic][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic][0]["consumer_offset"] == 4
