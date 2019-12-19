import pendulum
import pytest
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque.cli.commands import set_offsets
from esque.clients.consumer import ConsumerFactory


@pytest.mark.integration
def test_set_offsets_offset_to_absolute_value(
    topic: str,
    produced_messages_same_partition,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group,
    consumergroup_controller,
):
    produced_messages_same_partition(topic, producer)

    vanilla_consumer = ConsumerFactory().create_consumer(
        group_id=consumer_group,
        topic_name=None,
        output_directory=None,
        last=False,
        avro=False,
        initialize_default_output_directory=False,
        match=None,
        enable_auto_commit=True,
    )

    vanilla_consumer.subscribe([topic])
    vanilla_consumer.consume(10)
    vanilla_consumer.close()
    del vanilla_consumer

    consumergroup_desc_before = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )

    interactive_cli_runner.invoke(
        set_offsets,
        args=[consumer_group, "--topic-name", topic, "--offset-to-value", "1"],
        input="y\n",
        catch_exceptions=True,
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )
    assert consumergroup_desc_before["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 1


@pytest.mark.integration
def test_set_offsets_offset_to_delta(
    topic: str,
    produced_messages_same_partition,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group,
    consumergroup_controller,
):
    produced_messages_same_partition(topic, producer)

    vanilla_consumer = ConsumerFactory().create_consumer(
        group_id=consumer_group,
        topic_name=None,
        output_directory=None,
        last=False,
        avro=False,
        initialize_default_output_directory=False,
        match=None,
        enable_auto_commit=True,
    )
    vanilla_consumer.subscribe([topic])
    vanilla_consumer.consume(10)
    vanilla_consumer.close()
    del vanilla_consumer

    consumergroup_desc_before = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )

    interactive_cli_runner.invoke(
        set_offsets,
        args=[consumer_group, "--topic-name", topic, "--offset-by-delta", "-2"],
        input="y\n",
        catch_exceptions=True,
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )
    assert consumergroup_desc_before["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 8


@pytest.mark.integration
def test_set_offsets_offset_to_delta_all_topics(
    topic: str,
    produced_messages_same_partition,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group,
    consumergroup_controller,
):
    produced_messages_same_partition(topic, producer)

    vanilla_consumer = ConsumerFactory().create_consumer(
        group_id=consumer_group,
        topic_name=None,
        output_directory=None,
        last=False,
        avro=False,
        initialize_default_output_directory=False,
        match=None,
        enable_auto_commit=True,
    )
    vanilla_consumer.subscribe([topic])
    vanilla_consumer.consume(10)
    vanilla_consumer.close()
    del vanilla_consumer

    consumergroup_desc_before = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )

    interactive_cli_runner.invoke(
        set_offsets, args=[consumer_group, "--offset-by-delta", "-2"], input="y\n", catch_exceptions=True
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )
    assert consumergroup_desc_before["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 8


@pytest.mark.integration
def test_set_offsets_offset_from_group(
    topic: str,
    produced_messages_same_partition,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group,
    target_consumer_group,
    consumergroup_controller,
):
    produced_messages_same_partition(topic, producer)

    vanilla_consumer = ConsumerFactory().create_consumer(
        group_id=consumer_group,
        topic_name=None,
        output_directory=None,
        last=False,
        avro=False,
        initialize_default_output_directory=False,
        match=None,
        enable_auto_commit=True,
    )
    vanilla_consumer.subscribe([topic])
    vanilla_consumer.consume(10)
    vanilla_consumer.close()
    del vanilla_consumer

    consumergroup_desc_before = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )

    interactive_cli_runner.invoke(
        set_offsets, args=[consumer_group, "--offset-by-delta", "-2"], input="y\n", catch_exceptions=True
    )
    consumergroup_desc_after = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )

    # create a new consumer in a separate group and consume just one message
    vanilla_target_consumer = ConsumerFactory().create_consumer(
        group_id=target_consumer_group,
        topic_name=None,
        output_directory=None,
        last=False,
        avro=False,
        initialize_default_output_directory=False,
        match=None,
        enable_auto_commit=True,
    )

    vanilla_target_consumer.subscribe([topic])
    vanilla_target_consumer.consume(1)
    vanilla_target_consumer.close()
    del vanilla_target_consumer

    interactive_cli_runner.invoke(
        set_offsets,
        args=[target_consumer_group, "--offset-from-group", consumer_group],
        input="y\n",
        catch_exceptions=True,
    )
    consumergroup_desc_target = consumergroup_controller.get_consumergroup(consumer_id=target_consumer_group).describe(
        verbose=True
    )

    assert consumergroup_desc_before["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 8
    assert consumergroup_desc_target["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 8


@pytest.mark.integration
def test_set_offsets_offset_to_timestamp_value(
    topic: str,
    produced_messages_same_partition,
    interactive_cli_runner,
    producer: ConfluenceProducer,
    consumer_group,
    consumergroup_controller,
):
    produced_messages_same_partition(topic, producer, 1.5)

    vanilla_consumer = ConsumerFactory().create_consumer(
        group_id=consumer_group,
        topic_name=None,
        output_directory=None,
        last=False,
        avro=False,
        initialize_default_output_directory=False,
        match=None,
        enable_auto_commit=True,
    )

    vanilla_consumer.subscribe([topic])
    messages = []
    for i in range(0, 10):
        messages.append(vanilla_consumer.consume_single_message())
    vanilla_consumer.close()
    del vanilla_consumer

    consumergroup_desc_before = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )

    fifth_message = messages[4]
    timestamp = fifth_message.timestamp()
    dt = pendulum.from_timestamp(round(timestamp[1] / 1000) - 1)

    interactive_cli_runner.invoke(
        set_offsets,
        args=[consumer_group, "--topic-name", topic, "--offset-to-timestamp", dt.format("YYYY-MM-DDTHH:mm:ss")],
        input="y\n",
        catch_exceptions=True,
    )
    # Check assertions:
    consumergroup_desc_after = consumergroup_controller.get_consumergroup(consumer_id=consumer_group).describe(
        verbose=True
    )
    assert consumergroup_desc_before["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 10
    assert consumergroup_desc_after["offsets"][topic.encode("UTF-8")][0]["consumer_offset"] == 4
