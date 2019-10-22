import pytest
from click.testing import CliRunner
from confluent_kafka.cimpl import Producer as ConfluenceProducer

from esque.cli.commands import consume
from esque.cli.commands import produce


@pytest.mark.integration
def test_plain_text_message_cli_pipe(
    producer: ConfluenceProducer, topic: str, non_interactive_cli_runner: CliRunner, produced_messages_same_partition
):
    produced_messages_same_partition(topic_name=topic, producer=producer)

    result1 = non_interactive_cli_runner.invoke(consume, args=["--stdout", "--numbers", "10", topic])
    result2 = non_interactive_cli_runner.invoke(produce, args=["--stdin", topic], input=result1.output)
    # Check assertions:
    assert "10" in result2.output
    assert result2.exit_code == 0
