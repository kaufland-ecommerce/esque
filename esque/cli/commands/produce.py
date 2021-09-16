from pathlib import Path

import click

from esque.cli.autocomplete import list_contexts, list_topics
from esque.cli.helpers import attrgetter, ensure_approval, isatty
from esque.cli.options import State, default_options
from esque.cli.output import blue_bold, green_bold
from esque.clients.producer import ProducerFactory
from esque.errors import TopicDoesNotExistException
from esque.resources.topic import Topic


@click.command("produce")
@click.argument("topic", autocompletion=list_topics)
@click.option(
    "-d",
    "--directory",
    metavar="<directory>",
    help="Directory containing Kafka messages.",
    type=click.STRING,
    required=False,
)
@click.option(
    "-t",
    "--to",
    "to_context",
    metavar="<destination_ctx>",
    help="Destination context.",
    type=click.STRING,
    autocompletion=list_contexts,
    required=False,
)
@click.option(
    "-m",
    "--match",
    metavar="<filter_expresion>",
    help="Message filtering expression.",
    type=click.STRING,
    required=False,
)
@click.option(
    "-a",
    "--avro",
    help="Set this flag if the topic contains avro data. This flag is mutually exclusive with the --binary flag",
    default=False,
    is_flag=True,
)
@click.option(
    "-b",
    "--binary",
    help="Set this flag if the topic contains binary data. Or the data should not be (de-)serialized. "
    "This flag is mutually exclusive with the --avro flag",
    default=False,
    is_flag=True,
)
@click.option(
    "--stdin", "read_from_stdin", help="Read messages from STDIN instead of a directory.", default=False, is_flag=True
)
@click.option(
    "-y",
    "--ignore-errors",
    "ignore_stdin_errors",
    help="Only when reading from STDIN. If JSON validation fails, write the malformed JSON as a string in message value"
    " (without key and specified partition assignment).",
    default=False,
    is_flag=True,
)
@default_options
def produce(
    state: State,
    topic: str,
    to_context: str,
    directory: str,
    avro: bool,
    binary: bool,
    match: str = None,
    read_from_stdin: bool = False,
    ignore_stdin_errors: bool = False,
):
    """Produce messages to a topic.

    Write messages to a given topic in a given context. These messages can come from either a directory <directory>
    containing files corresponding to the different partitions or from STDIN.

    \b
    EXAMPLES:
    # Write all messages from the files in <directory> to TOPIC in the <destination_ctx> context.
    esque produce -d <directory> -t <destination_ctx> TOPIC

    \b
    # Start environment in terminal to write messages to TOPIC in the <destination_ctx> context.
    esque produce --stdin -f <destination_ctx> -y TOPIC

    \b
    # Copy source_topic to destination_topic.
    esque consume -f first-context --stdout source_topic | esque produce -t second-context --stdin destination_topic
    """
    if binary and avro:
        raise ValueError("Cannot set data to be interpreted as binary AND avro.")

    if directory is None and not read_from_stdin:
        raise ValueError("You have to provide a directory or use the --stdin flag.")

    if directory is not None:
        input_directory = Path(directory)
        if not input_directory.exists():
            raise ValueError(f"Directory {directory} does not exist!")

    if not to_context:
        to_context = state.config.current_context
    state.config.context_switch(to_context)

    topic_controller = state.cluster.topic_controller
    if topic not in map(attrgetter("name"), topic_controller.list_topics(get_topic_objects=False)):
        click.echo(f"Topic {blue_bold(topic)} does not exist in context {blue_bold(to_context)}.")
        if ensure_approval("Would you like to create it now?"):
            topic_controller.create_topics([Topic(topic)])
        else:
            raise TopicDoesNotExistException(f"Topic {topic} does not exist!", -1)

    stdin = click.get_text_stream("stdin")
    if read_from_stdin and isatty(stdin):
        click.echo(
            "Type the messages to produce, "
            + ("in JSON format, " if not ignore_stdin_errors else "")
            + blue_bold("one per line")
            + ". End with "
            + blue_bold("CTRL+D")
            + "."
        )
    elif read_from_stdin and not isatty(stdin):
        click.echo(f"Reading messages from an external source, {blue_bold('one per line')}).")
    else:
        click.echo(
            f"Producing from directory {blue_bold(str(directory))} to topic {blue_bold(topic)}"
            f" in target context {blue_bold(to_context)}"
        )
    producer = ProducerFactory().create_producer(
        topic_name=topic,
        input_directory=input_directory if not read_from_stdin else None,
        avro=avro,
        match=match,
        ignore_stdin_errors=ignore_stdin_errors,
        binary=binary,
    )
    total_number_of_messages_produced = producer.produce()
    click.echo(
        green_bold(str(total_number_of_messages_produced))
        + " messages successfully produced to topic "
        + blue_bold(topic)
        + " in context "
        + blue_bold(to_context)
        + "."
    )
