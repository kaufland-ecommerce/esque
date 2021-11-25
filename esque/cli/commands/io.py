from typing import Optional

import click

from esque.cli.options import State, default_options
from esque.io.pipeline import PipelineBuilder


@click.command("io")
@click.option("-i", "--input-uri", help="Input URI", metavar="<input_uri>", required=True)
@click.option(
    "-o", "--output-uri", help="Output URI", metavar="<output_uri>", default="pipe+json://stdout?kv__indent=2"
)
@click.option(
    "-l",
    "--limit",
    help="Run until <limit> messages have been read in total over all partitions. There's no guarantee in which order"
    "multiple partitions are being read from. Will continue reading if the end of topic was reached.Stop with Ctrl-C. "
    "If not given, will read forever.",
    metavar="<limit>",
    default=None,
    type=int,
)
@click.option(
    "-s",
    "--start",
    help="Start reading at offset <start> for each partition. If not given, will start at beginning.",
    metavar="<start>",
    default=None,
    type=int,
)
@default_options
def io(state: State, input_uri: str, output_uri: str, limit: Optional[int], start: Optional[int]):
    """Run a message pipeline.

    Read all messages from the input configured by <input_uri> and write them to the output configured by <output_uri>.

    \b
    URI Format:
    <handler scheme>+<key serializer scheme>[+<value serializer scheme>]://[<host>][/<path>][?<param 1>(&<param 2>)]

    \b
    <handler scheme>    - Scheme that defines which handler should be used.
                          (See "Available Handlers" below)
    <serializer scheme> - Define data serialization format for key and value in message (e.g json or avro).
                          If no value serializer scheme defined key scheme will be used for both.
                          (See "Available Serializers" below)
    <host> and <path>   - Depends on handler and defines the exact source or target of the data pipeline.
                          (See "Available Handlers" below)
    <param>             - Define parameters for handler and/or serializer. Use the following prefixes to define
                          which component(s) to forward a parameter to:
                            k__  forward to key serializer
                            v__  forward to value serializer
                            kv__ forward to both key and value serializer
                            h__  forward to handler
                          E.g. "k__indent=1" will pass the parameter "indent" with a value of "1" to the key serializer.
                          Use the "kv__" prefix if you want to supply the same setting for both serializers in order to
                          avoid typing it twice. It's equivalent to having the same parameter and value once with "k__"
                          and once with "v__" as prefix.
                          Use "esque urlencode some&string:with;special_characters" to make values that would interfere
                          with uri parsing safe to use as query parameters.
                          (For supported parameters, see "Available Handlers" and "Available Serializers" below)

    Available Handlers:

    \b
    - kafka
      <host> -> Used for esque context. If empty, use active context.
      <path> -> Used as topic name.
      Supported Parameters:
        * consumer_group_id
          Use this consumer group id, when reading from a topic. (Default is "esque-client")
        * send_timestamp
          Use any value (preferably 1) to indicate whether the original message timestamp shall be sent along with the
          message.
          If not given, esque won't send it and the Kafka broker will set the timestamp based on the time when it
          receives the message.

    \b
    - pipe
      <host> -> Valid values are "stdin", "stdout" or "stderr".
      <path> -> No meaning, should stay empty.
      Supported Parameters:
        * key_encoding
          How to embed/extract the message key and value into/from the json objects that are written/read to/from the
          pipe. Valid values are "base64", "utf-8" and "hex". Use "utf-8" if you know the values are strings and don't
          mess up the output. Otherwise "base64" is a safer choice. (Default is "utf-8")
        * value_encoding
          Same as "key_encoding" but for the values. (Default is "utf-8")
        * pretty_print
          Use any value (preferably 1) if you want the json objects to be pretty printed into multiple lines.
          Do not use this mode if you want to further process the messages with "esque produce" or tools like "jq".
    \b
    - path
      <host> -> No meaning, should stay empty.
      <path> -> The path where to read messages from or write them to.
                Has to be a directory, will be created if it doesn't exist.
                Will raise en exception if it exists and already contains data.
                Take into account that path starts after the first "/" after the empty host part of the uri.
                Therefore "path+raw://" is invalid, "path+raw:///foo/bar" will result in "foo/bar" (relative) and
                "path+raw:////foo/bar" will result in "/foo/bar" (absolute).
                So the URI will need three subsequent slashes for a relative path and four for an absolute path.
      Supported Parameters:
        none

    \b
    Available Serializers:
    - raw
      Doesn't deserialize the message payload and ignores the schema. Basically transfers messages 1:1.
      Useful if you don't need to do any transformations and just want to get data from A to B without the overhead of
      serialization, deserialization and schema handling.
      WARNING: Be careful with data that has a schema which was or has to be registered by a schema registry!
               This binary data is usually prefixed with a schema id that is valid only for one specific registry
               instance. Transferring it to a different cluster 1:1 most likely makes it unable to be deserialized
               because of two reasons:
                 1. The schema id is from another schema registry instance and either doesn't exist on the target or
                    refers to the wrong schema.
                 2. The schema was not registered for the target subject in the target schema registry.
      Supported Parameters:
       none

    \b
    - json
      Supported Parameters:
        * indent
          When serializing data into json, use this many spaces to indent (pretty format) the output.
          By default the serializer uses no line breaks or indentation and puts everything into one line.
        * encoding
          Use this character encoding when transforming the json string into bytes for the message key or value.
          (Default is "utf-8")

    \b
    - string
      Supported Parameters:
        * encoding
          Use this character encoding to serialize or deserialize the string to/from the message key or value.

    \b
    - reg-avro
      Schema registry based AVRO serializer.
      Supported Parameters:
        * schema_registry_uri (required)
          The url of the schema registry. E.g. https://schema-registry.example.com
          You can use "esque urlencode https://schema-registry.example.com" to get a representation of that url that's
          you safe to pass in as query param in the input or output uri.
        * schema_subject (required only for writing, not for reading)
          The subject to register new schemas for.
          A subject is the topic name with "-key" or "-value" as suffix, depending on whether this is the key or value
          serializer.

    \b
    EXAMPLES:
    # Read data from an avro topic
    esque io -i "kafka+reg-avro:///mytopic?kv__schema_registry_uri=$(esque urlencode https://schema-registry.example.com)"

    \b
    # Extract and format message value using jq
    esque io -i "kafka+json:///mytopic" -o "pipe+json://stdout" | jq '.value | fromjson'

    \b
    # Dump messages from a kafka topic into a file
    esque io -i "kafka+raw://dev/mytopic" -o "pipe+json://stdout" > mytopic_dump.json

    \b
    # Restore messages from a file
    cat mytopic_dump.json > esque io -i "pipe+json://stdin" -o "kafka+raw://dev/mytopic"
    """
    builder = PipelineBuilder()
    builder.with_input_from_uri(input_uri)
    builder.with_output_from_uri(output_uri)

    builder.with_range(start, limit)

    pipeline = builder.build()

    pipeline.run_pipeline()
