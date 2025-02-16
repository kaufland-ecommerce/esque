# esque - an operational Kafka tool

[![pypi Version](https://img.shields.io/pypi/v/esque.svg)](https://pypi.org/project/esque/)
[![Python Versions](https://img.shields.io/pypi/pyversions/esque.svg)](https://pypi.org/project/esque/)
![Build Status](https://github.com/real-digital/esque/workflows/Style,%20Unit%20And%20Integration%20Tests/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/real-digital/esque/badge.svg?branch=master)](https://coveralls.io/github/real-digital/esque?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

In the Kafka world nothing is easy, but `esque` (pronounced *esk*) is an attempt at it.

`esque` is a user-centric command line interface for Kafka administration.

## Why should you care?

Some stuff is hard, and that is okay, but listing your kafka topics shouldn't be.

While adopting kafka at real.digital(Kaufland) we noticed the immense entry barrier it poses to newcomers.
We can't recount how often we wrote Slack messages asking for the script to check the
status of topics or consumer groups. This is partly (but not only) due to a
fragmented and unclear definition of tooling and APIs for kafka.
In a wide array of administration tools, `esque` distances itself by striving to provide Kafka Ops for Humans, in a usable and natural way.

We feel that the goal of `esque` embodies the principle: “**keep easy things easy, and make hard things possible**”.

## Principles

* batteries included
* feature rich
* robust
* insightful
* by engineers for engineers

## Feature Overview

* Support for any type of Kafka deployment >1.2
* Display Resources (Topics, Consumer Groups, Brokers)
* Get detailed Overviews of Resources (Topics, Consumer Groups, Brokers)
* Create/Delete Topics
* Edit Topic Configurations
* Edit Consumer Offset for Topics
* SASL/SSL Support out of the box
* Consuming from Avro,Protobuf,plaintext,struct Topics (including Avro Schema Resolution from Schema Registry)
* Producing to and from Avro and Plaintext Topics (including Avro Schema Resolution from Schema Registry)
* Context Switch (Easily Switch between pre-defined Clusters)
* Kafka Ping (Test roundtrip time to your kafka cluster)

## Command Overview

```bash
$ esque
Usage: esque [OPTIONS] COMMAND [ARGS]...

  esque - an operational kafka tool.

  In the Kafka world nothing is easy, but esque (pronounced esk) is an
  attempt at it.

Options:
  --version      Show the version and exit.
  -v, --verbose  Return stack trace on error.
  --no-verify    Skip all verification dialogs and answer them with yes.
  --help         Show this message and exit.

Commands:
  apply      Apply a set of topic configurations.
  config     Configuration-related options.
  consume    Consume messages from a topic.
  create     Create a new instance of a resource.
  ctx        List contexts and switch between them.
  delete     Delete a resource.
  describe   Get detailed information about a resource.
  edit       Edit a resource.
  get        Get a quick overview of different resources.
  io         Run a message pipeline.
  ping       Test the connection to the kafka cluster.
  produce    Produce messages to a topic.
  set        Set resource attributes.
  urlencode  Url-encode the given value.
```

## Installation and Usage

### Installation

`esque` is available at [pypi.org](https://pypi.org/project/esque/) and can be installed with `pip install esque`. 

`esque` requires Python 3.9+ to run(Python 3.13 is not yet supported)..

#### Installation on Alpine Linux

There are no wheels for Alpine Linux, so `esque` requires a few extra dependencies to build them during installation.

```bash
apk add python3-dev py3-pip librdkafka librdkafka-dev g++
```

#### Installation on Apple Silicon

The installation for Kafka is slightly different for Apple Silicon devices, so simply running `pip install esque` may 
result in errors. 

The fix for this is to first install the librdkafka library with `brew install librdkafka` (make note of which version is installed).
Then, add the following to the `.zshrc` file with the correct version of librdkafka:
```bash
export C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/X.X.X/include/
export LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/X.X.X/lib/
```
After starting a new shell (or `source ~/.zshrc`), you will be able to install `esque` normally with pip.

### Autocompletion

The autocompletion scripts for `bash` and `zsh` can be generated by running `esque config autocomplete`.

### Usage

#### Config Definition

When starting `esque` for the first time the following message will appear:

```bash
No config provided in ~/.esque
Should a sample file be created in ~/.esque [y/N]:
```

When answering with `y` `esque` will copy over the [sample config](https://github.com/real-digital/esque/blob/master/esque/config/sample_config.yaml) to `~/.esque/esque_config.yaml`.
Afterwards you can modify that file to fit your cluster definitions.

Alternatively might just provide a config file following the sample config's file in that path.

##### Config Example

```yaml
version: 1
current_context: local
contexts:
  # This context corresponds to a local development cluster
  # created by docker-compose when running esque from the host machine.
  local:
    bootstrap_servers:
      - localhost:9092
    security_protocol: PLAINTEXT
    schema_registry: http://localhost:8081
    proto:
      topic1:
        protoc_py_path: /home/user/pb/
        module_name: hi_pb2
        class_name: HelloWorldResponse
      any_key:
        protoc_py_path: 'path to compiled files using protoc'
        module_name: "api_stubs_py.api.bye_pb2"
        class_name: ByeMessage
    default_values:
      num_partitions: 1
      replication_factor: 1
```

#### Config file for "apply" command

The config for the apply command has to be a yaml file and
is given with the option -f or --file.

In the current version only topic configurations can be
changed and specified.

It has to use the same schema, which is used
for the following example:

```yaml
topics:
  - name: topic_one
    replication_factor: 3
    num_partitions: 50
    config:
      cleanup.policy: compact
  - name: topic_two
    replication_factor: 3
    num_partitions: 50
    config:
      cleanup.policy: compact
```
## How to de-serialize protobuf files

To de-serialize protobuf files you need to have generated files using [protoc](https://grpc.io/docs/protoc-installation/) command.
Let's assume you have following proto file and you have dispatched message to a kafka topic with `HelloWorldResponse`:
```
syntax = "proto3";

service Hi {
    rpc Get (HelloWorldRequest) returns (HelloWorldResponse);
}

message HelloWorldRequest {
    string name = 1;
}

message HelloWorldResponse {
    string type_string = 1;
    optional string optional_string = 2;
    EnumType type_enum = 3;
    int32 type_int32 = 4;
    int64 type_int64 = 5;
    optional int64 optional_int64 = 6;
    float type_float = 7;
}
enum EnumType {
    ENUM_TYPE_UNSPECIFIED = 0;
}

```
first you need to compile it :
```
protoc --python_out /home/user/pb/ hi.proto
```

then it will create files like :
```
hi.proto
hi_pb2.py
```
After compiling protobuf files for python you need three things.

* `protoc_py_path`: absolute path to the compiled files using protoc. example :  `/home/user/pb/`
* `module_name`: path to the file having contains your message. our example:  `hi_pb2` (without .py)
* `class_name`: message name in protobuf. our example: `HelloWorldResponse`

You can insert this parameters using consume command. example :
```
esque consume topic_name -s proto --val-protoc-py-path /home/user/pb/ --val-protoc-module-name hi_pb2 --val-protoc-class-name HelloWorldResponse
```
if this is a topic you use everyday you can save this in configuration file like :
```yaml
    proto:
      topic_name:
        protoc_py_path: /home/user/pb/
        module_name: hi_pb2
        class_name: HelloWorldResponse
```
and next you just need to run `esque consume topic_name -s proto`.

## Development

To setup your development environment, make sure you have at least Python 3.9 & [poetry](https://github.com/sdispater/poetry) installed, then run

```bash
poetry install
poetry shell
```

### Pre Commit Hooks

To install pre commit hooks run:

```bash
pip install pre-commit
pre-commit install
pre-commit install-hooks
```

### Run tests

#### Integration Tests

esque comes with a docker-compose based kafka stack which you can start up with `make test-suite`.

You can then run the integration tests against this stack with `pytest tests/ --integration --local`.

Alternatively you can go the fast way and just run the whole stack + integration tests in docker:

```bash
make integration-test
```

#### Unit Tests

If you only want the unit tests, just run:

```bash
make test
```