# esque - an operational Kafka tool

[![pypi Version](https://img.shields.io/pypi/v/esque.svg)](https://pypi.org/project/esque/) [![Python Versions](https://img.shields.io/pypi/pyversions/esque.svg)](https://pypi.org/project/esque/) [![Build Status](https://travis-ci.org/real-digital/esque.svg?branch=master)](https://travis-ci.org/real-digital/esque) [![Coverage Status](https://coveralls.io/repos/github/real-digital/esque/badge.svg)](https://coveralls.io/github/real-digital/esque?branch=add-coverage) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

In the Kafka world nothing is easy, but `esque` (pronounced *esk*) is an attempt at it.

Esque is a user-centric command line interface for Kafka administration. 

## Why should you care?

Some stuff is hard, and that is okay, but listing your kafka topics shouldn't be.

While adopting kafka at real.digital we noticed the immense entry barrier it poses to newcomers. 
We can't recount how often we wrote Slack messages asking for the script to check the 
status of topics or consumer groups. This is partly (but not only) due to a 
fragmented and unclear definition of tooling and APIs for kafka. 
In a wide array of administration tools, `esque` distances itself by striving to provide Kafka Ops for Humans, in a usable and natural way. We feel that the goal of Esque embodies one of the Perl design principles: “**keep easy things easy, and make hard things possible**”. 

## Principles

Our driving objectives can be summed up through these four principles:
* comes with batteries included,
* feature rich,
* robust,
* insightful,
* made by engineers for engineers

## Basic Features

* Support for any type of Kafka deployment >1.2,
* List Resources (Topics, Consumergroups, Brokers),
* Get detailed Overviews of Resources (Topics, Consumergroups, Brokers),
* Create/Delete Topics,
* Context Switch (Easily Switch between pre-defined Clusters),
* Kafka Ping (Test roundtrip time to your kafka cluster)

## Installation and Usage

### Installation

`esque` is available at pypi.org and can be installed with `pip install esque`. `esque` requires Python 3.6+ to run.

### Autocompletion

Autocompletion is automatically installed via a post-install hook in the setup.py. 
If it doesn't work for some reason you can still install it yourself: 

#### Bash

```
 echo 'eval "$(_ESQUE_COMPLETE=source esque)"' >> ~/.esque/autocompletion.sh
 echo "source ~/.esque/autocompletion.sh" >> ~/.bashrc
```

#### ZSH

```
echo 'eval "$(_ESQUE_COMPLETE=source_zsh esque)"' >> ~/.esque/autocompletion.zsh
echo "source ~/.esque/autocompletion.zsh" >> ~/.zshrc
```

### Usage

#### Config Definition

When starting `esque` for the first time the following message will appear:

```
No config provided in ~/.esque
Should a sample file be created in ~/.esque [y/N]:
```

When answering with `y` `esque` will copy over the [sample config](https://github.com/real-digital/esque/blob/master/sample_config.cfg) to `~/.esque/esque.cfg`.
Afterwards you can modify that file to fit your cluster definitions.

Alternatively might just provide a config file following the sample config's file in that path.


### Command Overview

```
$ esque
Usage: esque [OPTIONS] COMMAND [ARGS]...

  (Kafka-)esque.

Options:
  --help  Show this message and exit.

Commands:
  apply     Apply a configuration
  create    Create a new instance of a resource.
  ctx       Switch clusters.
  delete    Delete a resource.
  describe  Get detailed informations about a resource.
  get       Get a quick overview of different resources.
  ping      Tests the connection to the kafka cluster.
```

#### Config file for "apply" command

The config for the apply command has to be a yaml file and
is given with the option -f or --file.

In the current version only topic configurations can be
changed and specified.

It has to use the schema, which is used 
for the following example:

```
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

## Development

To setup your development environment, make sure you have at least Python 3.6 & [Pipenv](https://github.com/pypa/pipenv) installed, then run 

```
pipenv install --dev
pipenv shell 
export PYTHONPATH=$(pwd)
```

### Run tests

To start up a local test setup (Kafka and Zookeeper), you can run

```
docker-compose up
```
While this `docker-compose` stack is up, you can run the tests from the CLI via `pytest tests/ --integration --local`


Alternatively, you can also run the entire test suite, without needing to setup the development environment, in docker compose via `docker-compose -f docker-compose.yml -f docker-compose.test.yml` 


### Pre Commit Hooks

To install pre commit hooks run:

```
pip install pre-commit
pre-commit install
pre-commit install-hooks
```
## Additional information

You can find additional information, development cycle and release information on our [wiki page](https://github.com/real-digital/esque/wiki).


## Alternatives

- [LinkedIn KafkaTools](https://github.com/linkedin/kafka-tools)
- [PyKafka Tools](https://github.com/Parsely/pykafka/blob/master/pykafka/cli/kafka_tools.py)
- [Official Kafka Scripts](https://github.com/apache/kafka/tree/trunk/bin)
- [kafkacat](https://github.com/edenhill/kafkacat)
