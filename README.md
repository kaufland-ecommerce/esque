# esque - a kafka tool.

[![pypi Version](https://img.shields.io/pypi/v/esque.svg)](https://pypi.org/project/esque/) [![Python Versions](https://img.shields.io/pypi/pyversions/esque.svg)](https://pypi.org/project/esque/) [![Build Status](https://travis-ci.org/real-digital/esque.svg?branch=master)](https://travis-ci.org/real-digital/esque) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

In the Kafka world nothing is easy, but `esque` (pronounced esk) is an attempt at it.

## Why should you care?

Some stuff is hard, and that is okay, but listing your kafka topics shouldn't.

While adopting kafka at real.digital we noticed the immense entry barrier it poses to newcomers. 
We can't recount how often we wrote Slack messages asking for the script to check the status of topics or consumergroups.
This is partly (but not only) due to a fragmented and unclear definition of tooling, APIs for kafka.

`esque` tries to become a human-friendly point of first contact for your kafka cluster by exposing a `kubectl`-like interface to it.

## Main Features

* List Resources (Topics, Consumergroups, Brokers)
* Get detailed Overviews of Resources (Topics, Consumergroups, Brokers)
* Create/Delete Topics
* Context Switch (Easily Switch between pre-defined Clusters)
* Kafka Ping (Test roundtrip time to your kafka cluster)
* and many more planned...

## Installation and Usage

### Installation

`esque` is available at pypi.org and can be installed with `pip install esque`. `esque` requires Python 3.6+ to run.

### Enable Autocompletion

`esque` uses Magic Environment Variables to provide autocompletion to you. You can enable autocompletion by adding the one of the following snippets to your `.bashrc`/`.zshrc` 

#### Bash

```
eval "$(_ESQUE_COMPLETE=source esque)"
```

#### ZSH

```
eval "$(_ESQUE_COMPLETE=source_zsh esque)"
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
  create    Create a new instance of a resource.
  ctx       Switch clusters.
  delete    Delete a resource.
  describe  Get detailed informations about a resource.
  get       Get a quick overview of different resources.
  ping      Tests the connection to the kafka cluster.
```

## Development
```
pipenv install --dev
pipenv shell 
export PYTHONPATH=$(pwd)
```

### Run tests
```
docker-compose up
```

## Alternatives

- [LinkedIn KafkaTools](https://github.com/linkedin/kafka-tools)
- [PyKafka Tools](https://github.com/Parsely/pykafka/blob/master/pykafka/cli/kafka_tools.py)
- [Official Kafka Scripts](https://github.com/apache/kafka/tree/trunk/bin)
- [kafkacat](https://github.com/edenhill/kafkacat)
