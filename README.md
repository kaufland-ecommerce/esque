# Kafkaesque - a kafka tool.

[![pypi Version](https://img.shields.io/pypi/v/esque.svg)](https://pypi.org/project/esque/) [![Python Versions](https://img.shields.io/pypi/pyversions/esque.svg)](https://pypi.org/project/esque/) [![Build Status](https://travis-ci.org/real-digital/esque.svg?branch=master)](https://travis-ci.org/real-digital/esque) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

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
```
pipenv install --dev
pipenv shell 
export PYTHONPATH=$(pwd)
```

### Run tests
```
docker-compose up
```


