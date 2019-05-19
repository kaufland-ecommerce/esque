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

To run all tests, just run

```
docker-compose up
```

While the `docker-compose` stack is up, you can also run the tests from the CLI via `pytest tests/ --integration --local` 

