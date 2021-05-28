#!/usr/bin/env bash

esque(){
  docker run \
    --mount type=bind,source="$(pwd)",target=/home/esque/work \
    --mount type=bind,source="${HOME}/.esque/esque_config.yaml",target=/home/esque/esque_config.yaml \
    --rm \
    -it \
    -e ESQUE_CONF_PATH=/home/esque/esque_config.yaml \
    esque "${@}"
}
