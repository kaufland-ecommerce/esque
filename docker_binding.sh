#!/usr/bin/env bash

esque(){
  docker run \
    --mount type=bind,source="$(pwd)",target=/home/esque/work \
    --mount type=bind,source="${HOME}/.esque/config.yaml",target=/home/esque/config.yaml \
    -e ESQUE_CONF_PATH=/home/esque/config.yaml \
    -e PYTHONPATH=/esque \
    esque "${@}"
}
