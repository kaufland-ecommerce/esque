FROM python:3.9

RUN apt-get update \
    && apt-get upgrade \
    && apt-get install --yes --no-install-recommends \
        kafkacat \
        vim \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && pip install -U pip \
    && pip install --pre poetry

WORKDIR /esque

COPY . /esque
RUN poetry config "virtualenvs.create" "false"
RUN poetry install

# Create user
RUN useradd -ms /bin/bash esque
USER esque

WORKDIR /home/esque/work

ENTRYPOINT ["esque"]
