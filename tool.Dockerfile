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
    && apt-get clean \
    && pip install -U pip

WORKDIR /esque

RUN pip install -U pip
RUN pip install --pre poetry

COPY . /esque
RUN poetry config "virtualenvs.create" "false"
RUN poetry install

# Create user
RUN useradd -ms /bin/bash esque
USER esque

WORKDIR /home/esque/work

ENTRYPOINT ["esque"]
