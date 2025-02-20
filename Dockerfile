FROM python:3.10

RUN apt-get update && \
    apt-get install -y \
    cmake \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --branch "v1.0.0" --depth 1 https://github.com/edenhill/librdkafka.git \
 && cd librdkafka \
 && ./configure --prefix /usr \
 && make \
 && make install

RUN git clone --branch "1.7.0" --depth 1 https://github.com/edenhill/kafkacat.git \
 && cd kafkacat \
 && ./bootstrap.sh \
 && cp kcat /usr/bin/ \
 && cd .. \
 && rm -rf kafkacat/

RUN mkdir -p /esque

WORKDIR /esque

RUN pip install -U pip
RUN pip install --pre poetry

COPY ./scripts /esque/scripts
COPY ./tests /esque/tests
COPY ./esque /esque/esque
COPY ./pyproject.toml /esque/pyproject.toml
COPY ./poetry.lock /esque/poetry.lock

RUN poetry config "virtualenvs.create" "false"
RUN poetry install
ENTRYPOINT ["/bin/bash"]

CMD ["pytest", "tests/"]