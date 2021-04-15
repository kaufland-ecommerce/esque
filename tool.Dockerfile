FROM python:3.9

RUN git clone --branch "v1.0.0" --depth 1 https://github.com/edenhill/librdkafka.git \
 && cd librdkafka \
 && ./configure --prefix /usr \
 && make \
 && make install

RUN git clone --branch "1.4.0" --depth 1 https://github.com/edenhill/kafkacat.git \
 && cd kafkacat \
 && ./bootstrap.sh \
 && cp kafkacat /usr/bin/ \
 && cd .. \
 && rm -rf kafkacat/

RUN mkdir -p /esque

WORKDIR /esque

RUN pip install -U pip
RUN pip install --pre poetry

COPY . /esque
RUN poetry config "virtualenvs.create" "false"
RUN poetry install

# Create user
RUN useradd -ms /bin/bash esque
USER esque

ENV PATH="/home/esque/.local/bin:$PATH"

RUN mkdir -p /home/esque/work
WORKDIR /home/esque/work

ENTRYPOINT ["esque"]
