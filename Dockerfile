FROM python:3.6

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

COPY ./ /esque

RUN apt-get update && apt-get install

RUN pip3 install pipenv

RUN pipenv install --system --dev --deploy

ENTRYPOINT ["/bin/bash"]

CMD ["pytest", "tests/"]
