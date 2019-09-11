FROM openjdk:11-jdk-stretch

RUN apt-get update && apt install wget -y && rm -rf /var/lib/apt/lists/*

RUN wget http://packages.confluent.io/archive/5.2/confluent-community-5.2.1-2.12.tar.gz -O confluent-community.tgz
RUN mkdir -p confluent-community && tar xzf confluent-community.tgz -C confluent-community --strip-components 1 && rm confluent-community.tgz

COPY ./scripts/wait-for-it.sh wait-for-it.sh
COPY ./scripts/init.sh init.sh