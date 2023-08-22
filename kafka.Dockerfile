FROM openjdk:11-jdk-stretch


RUN sed -i s/deb.debian.org/archive.debian.org/g /etc/apt/sources.list
RUN sed -i 's|security.debian.org|archive.debian.org|g' /etc/apt/sources.list
RUN sed -i '/stretch-updates/d' /etc/apt/sources.list

RUN apt-get update -q       \
 && apt install             \
    -qqy -o=Dpkg::Use-Pty=0 \
    wget -y                 \
 && rm -rf /var/lib/apt/lists/*

RUN wget --no-verbose        \
         --show-progress     \
         --progress=dot:mega \
         http://packages.confluent.io/archive/5.2/confluent-community-5.2.6-2.12.tar.gz -O confluent-community.tgz \
 && mkdir -p                        \
          confluent-community       \
 && tar xzf confluent-community.tgz \
        -C confluent-community      \
        --strip-components 1        \
 && rm confluent-community.tgz

COPY ./scripts/wait-for-it.sh wait-for-it.sh
COPY ./scripts/init.sh init.sh
