import json

import pytest
import yaml
from click.testing import CliRunner

from esque.cli.commands import describe_topic, describe_broker

NUMBER_OF_BROKER_OPTIONS = 192  # TODO: Different on gitlab and docker???


@pytest.mark.integration
def test_smoke_test_describe_topic(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic])

    assert result.exit_code == 0


@pytest.mark.integration
def test_describe_topic_to_yaml(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic, "-o", "yaml"])
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    check_described_topic_format(yaml_dict)


@pytest.mark.integration
def test_describe_topic_to_json(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, [topic, "-o", "json"])
    output = result.output
    assert output[0] == "{"
    json_dict = json.loads(output)
    check_described_topic_format(json_dict)


@pytest.mark.integration
def test_describe_broker_to_yaml(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "yaml"])
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    for key in yaml_dict.keys():
        assert key in [
            "advertised.host.name",
            "advertised.listeners",
            "advertised.port",
            "alter.config.policy.class.name",
            "alter.log.dirs.replication.quota.window.num",
            "alter.log.dirs.replication.quota.window.size.seconds",
            "authorizer.class.name",
            "auto.create.topics.enable",
            "auto.leader.rebalance.enable",
            "background.threads",
            "broker.id",
            "broker.id.generation.enable",
            "broker.interceptor.class",
            "broker.rack",
            "client.quota.callback.class",
            "compression.type",
            "confluent.support.customer.id",
            "confluent.support.metrics.enable",
            "connection.failed.authentication.delay.ms",
            "connections.max.idle.ms",
            "connections.max.reauth.ms",
            "control.plane.listener.name",
            "controlled.shutdown.enable",
            "controlled.shutdown.max.retries",
            "controlled.shutdown.retry.backoff.ms",
            "controller.socket.timeout.ms",
            "create.topic.policy.class.name",
            "default.replication.factor",
            "delegation.token.expiry.check.interval.ms",
            "delegation.token.expiry.time.ms",
            "delegation.token.master.key",
            "delegation.token.max.lifetime.ms",
            "delete.records.purgatory.purge.interval.requests",
            "delete.topic.enable",
            "fetch.purgatory.purge.interval.requests",
            "group.initial.rebalance.delay.ms",
            "group.max.session.timeout.ms",
            "group.max.size",
            "group.min.session.timeout.ms",
            "host.name",
            "inter.broker.listener.name",
            "inter.broker.protocol.version",
            "kafka.metrics.polling.interval.secs",
            "kafka.metrics.reporters",
            "leader.imbalance.check.interval.seconds",
            "leader.imbalance.per.broker.percentage",
            "listener.security.protocol.map",
            "listeners",
            "log.cleaner.backoff.ms",
            "log.cleaner.dedupe.buffer.size",
            "log.cleaner.delete.retention.ms",
            "log.cleaner.enable",
            "log.cleaner.io.buffer.load.factor",
            "log.cleaner.io.buffer.size",
            "log.cleaner.io.max.bytes.per.second",
            "log.cleaner.min.cleanable.ratio",
            "log.cleaner.min.compaction.lag.ms",
            "log.cleaner.threads",
            "log.cleanup.policy",
            "log.dir",
            "log.dirs",
            "log.flush.interval.messages",
            "log.flush.interval.ms",
            "log.flush.offset.checkpoint.interval.ms",
            "log.flush.scheduler.interval.ms",
            "log.flush.start.offset.checkpoint.interval.ms",
            "log.index.interval.bytes",
            "log.index.size.max.bytes",
            "log.message.downconversion.enable",
            "log.message.format.version",
            "log.message.timestamp.difference.max.ms",
            "log.message.timestamp.type",
            "log.preallocate",
            "log.retention.bytes",
            "log.retention.check.interval.ms",
            "log.retention.hours",
            "log.retention.minutes",
            "log.retention.ms",
            "log.roll.hours",
            "log.roll.jitter.hours",
            "log.roll.jitter.ms",
            "log.roll.ms",
            "log.segment.bytes",
            "log.segment.delete.delay.ms",
            "max.connections.per.ip",
            "max.connections.per.ip.overrides",
            "max.incremental.fetch.session.cache.slots",
            "message.max.bytes",
            "metric.reporters",
            "metrics.num.samples",
            "metrics.recording.level",
            "metrics.sample.window.ms",
            "min.insync.replicas",
            "num.io.threads",
            "num.network.threads",
            "num.partitions",
            "num.recovery.threads.per.data.dir",
            "num.replica.alter.log.dirs.threads",
            "num.replica.fetchers",
            "offset.metadata.max.bytes",
            "offsets.commit.required.acks",
            "offsets.commit.timeout.ms",
            "offsets.load.buffer.size",
            "offsets.retention.check.interval.ms",
            "offsets.retention.minutes",
            "offsets.topic.compression.codec",
            "offsets.topic.num.partitions",
            "offsets.topic.replication.factor",
            "offsets.topic.segment.bytes",
            "password.encoder.cipher.algorithm",
            "password.encoder.iterations",
            "password.encoder.key.length",
            "password.encoder.keyfactory.algorithm",
            "password.encoder.old.secret",
            "password.encoder.secret",
            "port",
            "principal.builder.class",
            "producer.purgatory.purge.interval.requests",
            "queued.max.request.bytes",
            "queued.max.requests",
            "quota.consumer.default",
            "quota.producer.default",
            "quota.window.num",
            "quota.window.size.seconds",
            "replica.fetch.backoff.ms",
            "replica.fetch.max.bytes",
            "replica.fetch.min.bytes",
            "replica.fetch.response.max.bytes",
            "replica.fetch.wait.max.ms",
            "replica.high.watermark.checkpoint.interval.ms",
            "replica.lag.time.max.ms",
            "replica.socket.receive.buffer.bytes",
            "replica.socket.timeout.ms",
            "replication.quota.window.num",
            "replication.quota.window.size.seconds",
            "request.timeout.ms",
            "reserved.broker.max.id",
            "sasl.client.callback.handler.class",
            "sasl.enabled.mechanisms",
            "sasl.jaas.config",
            "sasl.kerberos.kinit.cmd",
            "sasl.kerberos.min.time.before.relogin",
            "sasl.kerberos.principal.to.local.rules",
            "sasl.kerberos.service.name",
            "sasl.kerberos.ticket.renew.jitter",
            "sasl.kerberos.ticket.renew.window.factor",
            "sasl.login.callback.handler.class",
            "sasl.login.class",
            "sasl.login.refresh.buffer.seconds",
            "sasl.login.refresh.min.period.seconds",
            "sasl.login.refresh.window.factor",
            "sasl.login.refresh.window.jitter",
            "sasl.mechanism.inter.broker.protocol",
            "sasl.server.callback.handler.class",
            "security.inter.broker.protocol",
            "socket.receive.buffer.bytes",
            "socket.request.max.bytes",
            "socket.send.buffer.bytes",
            "ssl.cipher.suites",
            "ssl.client.auth",
            "ssl.enabled.protocols",
            "ssl.endpoint.identification.algorithm",
            "ssl.key.password",
            "ssl.keymanager.algorithm",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.keystore.type",
            "ssl.principal.mapping.rules",
            "ssl.protocol",
            "ssl.provider",
            "ssl.secure.random.implementation",
            "ssl.trustmanager.algorithm",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.truststore.type",
            "transaction.abort.timed.out.transaction.cleanup.interval.ms",
            "transaction.max.timeout.ms",
            "transaction.remove.expired.transaction.cleanup.interval.ms",
            "transaction.state.log.load.buffer.size",
            "transaction.state.log.min.isr",
            "transaction.state.log.num.partitions",
            "transaction.state.log.replication.factor",
            "transaction.state.log.segment.bytes",
            "transactional.id.expiration.ms",
            "unclean.leader.election.enable",
            "zookeeper.connect",
            "zookeeper.connection.timeout.ms",
            "zookeeper.max.in.flight.requests",
            "zookeeper.session.timeout.ms",
            "zookeeper.set.acl",
            "zookeeper.sync.time.ms",
        ]
    check_described_broker_format(yaml_dict)


@pytest.mark.integration
def test_describe_broker_to_json(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, [broker_id, "-o", "json"])
    output = result.output
    assert output[0] == "{"
    json_dict = json.loads(output)
    check_described_broker_format(json_dict)


@pytest.mark.integration
def test_describe_topic_from_stdin(cli_runner: CliRunner, topic: str):
    result = cli_runner.invoke(describe_topic, ["-o", "yaml"], topic)
    output = result.output
    assert output[0] != "{"
    yaml_dict = yaml.safe_load(output)
    check_described_topic_format(yaml_dict)


@pytest.mark.integration
def test_describe_broker_from_stdin(cli_runner: CliRunner, broker_id: str):
    result = cli_runner.invoke(describe_broker, ["-o", "json"], broker_id)
    output = result.output
    assert output[0] == "{"
    yaml_dict = json.loads(output)
    check_described_broker_format(yaml_dict)


def check_described_topic_format(described_topic: dict):
    keys = described_topic.keys()
    assert "Topic" in keys
    assert sum("Partition" in key for key in keys) == len(keys) - 2
    assert "Config" in keys


def check_described_broker_format(described_broker: dict):
    keys = described_broker.keys()
    assert len(keys) == NUMBER_OF_BROKER_OPTIONS
    config_options = ["advertised.host.name", "advertised.listeners", "advertised.port", "zookeeper.session.timeout.ms"]
    for option in config_options:
        assert option in keys
