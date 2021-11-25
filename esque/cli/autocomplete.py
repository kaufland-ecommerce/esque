from esque.cli.options import State
from esque.controller.consumergroup_controller import ConsumerGroupController
from esque.resources.broker import Broker


def list_brokers(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    all_broker_hosts_names = [f"{broker.host}:{broker.port}" for broker in Broker.get_all(state.cluster)]
    return [broker for broker in all_broker_hosts_names if broker.startswith(incomplete)]


def list_consumergroups(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    return [
        group
        for group in ConsumerGroupController(state.cluster).list_consumer_groups()
        if group.startswith(incomplete)
    ]


def list_contexts(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    return [context for context in state.config.available_contexts if context.startswith(incomplete)]


def list_topics(ctx, args, incomplete):
    state = ctx.ensure_object(State)
    cluster = state.cluster
    return [
        topic.name for topic in cluster.topic_controller.list_topics(search_string=incomplete, get_topic_objects=False)
    ]
