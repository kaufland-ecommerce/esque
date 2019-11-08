import logging
import re
from typing import Dict, List, Optional

import pendulum
import pykafka
from confluent_kafka.cimpl import TopicPartition

from esque.cli.output import red_bold
from esque.clients.consumer import ConsumerFactory
from esque.cluster import Cluster
from esque.config import Config
from esque.controller.topic_controller import TopicController
from esque.errors import translate_third_party_exceptions
from esque.resources.consumergroup import ConsumerGroup


class ConsumerGroupOffsetPlan:
    def __init__(
        self,
        topic_name: str,
        current_offset: int,
        proposed_offset: int,
        high_watermark: int,
        low_watermark: int,
        partition_id: int,
    ) -> None:
        self.topic_name = topic_name
        self.current_offset = current_offset
        self.proposed_offset = proposed_offset
        self.high_watermark = high_watermark
        self.low_watermark = low_watermark
        self.partition_id = partition_id


class ConsumerGroupController:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster
        self._logger = logging.getLogger(__name__)

    @translate_third_party_exceptions
    def get_consumergroup(self, consumer_id) -> ConsumerGroup:
        return ConsumerGroup(consumer_id, self.cluster)

    @translate_third_party_exceptions
    def list_consumer_groups(self) -> List[str]:
        brokers: Dict[int, pykafka.broker.Broker] = self.cluster.pykafka_client.cluster.brokers
        return list(
            set(group.decode("UTF-8") for _, broker in brokers.items() for group in broker.list_groups().groups)
        )

    @translate_third_party_exceptions
    def edit_consumer_group_offsets(self, consumer_id: str, offset_plan: List[ConsumerGroupOffsetPlan]):
        """
        Commit consumergroup offsets to specific values
        :param consumer_id: ID of the consumer group
        :param offset_plan: List of ConsumerGroupOffsetPlan objects denoting the offsets for each partition in different topics
        :return:
        """
        _config = Config().create_confluent_config()
        _config.update({"group.id": consumer_id, "enable.auto.commit": False})
        consumer = ConsumerFactory().create_custom_consumer(_config)
        partitions = [
            TopicPartition(
                topic=plan_element.topic_name, partition=plan_element.partition_id, offset=plan_element.proposed_offset
            )
            for plan_element in filter(lambda el: el.current_offset != el.proposed_offset, offset_plan)
        ]
        consumer.commit(offsets=partitions)

    def create_consumer_group_offset_change_plan(
        self,
        consumer_id: str,
        topic_name: str,
        offset_to_value: Optional[int],
        offset_by_delta: Optional[int],
        offset_to_timestamp: Optional[str],
        offset_from_group: Optional[str],
        force: bool,
    ) -> List[ConsumerGroupOffsetPlan]:

        consumer_group_state, offset_plans = self._read_current_consumergroup_offsets(
            consumer_id=consumer_id, topic_name_expression=topic_name
        )
        if consumer_group_state == "Empty" or force:
            if offset_to_value:
                for _, plan_element in offset_plans.items():
                    (allowed_offset, error, message) = self._select_new_offset_for_consumer(
                        offset_to_value, plan_element
                    )
                    plan_element.proposed_offset = allowed_offset
                    if error:
                        self._logger.error(message)
            elif offset_by_delta:
                for _, plan_element in offset_plans.items():
                    requested_offset = plan_element.current_offset + offset_by_delta
                    (allowed_offset, error, message) = self._select_new_offset_for_consumer(
                        requested_offset, plan_element
                    )
                    plan_element.proposed_offset = allowed_offset
                    if error:
                        self._logger.error(message)
            elif offset_to_timestamp:
                timestamp_limit = pendulum.parse(offset_to_timestamp)
                current_offset_dict = TopicController(self.cluster, None).get_offsets_closest_to_timestamp(
                    topic_name=topic_name, timestamp_limit=timestamp_limit
                )
                for _, plan_element in offset_plans.items():
                    plan_element.current_offset = current_offset_dict.get(plan_element.partition_id, 0)
            elif offset_from_group:
                _, mirror_consumer_group = self._read_current_consumergroup_offsets(
                    consumer_id=offset_from_group, topic_name_expression=topic_name
                )
                for key, value in mirror_consumer_group.items():
                    if key in offset_plans.keys():
                        offset_plans[key].proposed_offset = value.current_offset
                    else:
                        value.current_offset = 0
                        offset_plans[key] = value
            return list(offset_plans.values())
        elif consumer_group_state != "Dead":
            self._logger.error(
                "Consumergroup {} is not empty. Use the {} option if you want to override this safety mechanism.".format(
                    consumer_id, red_bold("--force")
                )
            )
            return list(offset_plans.values())

    def _select_new_offset_for_consumer(self, requested_offset: int, offset_plan: ConsumerGroupOffsetPlan):
        if requested_offset < offset_plan.low_watermark:
            final_value = offset_plan.low_watermark
            error = True
            message = "The requested offset ({}) is outside of the allowable range [{},{}] for partition {} in topic {}. Setting to low watermark.".format(
                requested_offset,
                red_bold(str(offset_plan.low_watermark)),
                offset_plan.high_watermark,
                offset_plan.partition_id,
                offset_plan.topic_name,
            )
        elif requested_offset > offset_plan.high_watermark:
            final_value = offset_plan.high_watermark
            error = True
            message = "The requested offset ({}) is outside of the allowable range [{},{}] for partition {} in topic {}. Setting to high watermark.".format(
                requested_offset,
                offset_plan.low_watermark,
                red_bold(str(offset_plan.high_watermark)),
                offset_plan.partition_id,
                offset_plan.topic_name,
            )
        else:
            final_value = requested_offset
            error = False
            message = ""
        return final_value, error, message

    def _read_current_consumergroup_offsets(self, consumer_id: str, topic_name_expression: str):
        offset_plans = {}
        topic_name_pattern = re.compile(topic_name_expression, re.IGNORECASE)
        consumer_group_state = "Stable"
        try:
            consumer_group = self.get_consumergroup(consumer_id)
            consumer_group_desc = consumer_group.describe(verbose=True)
            consumer_group_state = consumer_group_desc["meta"]["state"].decode("UTF-8")
            for subscribed_topic_name in consumer_group_desc["offsets"]:
                decoded_topic_name = subscribed_topic_name.decode("UTF-8")
                if topic_name_pattern.match(decoded_topic_name):
                    for partition_id, partition_info in consumer_group_desc["offsets"][subscribed_topic_name].items():
                        consumer_offset_plan = ConsumerGroupOffsetPlan(
                            topic_name=decoded_topic_name,
                            current_offset=partition_info["consumer_offset"],
                            proposed_offset=partition_info["consumer_offset"],
                            high_watermark=partition_info["topic_high_watermark"],
                            low_watermark=partition_info["topic_low_watermark"],
                            partition_id=partition_id,
                        )
                        offset_plans[f"{decoded_topic_name}::{partition_id}"] = consumer_offset_plan
        except AttributeError:
            self._logger.error("Consumergroup {} not available.".format(consumer_id))
            return "Dead", offset_plans
        if len(offset_plans) == 0:
            self._logger.error("No offsets have ever been committed by consumergroup {}.".format(consumer_id))
        return consumer_group_state, offset_plans
