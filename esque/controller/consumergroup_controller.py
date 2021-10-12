import logging
import re
from typing import Dict, List, Optional, Tuple

import pendulum
from confluent_kafka.cimpl import Consumer, TopicPartition

from esque.cluster import Cluster
from esque.config import Config
from esque.controller.topic_controller import TopicController
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

    @property
    def offset_equal(self) -> bool:
        return self.current_offset == self.proposed_offset


class ConsumerGroupController:
    def __init__(self, cluster: Cluster):
        self.cluster = cluster
        self._logger = logging.getLogger(__name__)

    def get_consumer_group(self, consumer_id: str) -> ConsumerGroup:
        return ConsumerGroup(consumer_id, self.cluster)

    def create_consumer_group(self, consumer_id: str, offsets: List[TopicPartition]) -> ConsumerGroup:
        self.commit_offsets(consumer_id, offsets)
        return ConsumerGroup(consumer_id, self.cluster)

    def list_consumer_groups(self, prefix: str = "") -> List[str]:
        return [
            group
            for group, _protocol in self.cluster.kafka_python_client.list_consumer_groups()
            if group.startswith(prefix)
        ]

    def delete_consumer_groups(self, consumer_ids: List[str]):
        self.cluster.kafka_python_client.delete_consumer_groups(group_ids=consumer_ids)

    def commit_offsets(self, consumer_id: str, offsets: List[TopicPartition]):
        config = Config.get_instance()
        consumer = Consumer({"group.id": consumer_id, **config.create_confluent_config()})
        consumer.commit(offsets=offsets, asynchronous=False)
        consumer.close()

    def edit_consumer_group_offsets(self, consumer_id: str, offset_plan: List[ConsumerGroupOffsetPlan]):
        """
        Commit consumergroup offsets to specific values
        :param consumer_id: ID of the consumer group
        :param offset_plan: List of ConsumerGroupOffsetPlan objects denoting the offsets for each partition in different topics
        :return:
        """

        offsets = [
            TopicPartition(
                topic=plan_element.topic_name, partition=plan_element.partition_id, offset=plan_element.proposed_offset
            )
            for plan_element in offset_plan
            if not plan_element.offset_equal
        ]
        self.commit_offsets(consumer_id, offsets)

    def create_consumer_group_offset_change_plan(
        self,
        consumer_id: str,
        topic_name: str,
        offset_to_value: Optional[int],
        offset_by_delta: Optional[int],
        offset_to_timestamp: Optional[str],
        offset_from_group: Optional[str],
    ) -> Optional[List[ConsumerGroupOffsetPlan]]:

        consumer_group_state, offset_plans = self.read_current_consumer_group_offsets(
            consumer_id=consumer_id, topic_name_expression=topic_name
        )
        if consumer_group_state == "Dead":
            self._logger.error(f"The consumer group {consumer_id} does not exist.")
            return None
        if consumer_group_state != "Empty":
            self._logger.error(
                f"Consumergroup {consumer_id} has active consumers. Please turn off all consumers in this group and try again."
            )
            return None

        if offset_to_value is not None:
            self._set_offset_to_value(offset_plans, offset_to_value)
        elif offset_by_delta is not None:
            self._set_offset_by_delta(offset_by_delta, offset_plans)
        elif offset_to_timestamp is not None:
            self._set_offset_to_timestamp(offset_plans, offset_to_timestamp, topic_name)
        elif offset_from_group is not None:
            self._set_offset_from_group(offset_from_group, offset_plans, topic_name)
        return list(offset_plans.values())

    def _set_offset_from_group(self, offset_from_group, offset_plans, topic_name):
        _, mirror_consumer_group = self.read_current_consumer_group_offsets(
            consumer_id=offset_from_group, topic_name_expression=topic_name
        )
        for key, value in mirror_consumer_group.items():
            if key in offset_plans.keys():
                offset_plans[key].proposed_offset = value.current_offset
            else:
                value.current_offset = 0
                offset_plans[key] = value

    def _set_offset_to_timestamp(self, offset_plans, offset_to_timestamp, topic_name):
        timestamp_limit = pendulum.parse(offset_to_timestamp)
        proposed_offset_dict = TopicController(self.cluster).get_offsets_closest_to_timestamp(
            topic_name=topic_name, timestamp=timestamp_limit
        )
        for plan_element in offset_plans.values():
            plan_element.proposed_offset = proposed_offset_dict[plan_element.partition_id].offset

    def _set_offset_by_delta(self, offset_by_delta, offset_plans):
        for plan_element in offset_plans.values():
            requested_offset = plan_element.current_offset + offset_by_delta
            (allowed_offset, error, message) = self.select_new_offset_for_consumer(requested_offset, plan_element)
            plan_element.proposed_offset = allowed_offset
            if error:
                self._logger.error(message)

    def _set_offset_to_value(self, offset_plans, offset_to_value):
        for plan_element in offset_plans.values():
            (allowed_offset, error, message) = self.select_new_offset_for_consumer(offset_to_value, plan_element)
            plan_element.proposed_offset = allowed_offset
            if error:
                self._logger.error(message)

    @staticmethod
    def select_new_offset_for_consumer(
        requested_offset: int, offset_plan: ConsumerGroupOffsetPlan
    ) -> Tuple[int, bool, str]:
        from esque.cli.output import red_bold

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

    def read_current_consumer_group_offsets(
        self, consumer_id: str, topic_name_expression: str
    ) -> Tuple[str, Dict[str, ConsumerGroupOffsetPlan]]:
        offset_plans = {}
        topic_name_pattern = re.compile(topic_name_expression, re.IGNORECASE)
        try:
            consumer_group = self.get_consumer_group(consumer_id)
            consumer_group_desc = consumer_group.describe(partitions=True)
            consumer_group_state = consumer_group_desc["meta"]["state"]
            for subscribed_topic_name in consumer_group_desc["offsets"]:
                decoded_topic_name = subscribed_topic_name
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
            return "Dead", offset_plans
        if len(offset_plans) == 0:
            self._logger.warning("No offsets have ever been committed by consumergroup {}.".format(consumer_id))
        return consumer_group_state, offset_plans
