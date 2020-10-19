import logging
import re
from typing import Dict, List, Optional, Tuple

import pendulum
import pykafka
from confluent_kafka.cimpl import TopicPartition
from kafka import KafkaAdminClient

from esque.clients.consumer import ConsumerFactory
from esque.cluster import Cluster
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

    def get_consumer_group(self, consumer_id) -> ConsumerGroup:
        return ConsumerGroup(consumer_id, self.cluster)

    def list_consumer_groups(self) -> List[str]:
        brokers: Dict[int, pykafka.broker.Broker] = self.cluster.pykafka_client.cluster.brokers
        return list(
            set(group.decode("UTF-8") for _, broker in brokers.items() for group in broker.list_groups().groups)
        )

    def delete_consumer_groups(self, consumer_ids: List[str]):
        admin_client: KafkaAdminClient = KafkaAdminClient(bootstrap_servers=self.cluster.bootstrap_servers)
        admin_client.delete_consumer_groups(group_ids=consumer_ids)

    def edit_consumer_group_offsets(self, consumer_id: str, offset_plan: List[ConsumerGroupOffsetPlan]):
        """
        Commit consumergroup offsets to specific values
        :param consumer_id: ID of the consumer group
        :param offset_plan: List of ConsumerGroupOffsetPlan objects denoting the offsets for each partition in different topics
        :return:
        """
        consumer = ConsumerFactory().create_consumer(
            group_id=consumer_id,
            topic_name=None,
            output_directory=None,
            last=False,
            avro=False,
            initialize_default_output_directory=False,
            match=None,
            enable_auto_commit=False,
        )

        offsets = [
            TopicPartition(
                topic=plan_element.topic_name, partition=plan_element.partition_id, offset=plan_element.proposed_offset
            )
            for plan_element in offset_plan
            if not plan_element.offset_equal
        ]
        consumer.commit(offsets=offsets)

    def create_consumer_group_offset_change_plan(
        self,
        consumer_id: str,
        topic_name: str,
        offset_to_value: Optional[int],
        offset_by_delta: Optional[int],
        offset_to_timestamp: Optional[str],
        offset_from_group: Optional[str],
    ) -> List[ConsumerGroupOffsetPlan]:

        consumer_group_state, offset_plans = self.read_current_consumer_group_offsets(
            consumer_id=consumer_id, topic_name_expression=topic_name
        )
        if consumer_group_state == "Dead":
            self._logger.error("The consumer group {} does not exist.".format(consumer_id))
            return None
        elif consumer_group_state == "Empty":
            if offset_to_value is not None:
                for plan_element in offset_plans.values():
                    (allowed_offset, error, message) = self.select_new_offset_for_consumer(
                        offset_to_value, plan_element
                    )
                    plan_element.proposed_offset = allowed_offset
                    if error:
                        self._logger.error(message)
            elif offset_by_delta is not None:
                for plan_element in offset_plans.values():
                    requested_offset = plan_element.current_offset + offset_by_delta
                    (allowed_offset, error, message) = self.select_new_offset_for_consumer(
                        requested_offset, plan_element
                    )
                    plan_element.proposed_offset = allowed_offset
                    if error:
                        self._logger.error(message)
            elif offset_to_timestamp is not None:
                timestamp_limit = pendulum.parse(offset_to_timestamp)
                proposed_offset_dict = TopicController(self.cluster, None).get_offsets_closest_to_timestamp(
                    group_id=consumer_id, topic_name=topic_name, timestamp_limit=timestamp_limit
                )
                for plan_element in offset_plans.values():
                    plan_element.proposed_offset = proposed_offset_dict.get(plan_element.partition_id, 0)
            elif offset_from_group is not None:
                _, mirror_consumer_group = self.read_current_consumer_group_offsets(
                    consumer_id=offset_from_group, topic_name_expression=topic_name
                )
                for key, value in mirror_consumer_group.items():
                    if key in offset_plans.keys():
                        offset_plans[key].proposed_offset = value.current_offset
                    else:
                        value.current_offset = 0
                        offset_plans[key] = value
            return list(offset_plans.values())
        else:
            self._logger.error(
                "Consumergroup {} is not empty. Setting offsets is only allowed for empty consumer groups.".format(
                    consumer_id
                )
            )
            return list(offset_plans.values())

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
            return "Dead", offset_plans
        if len(offset_plans) == 0:
            self._logger.warning("No offsets have ever been committed by consumergroup {}.".format(consumer_id))
        return consumer_group_state, offset_plans
