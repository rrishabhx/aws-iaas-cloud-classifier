import logging
import time
import math
import statistics as st
from botocore.exceptions import ClientError

import settings as s
from aws import ec2_manager as ec2
from aws import msg_queue as mq

logger = s.init_logger(__name__)


def get_total_requests_in_sqs():
    try:
        time.sleep(1)
        size_values = []
        for _ in range(5):
            size_values.append(int(mq.get_queue_size(s.REQUEST_QUEUE)))

        logger.info("List of queue sizes fetched from SQS cluster: " + " ".join(str(value) for value in size_values))
        approx_size = st.multimode(size_values)[0]
        logger.info("Approx size of Queue: %s", approx_size)
    except ClientError as error:
        raise error
    else:
        return approx_size


def get_total_instances_to_create():
    queue_size = get_total_requests_in_sqs()
    logger.info("Total Requests in SQS: %s", queue_size)

    total_required_instances = math.ceil(queue_size / s.MAX_REQUESTS_PER_INSTANCE)
    logger.info("Total required App servers to handle load: %s", total_required_instances)

    logger.info("Total currently running App servers: %s", len(s.currently_running_instances))

    instances_count_to_create = min(s.MAX_POSSIBLE_INSTANCES, total_required_instances) - len(s.currently_running_instances)
    logger.info("App server instances to be created: %s", instances_count_to_create)

    return instances_count_to_create


def scale_out_app_tier():
    created_app_servers = []
    while get_total_instances_to_create() > 0:
        logger.info("Starting 1 App-server instance")
        instance_id = ec2.create_instances(s.AMI_ID_APP_2, s.INSTANCE_TYPE, s.EC2_KEY_PAIR, s.SECURITY_GROUP_NAME)
        s.currently_running_instances.add(instance_id)
        created_app_servers.append(instance_id)

    return created_app_servers


def scale_in_app_tier(created_app_servers):
    logger.info("Waiting for all requests in Request-Q to be processed")
    while mq.receive_messages(s.REQUEST_QUEUE, 1, s.WAIT_TIME_SECONDS, False) or mq.get_messages_in_flight(
            s.REQUEST_QUEUE) > 0:
        time.sleep(1)
        continue

    logger.info("Size of Request SQS is 0. Terminating App Instances...")
    for app_id in created_app_servers:
        try:
            ec2.terminate_instance(app_id)
            s.currently_running_instances.remove(app_id)
        except ClientError:
            logger.info("Couldn't terminate instance: %s. It must already be down.", app_id)


if __name__ == '__main__':
    # get_total_requests_in_sqs()
    scale_in_app_tier()
