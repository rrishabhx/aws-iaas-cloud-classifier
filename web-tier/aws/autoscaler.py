import logging
import time
import math
import statistics as st
from botocore.exceptions import ClientError

from settings import *
from aws import ec2_manager as ec2
from aws import msg_queue as mq

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_total_requests_in_sqs():
    try:
        size_values = []
        for _ in range(5):
            size_values.append(int(mq.get_queue_size(REQUEST_QUEUE)))

        logger.info("List of queue sizes fetched from SQS cluster: " + " ".join(str(value) for value in size_values))
        approx_size = st.mode(size_values)
        logger.info("Approx size of Queue: %s", approx_size)
    except ClientError as error:
        raise error
    else:
        return approx_size


def get_total_instances_to_create():
    queue_size = get_total_requests_in_sqs()
    logger.info("Total Requests in SQS: %s", queue_size)

    total_required_instances = math.ceil(queue_size / MAX_REQUESTS_PER_INSTANCE)
    logger.info("Total required App servers to handle load: %s", total_required_instances)

    logger.info("Total currently running App servers: %s", len(currently_running_instances))

    instances_count_to_create = min(MAX_POSSIBLE_INSTANCES, total_required_instances) - len(currently_running_instances)
    logger.info("App server instances to be created: %s", instances_count_to_create)

    return instances_count_to_create


def scale_out_app_tier():
    while get_total_instances_to_create() > 0:
        logger.info("Starting 1 App-server instance")
        instance_id = ec2.create_instances(AMI_ID_APP_2, INSTANCE_TYPE, EC2_KEY_PAIR, SECURITY_GROUP_NAME)
        currently_running_instances.add(instance_id)
        time.sleep(1)


def scale_in_app_tier():
    logger.info("Waiting for all requests in Request-Q to be processed")
    while mq.receive_messages(REQUEST_QUEUE, 1, WAIT_TIME_SECONDS, False) and mq.get_messages_in_flight(
            REQUEST_QUEUE) > 0:
        time.sleep(1)
        continue

    logger.info("Size of Request SQS is 0. Terminating App Instances...")
    for app in ec2.get_running_instances_by_name(APP_SERVER_NAME):
        try:
            ec2.terminate_instance(app.id)
            currently_running_instances.remove(app.id)
        except:
            pass


if __name__ == '__main__':
    # get_total_requests_in_sqs()
    scale_in_app_tier()
