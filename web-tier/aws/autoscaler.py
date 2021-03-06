import logging
import boto3
import time
import statistics as st
from botocore.exceptions import ClientError

import constants
import ec2_manager as ec2
import msg_queue as mq

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

MIN_POSSIBLE_INSTANCES = 1
MAX_POSSIBLE_INSTANCES = 15
MAX_REQUESTS_PER_INSTANCE = 5


def get_total_requests_in_sqs():
    try:
        size_values = []
        for _ in range(5):
            size_values.append(int(mq.get_queue_size(constants.REQUEST_QUEUE)))

        logger.info("List of queue sizes fetched from SQS cluster: " + " ".join(str(value) for value in size_values))
        approx_size = st.mode(size_values)
        logger.info("Approx size of Queue: %s", approx_size)
    except ClientError:
        raise
    else:
        return approx_size


def scale_out_app_tier():
    queue_size = get_total_requests_in_sqs()
    logger.info("Total Requests in SQS: %s", queue_size)

    total_required_instances = queue_size // MAX_REQUESTS_PER_INSTANCE
    logger.info("Total required App servers to handle load: %s", total_required_instances)

    current_running_instances = ec2.total_running_instances()
    logger.info("Total currently running App servers: %s", current_running_instances)

    instances_count_to_create = min(MAX_POSSIBLE_INSTANCES, total_required_instances) - current_running_instances
    logger.info("Creating new App server instances: %s", instances_count_to_create)

    ec2.create_instances(constants.AMI_ID, constants.INSTANCE_TYPE, constants.EC2_KEY_PAIR,
                         constants.SECURITY_GROUP_NAME, instances_count_to_create)


def scale_in_app_tier():
    while get_total_requests_in_sqs() != 0:
        time.sleep(1)
        continue

    logger.info("Size of Request SQS is 0. Terminating App Instances...")
    for app in ec2.get_running_instances_by_name(constants.APP_SERVER_NAME):
        ec2.terminate_instance(app.id)


if __name__ == '__main__':
    # get_total_requests_in_sqs()
    scale_in_app_tier()
