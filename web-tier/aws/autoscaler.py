import logging
import time
import statistics as st
from botocore.exceptions import ClientError

from config import *
from . import ec2_manager as ec2
from . import msg_queue as mq

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN)


def get_total_requests_in_sqs():
    try:
        size_values = []
        for _ in range(5):
            size_values.append(int(mq.get_queue_size(REQUEST_QUEUE)))

        logger.warning("List of queue sizes fetched from SQS cluster: " + " ".join(str(value) for value in size_values))
        approx_size = st.mode(size_values)
        logger.warning("Approx size of Queue: %s", approx_size)
    except ClientError as error:
        raise error
    else:
        return approx_size


def get_total_instances_to_create():
    queue_size = get_total_requests_in_sqs()
    logger.warning("Total Requests in SQS: %s", queue_size)

    total_required_instances = queue_size // MAX_REQUESTS_PER_INSTANCE
    logger.warning("Total required App servers to handle load: %s", total_required_instances)

    current_running_instances = ec2.total_running_instances()
    logger.warning("Total currently running App servers: %s", current_running_instances)

    instances_count_to_create = min(MAX_POSSIBLE_INSTANCES, total_required_instances) - current_running_instances
    logger.warning("App server instances to be created: %s", instances_count_to_create)

    return instances_count_to_create


def scale_out_app_tier():
    while get_total_instances_to_create() > 0:
        logger.warning("Starting 1 App-server instance")
        ec2.create_instances(AMI_ID, INSTANCE_TYPE, EC2_KEY_PAIR, SECURITY_GROUP_NAME)
        time.sleep(1)


def scale_in_app_tier():
    logger.warning("Waiting for all requests in Request-Q to be processed")
    while get_total_requests_in_sqs() != 0:
        time.sleep(1)
        continue

    logger.warning("Size of Request SQS is 0. Terminating App Instances...")
    for app in ec2.get_running_instances_by_name(APP_SERVER_NAME):
        ec2.terminate_instance(app.id)


if __name__ == '__main__':
    # get_total_requests_in_sqs()
    scale_in_app_tier()
