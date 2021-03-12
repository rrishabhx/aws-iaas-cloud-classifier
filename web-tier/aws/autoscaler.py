import math
import time
from statistics import multimode

from botocore.exceptions import ClientError

import settings as s
from aws import ec2_manager as ec2
from aws import msg_queue as mq

logger = s.init_logger(__name__)


def get_total_requests_in_sqs():
    """
    Runs a loop to find the Request queue sizes and takes a Mode over them to get the approximate size of queue
    :return: Size of the request queue in int.
    """
    try:
        time.sleep(1)
        size_values = []
        for _ in range(5):
            size_values.append(int(mq.get_queue_size(s.REQUEST_QUEUE)))

        logger.info("List of queue sizes fetched from SQS cluster: " + " ".join(str(value) for value in size_values))
        approx_size = multimode(size_values)[0]
        logger.info("Approx size of Queue: %s", approx_size)
    except ClientError as error:
        raise error
    else:
        return approx_size


def get_total_instances_to_create():
    """
    Calculates total App-Tier instances to create for the current request.
    :return: Count of app-tier instances to create
    """
    queue_size = get_total_requests_in_sqs()
    logger.info("Total Requests in SQS: %s", queue_size)

    total_required_instances = math.ceil(queue_size / s.MAX_REQUESTS_PER_INSTANCE)
    logger.info("Total required App servers to handle load: %s", total_required_instances)

    logger.info("Total currently running App servers: %s", s.MAX_POSSIBLE_INSTANCES - s.instance_q().qsize())

    instances_count_to_create = min(s.MAX_POSSIBLE_INSTANCES, total_required_instances) - (
            s.MAX_POSSIBLE_INSTANCES - s.instance_q().qsize())

    logger.info("App server instances to be created: %s", instances_count_to_create)

    return instances_count_to_create


def scale_out_app_tier():
    """
    Horizontally scales-out the app-tier instances
    :return: Dictionary of instance-ids and instance-names as key-value pairs
    """
    created_app_servers = {}
    while get_total_instances_to_create() > 0 and not s.instance_q().empty():
        logger.info("Starting 1 App-server instance")
        instance_name = f"{s.APP_SERVER_NAME}-{s.instance_q().get()}"
        instance_id = ec2.create_app_instances(s.AMI_ID_APP_2, s.INSTANCE_TYPE, s.EC2_KEY_PAIR, instance_name,
                                               s.SECURITY_GROUP_NAME)
        created_app_servers[instance_id] = instance_name

    return created_app_servers


def scale_in_app_tier(created_app_servers):
    """
    Scales-in the app-tier tier instances spawned for current request by terminating them one by one
    :param created_app_servers: Dictionary of instance-ids and instance-names as key-value pairs
    """
    logger.info("Waiting for all requests in Request-Q to be processed")
    while mq.receive_messages(s.REQUEST_QUEUE, 1, s.WAIT_TIME_SECONDS, False) or mq.get_messages_in_flight(
            s.REQUEST_QUEUE) > 0:
        time.sleep(1)
        continue

    logger.info("Size of Request SQS is 0. Terminating App Instances...")
    for app_id, app_name in created_app_servers.items():
        try:
            ec2.terminate_instance(app_id)
            num = int(app_name.split("-")[2])
            s.instance_q().put(num)
        except ClientError:
            logger.info("Error occurred while terminating instance: %s. It could already be down.", app_id)
