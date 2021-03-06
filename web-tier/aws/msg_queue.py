import logging
import time

import boto3
from botocore.exceptions import ClientError

import constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR)

sqs_resource = boto3.resource('sqs')
sqs_client = boto3.client('sqs')


def create_q(name, attributes=None):
    sqs_resource.create_queue(QueueName=name, Attributes=attributes)


def get_queue(name):
    try:
        queue = sqs_resource.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def get_queue_size(queue_name):
    try:
        queue = get_queue(queue_name)
        size = queue.attributes.get('ApproximateNumberOfMessages')
    except ClientError as error:
        logger.exception("Couldn't find the size of the queue: %s", queue_name)
    else:
        return size


def update_q_attributes(queue_url, attributes):
    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes=attributes
    )


def receive_messages(queue, max_number, wait_time):
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        for msg in messages:
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
            delete_message(msg)
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        return messages


def delete_message(message):
    try:
        message.delete()
        logger.info("Deleted message: %s", message.message_id)
    except ClientError as error:
        logger.exception("Couldn't delete message: %s", message.message_id)
        raise error


def send_message(queue, message_body, message_attributes=None):
    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes
        )
    except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        raise error
    else:
        return response


if __name__ == '__main__':
    q = get_queue(constants.REQUEST_QUEUE)
    q_url = sqs_client.get_queue_url(QueueName=constants.REQUEST_QUEUE, QueueOwnerAWSAccountId="551493253543")

    # for i in range(1000):
    #     send_message(q, "What on earth is this?")
    #     time.sleep(1)

    # size = get_queue_size(q)
    # # print("Queue size:", get_queue_size(q))

    while True:
        receive_messages(q, 10, 3)
        # get_queue_size(q)
        # print("Queue size:", get_queue_size(q))
        # time.sleep(3)
