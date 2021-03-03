import logging
import time

import boto3
from botocore.exceptions import ClientError

import constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
    q = get_queue(constants.INPUT_QUEUE)

    for i in range(5):
        send_message(q, "What on earth is this?")

    while True:
        receive_messages(q, 10, 3)
        time.sleep(1)
