import logging
import time

import boto3
from botocore.exceptions import ClientError

import constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

sqs_resource = boto3.resource('sqs')
sqs_client = boto3.client('sqs')
queue = sqs_resource.get_queue_by_name(QueueName=constants.INPUT_QUEUE)


def create_q(name, attributes=None):
    sqs_resource.create_queue(QueueName=name, Attributes=attributes)


def get_queue(name):
    """
    Gets an SQS queue by name.

    Usage is shown in usage_demo at the end of this module.

    :param name: The name that was used to create the queue.
    :return: A Queue object.
    """
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


def get_msg_from_q():
    print("Printing Messages stored in the SQS\n")
    for msg in queue.receive_messages():
        print(msg.body)

        msg.delete()


def receive_messages(queue, max_number, wait_time):
    """
    Receive a batch of messages in a single request from an SQS queue.

    Usage is shown in usage_demo at the end of this module.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
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
    """
    Delete a message from a queue. Clients must delete messages after they
    are received and processed to remove them from the queue.

    Usage is shown in usage_demo at the end of this module.

    :param message: The message to delete. The message's queue URL is contained in
                    the message's metadata.
    :return: None
    """
    try:
        message.delete()
        logger.info("Deleted message: %s", message.message_id)
    except ClientError as error:
        logger.exception("Couldn't delete message: %s", message.message_id)
        raise error


def send_msg_to_q(body):
    return queue.send_message(MessageBody=body)


if __name__ == '__main__':
    # queue_url = sqs_client.get_queue_url(QueueName=constants.INPUT_QUEUE)['QueueUrl']
    #
    # Attributes = {'ReceiveMessageWaitTimeSeconds': '2'}
    # update_q_attributes(queue_url, Attributes)

    # for i in range(5):
    #     send_msg_to_q("What on earth is this?")

    while True:
        receive_messages(get_queue(constants.INPUT_QUEUE), 10, 3)
        time.sleep(2)
