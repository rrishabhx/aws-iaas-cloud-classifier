import boto3
from botocore.exceptions import ClientError

import settings as s

logger = s.init_logger(__name__)

sqs_resource = boto3.resource('sqs')
sqs_client = boto3.client('sqs')


def get_queue(name):
    """
    Gets queue object from name
    :param name: Name of the queue
    :return: SQS queue object
    """
    try:
        queue = sqs_resource.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


def get_queue_size(queue_name):
    """
    Finds approximate number of messages currently in the queue
    :param queue_name: Name of the queue
    :return: Length of the queue
    """
    try:
        queue = get_queue(queue_name)
        size = queue.attributes.get('ApproximateNumberOfMessages')
    except ClientError as error:
        logger.exception("Couldn't find the size of the queue: %s", error)
    else:
        return size


def get_messages_in_flight(queue_name):
    """
    Finds SQS messages that are currently in flight i.e being processed and hasn't
    been deleted thus invisible to other consumers
    :param queue_name:
    :return:
    """
    try:
        queue = get_queue(queue_name)
        size = queue.attributes.get('ApproximateNumberOfMessagesNotVisible')
    except ClientError as error:
        logger.exception("Couldn't find the in-flight messages of the queue: %s", error)
    else:
        return int(size)


def receive_messages(queue_name, max_number, wait_time, to_delete=True):
    """
    Fetch messages from SQS
    :param queue_name: Name of queue
    :param max_number: Max number of messages to fetch
    :param wait_time: Long polling for messages
    :param to_delete: To delete or not after receiving the message
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    try:
        queue = get_queue(queue_name)
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        for msg in messages:
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
            if to_delete:
                delete_message(msg)
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue_name)
        raise error
    else:
        return messages


def delete_message(message):
    """
    Deletes a particular SQS message object
    :param message: SQS message object
    """
    try:
        message.delete()
        logger.info("Deleted message: %s", message.message_id)
    except ClientError as error:
        logger.exception("Couldn't delete message: %s", message.message_id)
        raise error


def send_message(queue_name, message_body, message_attributes=None):
    """
    Sends message to the SQS queue.
    :param queue_name: Name of the queue
    :param message_body: Message body text
    :param message_attributes: Additional message attributes
    :return: The response from SQS that contains the list of successful and failed
             messages.
    """
    queue = get_queue(queue_name)

    if not message_attributes:
        message_attributes = {}
    logger.info("Send message: %s to the queue", message_body)
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
