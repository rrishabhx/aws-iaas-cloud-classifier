import logging
from botocore.exceptions import ClientError

import subprocess
import boto3
import time
import pickle
from settings import *

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

sqs_resource = boto3.resource('sqs')
sqs_client = boto3.client('sqs')


def fetch_image_from_s3(input_image_id):
    s3 = boto3.client(S3)
    s3.download_file(INPUT_BUCKET, input_image_id, IMAGES_PATH + input_image_id)
    runShellCommand(input_image_id)


def runShellCommand(input_image_id):
    prediction = subprocess.check_output(
        ['python3', 'image_classification.py', IMAGES_PATH + input_image_id, '2>&1', '|', 'tee -a classify.log'])
    prediction_output = prediction.strip().decode('utf-8')
    logger.info("Prediction output= %s", prediction_output)

    prediction_output_serialized = serialize(prediction_output)
    upload_file(input_image_id, prediction_output_serialized)


def serialize(text):
    serialized_object = pickle.dumps(text)
    return serialized_object


def upload_file(input_image_id, serialized_object):
    s3 = boto3.client(S3)
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=input_image_id, Body=serialized_object)


def initiate_app_tier():
    fetch_imageid_from_sqs()


def fetch_imageid_from_sqs():
    messages = receive_messages(REQUEST_QUEUE, MAX_NUMBER_OF_MSGS_TO_FETCH, WAIT_TIME_SECONDS, False)
    for msg in messages:
        fetch_image_from_s3(msg.body)
        delete_message(msg)


def receive_messages(queue_name, max_number, wait_time, to_delete=True):
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
    try:
        message.delete()
        logger.info("Deleted message: %s", message.message_id)
    except ClientError as error:
        logger.exception("Couldn't delete message: %s", message.message_id)
        raise error


def get_queue(name):
    try:
        queue = sqs_resource.get_queue_by_name(QueueName=name)
        logger.info("Got queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        logger.exception("Couldn't get queue named %s.", name)
        raise error
    else:
        return queue


while True:
    initiate_app_tier()
    time.sleep(1)
