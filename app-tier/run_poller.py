import pickle
import subprocess
import time

import boto3
from botocore.exceptions import ClientError

from settings import *

logger = init_logger(__name__)

sqs_resource = boto3.resource('sqs')
sqs_client = boto3.client('sqs')


def fetch_image_from_s3(input_image_id):
    try:
        s3 = boto3.client(S3)
        s3.download_file(INPUT_BUCKET, input_image_id, IMAGES_PATH + input_image_id)
        run_shell_command(input_image_id)
    except ClientError as error:
        logger.exception("Couldn't fetch image from input bucket: %s", INPUT_BUCKET)
        raise error


def run_shell_command(input_image_id):
    try:
        prediction = subprocess.check_output(
            ['python3', 'image_classification.py', IMAGES_PATH + input_image_id, '2>&1', '|', 'tee -a classify.log'])
        prediction_output = prediction.strip().decode('utf-8')
        logger.info("Prediction output= %s", prediction_output)

        prediction_output_serialized = serialize(prediction_output)
        upload_file(input_image_id, prediction_output_serialized)
    except ClientError as error:
        logger.exception("Couldn't process the input image in Deep Learning model")
        raise error


def serialize(text):
    try:
        serialized_object = pickle.dumps(text)
        return serialized_object
    except:
        logger.exception("Couldn't serialize the predicted value")
        return None


def upload_file(input_image_id, serialized_object):
    try:
        s3 = boto3.client(S3)
        s3.put_object(Bucket=OUTPUT_BUCKET, Key=input_image_id, Body=serialized_object)
    except:
        logger.exception("Couldn't upload the serialized object in the output bucket: %s", OUTPUT_BUCKET)


def initiate_app_tier():
    try:
        fetch_imageid_from_sqs()
    except ClientError as error:
        logger.exception("Couldn't initiate app-tier")
        raise error


def fetch_imageid_from_sqs():
    try:
        messages = receive_messages(REQUEST_QUEUE, MAX_NUMBER_OF_MSGS_TO_FETCH, WAIT_TIME_SECONDS, False)
        for msg in messages:
            fetch_image_from_s3(msg.body)
            delete_message(msg)
    except ClientError as error:
        logger.exception("Couldn't fetch imageId from queue")
        raise error


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
