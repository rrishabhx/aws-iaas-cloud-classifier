import logging
import boto3
from botocore.exceptions import ClientError

import subprocess
import boto3
import json
import os
import pickle

BUCKET_NAME='cc576projectbucket'
S3='s3'
IMAGES_PATH='/home/ubuntu/classifier/images/'
OUTPUT_FILE_PATH='/home/ubuntu/classifier/output/'


def fetch_image_from_s3(input_image_id):
    s3 = boto3.client(S3)
    s3.download_file(BUCKET_NAME,input_image_id,IMAGES_PATH+input_image_id)
    runShellCommand(input_image_id)


def runShellCommand(input_image_id):
    prediction=subprocess.check_output(['/usr/bin/python3', '/home/ubuntu/classifier/image_classification.py', IMAGES_PATH+input_image_id])
    prediction_output=prediction.strip().decode('utf-8')
    print(prediction_output)
    upload_file(input_image_id,serialize(prediction_output))


def serialize(text):
    serialized_object = pickle.dumps(text)
    print(serialized_object)
    return serialized_object


def upload_file(input_image_id,serialized_object):
    s3 = boto3.client(S3)
    s3.put_object(Bucket=BUCKET_NAME,Key=input_image_id,Body=serialized_object)
    print("hi")
    print(str(serialized_object))


def initiate_app_tier(isRunning):
    input_image_id=sqs()
    if input_image_id != None :
        fetch_image_from_s3(input_image_id)

#for testing     
def sqs():
    return 'newdog.jpg'

while True :
    initiate_app_tier(True)