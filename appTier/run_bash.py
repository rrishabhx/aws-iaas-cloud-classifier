import logging
import boto3
from botocore.exceptions import ClientError

import subprocess
import boto3
import json

BUCKET_NAME='cc576projectbucket'
S3='s3'

def fetch_image_from_s3(input_image_id):
    s3 = boto3.client(S3)
    s3.download_file(BUCKET_NAME,input_image_id,input_image_id)
    runShellCommand(input_image_id)


def runShellCommand(input_image_id):
    deepL_op=subprocess.call('python3 /home/ubuntu/classifier/image_classification.py '+input_image_id ,shell=True)
    print(deepL_op)
    thisdict = {}
    thisdict[input_image_id] = deepL_op
    print(thisdict)
    f = open(input_image_id+".txt", "w")
    f.write(json.dumps(thisdict))
    f.close()
    upload_file(input_image_id)


def upload_file(file_name):
    path=''
    object_name=file_name
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(path+file_name, BUCKET_NAME, object_name)
    print(response + input_image_id)

def initiate_app_tier(isRunning):
    input_image_id=sqs()
    if input_image_id!=null :
        fetch_image_from_s3(input_image_id)
        

while True :
    initiate_app_tier(True)