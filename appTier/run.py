import logging
import boto3
from botocore.exceptions import ClientError

import subprocess
import boto3
import json

def fetch_image_from_s3():
    s3 = boto3.client('s3')
    s3.download_file('cc576projectbucket','dogggs.jpg','dogOp2.jpg')


def runShellCommand():
    image_name = 'dogOp2.jpg'
    deepL_op=subprocess.call('python3 /home/ubuntu/classifier/image_classification.py '+image_name ,shell=True)
    print(deepL_op)
    thisdict = {}
    thisdict[image_name] = deepL_op
    print(thisdict)
    f = open("final2.txt", "w")
    f.write(json.dumps(thisdict))
    f.close()

def upload_file():
    path=''
    file_name='final2.txt'
    object_name=file_name
    bucket='cc576projectbucket'
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(path+file_name, bucket, object_name)
    print(response)

fetch_image_from_s3()
runShellCommand()
upload_file()
