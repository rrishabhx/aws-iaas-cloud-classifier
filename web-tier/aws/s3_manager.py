import pickle
import uuid

import boto3
from botocore.exceptions import ClientError

import settings as s

logger = s.init_logger(__name__)

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')


def serialize(text):
    """
    Serialize the text data to stream object.
    :param text: Textual data
    :return: Serialized object
    """
    serialized_object = pickle.dumps(text)
    return serialized_object


def deserialize(obj):
    """
    De-serializes the object to get the data
    :param obj:
    :return:
    """
    try:
        return pickle.loads(obj)
    except:
        return None


def upload_file_to_s3(file, filename, bucket_name, acl="public-read"):
    """Upload a file to an S3 bucket

    :param acl: Access control list
    :param file File to upload
    :param filename: Name of File
    :param bucket_name: Bucket name to upload to
    """
    try:
        s3_client.upload_fileobj(
            file,
            bucket_name,
            filename,
            ExtraArgs={
                "ACL": acl,
                "ContentType": file.content_type
            }
        )
        logger.info("Upload to S3 complete")
    except ClientError as e:
        logger.error("Couldn't upload image to S3: ", e)


def get_object(bucket_name, object_key):
    """
    Gets an object from a bucket.

    :param bucket_name: The bucket name that contains the object.
    :param object_key: The key of the object to retrieve.
    :return: The object data in bytes.
    """
    try:
        bucket = s3_resource.Bucket(bucket_name)
        body = bucket.Object(object_key).get()['Body'].read()
        logger.info("Got object '%s' from bucket '%s'.", object_key, bucket.name)
    except ClientError:
        return None
    else:
        return body


def list_objects(bucket_name, prefix=None):
    """
    Lists the objects in a bucket, optionally filtered by a prefix.

    :param bucket_name: The bucket to query.
    :param prefix: When specified, only objects that start with this prefix are listed.
    :return: The list of objects.
    """
    try:
        bucket = s3_resource.Bucket(bucket_name)
        if not prefix:
            objects = list(bucket.objects.all())
        else:
            objects = list(bucket.objects.filter(Prefix=prefix))
        logger.warning("Got objects %s from bucket '%s'",
                       [o.key for o in objects], bucket.name)
    except ClientError:
        logger.exception("Couldn't get objects for bucket '%s'.", bucket_name)
        raise
    else:
        return objects


def get_uniq_filename(file_name):
    """
    Adds a unique prefix to the file name
    :param file_name: Name of file
    :return: Unique name
    """
    uniq_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])

    return uniq_file_name
