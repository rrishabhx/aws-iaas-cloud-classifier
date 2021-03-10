import logging
import uuid
import pickle
import boto3

from botocore.exceptions import ClientError

from settings import *

logger = logging.getLogger(__name__)

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')


def put_object(bucket_name, object_key, data):
    """
    Upload data to a bucket and identify it with the specified object key.

    Usage is shown in usage_demo at the end of this module.

    :param bucket: The bucket to receive the data.
    :param object_key: The key of the object in the bucket.
    :param data: The data to upload. This can either be bytes or a string. When this
                 argument is a string, it is interpreted as a file name, which is
                 opened in read bytes mode.
    """
    put_data = data
    if isinstance(data, str):
        try:
            put_data = open(data, 'rb')
        except IOError:
            logger.exception("Expected file name or binary data, got '%s'.", data)
            raise

    try:
        bucket = s3_resource.Bucket(bucket_name)
        obj = bucket.Object(object_key)
        obj.put(Body=put_data)
        obj.wait_until_exists()
        logger.warning("Put object '%s' to bucket '%s'.", object_key, bucket.name)
    except ClientError:
        logger.exception("Couldn't put object '%s' to bucket '%s'.",
                         object_key, bucket_name)
        raise
    finally:
        if getattr(put_data, 'close', None):
            put_data.close()


def serialize(text):
    serialized_object = pickle.dumps(text)
    return serialized_object


def deserialize(obj):
    try:
        return pickle.loads(obj)
    except:
        return None


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file_to_s3(file, filename, bucket_name, acl="public-read"):
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
        logger.warning("Upload to S3 complete")
    except ClientError as e:
        logger.error("Couldn't upload image to S3: ", e)


def get_object(bucket_name, object_key):
    """
    Gets an object from a bucket.

    Usage is shown in usage_demo at the end of this module.

    :param bucket: The bucket that contains the object.
    :param object_key: The key of the object to retrieve.
    :return: The object data in bytes.
    """
    try:
        bucket = s3_resource.Bucket(bucket_name)
        body = bucket.Object(object_key).get()['Body'].read()
        logger.warning("Got object '%s' from bucket '%s'.", object_key, bucket.name)
    except ClientError:
        return None
    else:
        return body


def list_objects(bucket, prefix=None):
    """
    Lists the objects in a bucket, optionally filtered by a prefix.

    Usage is shown in usage_demo at the end of this module.

    :param bucket: The bucket to query.
    :param prefix: When specified, only objects that start with this prefix are listed.
    :return: The list of objects.
    """
    try:
        if not prefix:
            objects = list(bucket.objects.all())
        else:
            objects = list(bucket.objects.filter(Prefix=prefix))
        logger.warning("Got objects %s from bucket '%s'",
                       [o.key for o in objects], bucket.name)
    except ClientError:
        logger.exception("Couldn't get objects for bucket '%s'.", bucket.name)
        raise
    else:
        return objects


def get_uniq_filename(file_name):
    uniq_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])

    return uniq_file_name


def get_prediction_from_metadata(image_obj):
    metadata = s3_client.head_object(Bucket=OUTPUT_BUCKET, Key=image_obj.name)
    print(metadata["Metadata"])


if __name__ == '__main__':
    # output = "german shepherd"
    # serialized_txt = serialize_text(output)
    # print(serialized_txt)
    # put_object(OUTPUT_BUCKET, "1_dog.png", serialized_txt)

    # print(pickle.loads(serialized_txt))
    obj = get_object(OUTPUT_BUCKET, "1_dog.png")
    print(obj)
    print(pickle.loads(obj))
    # print(config.INPUT_BUCKET)
    # upload_file('uploads/0_cat.png', constants.INPUT_BUCKET)
# s3_buck = s3.Bucket('cse546-input-bucket')
# put_object(s3_buck, 'requirements.txt', 'requirements.txt')
# print(get_object(s3_buck, 'requirements.txt'))
