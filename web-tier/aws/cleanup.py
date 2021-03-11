from botocore.exceptions import ClientError

from aws import msg_queue as mq
from aws import s3_manager as s3
from aws import ec2_manager as ec2
from aws import autoscaler
import settings as s


def clean_sqs(*args):
    print("Cleaning SQS")
    for q in args:
        while mq.receive_messages(q, 10, 2):
            continue


def clean_s3(*args):
    print("Cleaning S3")
    for bucket in args:
        objs = s3.list_objects(bucket)
        for obj in objs:
            obj.delete()


def shutdown_app_servers(app_servers_list):
    print("Shutting down app-servers")
    for app_id in app_servers_list:
        try:
            ec2.terminate_instance(app_id)
        except ClientError:
            print("Error occurred while terminating instance: %s. It could already be down.", app_id)


if __name__ == '__main__':
    clean_sqs(s.REQUEST_QUEUE, s.RESPONSE_QUEUE)
    clean_s3(s.INPUT_BUCKET, s.OUTPUT_BUCKET)
    shutdown_app_servers(ec2.get_running_instances_by_name(s.APP_SERVER_NAME))
