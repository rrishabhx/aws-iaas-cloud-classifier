import logging
import time

import boto3
import settings as s
from botocore.exceptions import ClientError

logger = s.init_logger(__name__)

ec2_resource = boto3.resource('ec2')
ec2_client = boto3.client('ec2')


def create_instances(image_id, instance_type, key_name, security_group_names=None, max_count=1):
    instance_id = None
    try:
        user_data_init_app = '''#!/bin/bash
        python3 /root/app-tier/run_poller.py &> /root/app-tier/poller.log'''

        # user_data_working = '''#!/bin/bash
        # echo 'to hell with this shit' > /root/hello.txt'''

        instance_params = {
            'ImageId': image_id, 'InstanceType': instance_type, 'KeyName': key_name,
            'UserData': user_data_init_app
        }
        if security_group_names is not None:
            instance_params['SecurityGroups'] = security_group_names

        instances = ec2_resource.create_instances(**instance_params, MinCount=1, MaxCount=max_count)
        instance_id = instances[0].id
        # instances[0].wait_until_running()
        time.sleep(2)
        instances[0].create_tags(Tags=[{'Key': 'Name', 'Value': f'App-Server'}])

    except ClientError:
        logging.exception(
            "Couldn't create instance with image %s, instance type %s, and key %s.",
            image_id, instance_type, key_name)
        raise
    finally:
        return instance_id


def start_instance(instance_id):
    """
    Starts an instance. The request returns immediately. To wait for the instance
    to start, use the Instance.wait_until_running() function.

    :param instance_id: The ID of the instance to start.
    :return: The response to the start request. This includes both the previous and
             current state of the instance.
    """
    try:
        response = ec2_resource.Instance(instance_id).start()
        logger.info("Started instance %s.", instance_id)
    except ClientError:
        logger.exception("Couldn't start instance %s.", instance_id)
        raise
    else:
        return response


def terminate_instance(instance_id):
    """
    Terminates an instance. The request returns immediately. To wait for the
    instance to terminate, use Instance.wait_until_terminated().

    :param instance_id: The ID of the instance to terminate.
    """
    try:
        ec2_resource.Instance(instance_id).terminate()
        logger.info("Terminating instance %s.", instance_id)
    except ClientError:
        logging.exception("Couldn't terminate instance %s.", instance_id)
        raise


def stop_instance(instance_id):
    """
    Stops an instance. The request returns immediately. To wait for the instance
    to stop, use the Instance.wait_until_stopped() function.

    :param instance_id: The ID of the instance to stop.
    :return: The response to the stop request. This includes both the previous and
             current state of the instance.
    """
    try:
        response = ec2_resource.Instance(instance_id).stop()
        logger.info("Stopped instance %s.", instance_id)
    except ClientError:
        logger.exception("Couldn't stop instance %s.", instance_id)
        raise
    else:
        return response


def total_running_instances():
    try:
        instances = ec2_resource.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'pending']}])

        list_instances = [instance for instance in instances]
    except ClientError:
        logger.exception("Couldn't find total running instances")
        raise
    else:
        return len(list_instances)


def get_running_instances_by_name(name):
    try:
        instances = ec2_resource.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'pending']},
                     {'Name': 'tag:Name', 'Values': [name]}])

        list_instances = [instance for instance in instances]
    except ClientError:
        logger.exception("Couldn't find total running instances")
        raise
    else:
        return list_instances


def print_all_running_instances():
    instances = ec2_resource.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for instance in instances:
        print(instance.id, instance.instance_type)


if __name__ == '__main__':
    print(total_running_instances())

    instance = create_instances(s.AMI_ID_APP_2, "t2.micro", s.EC2_KEY_PAIR, s.SECURITY_GROUP_NAME)

    # for app in get_running_instances_by_name('App-Server'):
    #     terminate_instance(app.id)

# instance.create_tags(Tags=[{'Key':'Name',
#                             'Value': 'FarziInstance'}])
# print(instance)
# response = ec2_client.describe_key_pairs()
# print(response)

# instance_id = "i-09c5eae4563fde21c"
# terminate_instance(instance_id)
