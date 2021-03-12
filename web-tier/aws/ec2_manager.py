import logging
import time

import boto3
from botocore.exceptions import ClientError

import settings as s

logger = s.init_logger(__name__)

ec2_resource = boto3.resource('ec2')
ec2_client = boto3.client('ec2')


def create_app_instances(image_id, instance_type, key_name, instance_name, security_group_names=None, max_count=1):
    """
    Spawns a new app-tier EC2 instance with user_data set to the app-instance startup script.
    :param image_id: Image name with unique prefix
    :param instance_type: Type of instance
    :param key_name: Key pair name of AWS account
    :param instance_name: Instance name
    :param security_group_names: List of security groups
    :param max_count: Max count of instances to create
    :return: Instance id of the first instance created
    """
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
        instances[0].create_tags(Tags=[{'Key': 'Name', 'Value': f'{instance_name}'}])

    except ClientError:
        logging.exception(
            "Couldn't create instance with image %s, instance type %s, and key %s.",
            image_id, instance_type, key_name)
        raise
    finally:
        return instance_id


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


def get_running_instances_by_name(name):
    """
    Finds all currently running or pending state app-tier instances
    :param name: Name string to filter the instances
    :return: List of instances
    """
    try:
        instances = ec2_resource.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'pending']},
                     {'Name': 'tag:Name', 'Values': [name + '*']}])

        list_instances = [instance.id for instance in instances]
    except ClientError:
        logger.exception("Couldn't find total running instances")
        raise
    else:
        return list_instances
