import logging
import boto3
from botocore.exceptions import ClientError

import constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


ec2_resource = boto3.resource('ec2')
ec2_client = boto3.client('ec2')

def create_instance(
        image_id, instance_type, key_name, security_group_names=None):
    try:
        instance_params = {
            'ImageId': image_id, 'InstanceType': instance_type, 'KeyName': key_name
        }
        if security_group_names is not None:
            instance_params['SecurityGroups'] = security_group_names
        instance = ec2_resource.create_instances(**instance_params, MinCount=1, MaxCount=1)[0]
        logger.info("Created instance %s.", instance.id)
    except ClientError:
        logging.exception(
            "Couldn't create instance with image %s, instance type %s, and key %s.",
            image_id, instance_type, key_name)
        raise
    else:
        return instance


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


if __name__ == '__main__':
    # instance = create_instance(constants.AMI_ID, "t2.micro")
    response = ec2_client.describe_key_pairs()
    print(response)

    # instance_id = "i-09c5eae4563fde21c"
    # terminate_instance(instance_id)