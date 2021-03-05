import logging
import boto3
import time
import statistics as st
from botocore.exceptions import ClientError

import constants
import ec2_manager as ec2
import msg_queue as mq

MIN_POSSIBLE_INSTANCES = 1
MAX_POSSIBLE_INSTANCES = 5
MAX_REQUESTS_PER_INSTANCE = 50


def get_total_requests_in_sqs():
    try:
        size_values = []
        for _ in range(5):
            size_values.append(int(mq.get_queue_size(constants.REQUEST_QUEUE)))
            time.sleep(1)

        print(size_values)
        approx_size = st.mode(size_values)
    except ClientError:
        raise
    else:
        return approx_size



    # print("Avg size:", st.mean(size_values))
    # print("Median size:", st.median(size_values))
    # print("Mode size:", st.mode(size_values))


if __name__ == '__main__':
    while True:
        autoscale_app_servers()
