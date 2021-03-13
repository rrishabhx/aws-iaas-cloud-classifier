import logging

# SQS
REQUEST_QUEUE = "Request-Queue"
RESPONSE_QUEUE = "Response-Queue"
MAX_NUMBER_OF_MSGS_TO_FETCH = 2
WAIT_TIME_SECONDS = 3

# S3
INPUT_BUCKET = "cse546-input-bucket"
OUTPUT_BUCKET = "cse546-output-bucket"
S3 = 's3'

# Local
IMAGES_PATH = 'images/'


def init_logger(name):
    logger = logging.getLogger(name)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename='app_instance.log',
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    return logger
