from concurrent.futures.thread import ThreadPoolExecutor

# Shared state
currently_running_instances = set()

# ThreadPool
executor = ThreadPoolExecutor(5)

# Web-Tier
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.JPEG', '.png', '.gif']

# SQS
REQUEST_QUEUE = "Request-Queue"
RESPONSE_QUEUE = "Response-Queue"
MAX_NUMBER_OF_MSGS_TO_FETCH = 2
WAIT_TIME_SECONDS = 3

# EC2
AMI_ID_BASE = "ami-0ee8cf7b8a34448a6"
AMI_ID_APP = "ami-0d1d31fabedbddf87"
AMI_ID_APP_2 = "ami-013c7826f9e452597"
EC2_KEY_PAIR = "rizz_key_pair"
SECURITY_GROUP_NAME = ["AllowSSH"]
INSTANCE_TYPE = "t2.micro"
APP_SERVER_NAME = "App-Server"
MIN_POSSIBLE_INSTANCES = 1
MAX_POSSIBLE_INSTANCES = 17
MAX_REQUESTS_PER_INSTANCE = 5

# S3
INPUT_BUCKET = "cse546-input-bucket"
OUTPUT_BUCKET = "cse546-output-bucket"
