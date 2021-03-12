import os
import time

from flask import Flask, render_template, request, abort
from werkzeug.utils import secure_filename

import settings as s
from aws import autoscaler
from aws import msg_queue as mq
from aws import s3_manager as s3

logger = s.init_logger(__name__)

app = Flask(__name__)
app.config['UPLOAD_EXTENSIONS'] = s.IMAGE_EXTENSIONS
app.config['UPLOAD_PATH'] = 'uploads'


def handle_image_upload(image, image_name):
    """
    Uploads image to input S3 bucket and Sends image-id to the Request SQS
    :param image: Image data
    :param image_name: Image name with unique prefix (image-id)
    """
    logger.info("Trying to upload image: %s to S3 input bucket: %s", image, s.INPUT_BUCKET)
    s3.upload_file_to_s3(image, image_name, s.INPUT_BUCKET)

    logger.info("Sending image name to SQS: %s", image_name)
    mq.send_message(s.REQUEST_QUEUE, image_name)


def fetch_response_from_output_bucket(image_set):
    """
    Starts a Poller which requests the output bucket for every image-id in the request.
    :param image_set: A set() of IDs of images that came in HTTP POST request
    :return: A dictionary of image-ids and classifier prediction
    """
    image_predictions = {}
    while image_set:
        logger.info(f"Current image set size: {len(image_set)}")
        images_to_remove = list()

        for image_name in image_set:
            image_obj = s3.get_object(s.OUTPUT_BUCKET, image_name)
            if image_obj is not None:
                image_prediction = s3.deserialize(image_obj)

                image_predictions[image_name[6:]] = image_prediction
                images_to_remove.append(image_name)

        for image_name in images_to_remove:
            image_set.remove(image_name)

    return image_predictions


def fetch_from_response_queue(image_set):
    """
    Starts a Poller which requests the output response SQS for every image-id in the request.
    :param image_set: A set() of IDs of images that came in HTTP POST request
    :return: A dictionary of image-ids and classifier prediction
    """
    image_predictions = {}
    while image_set:
        # To Do: Update request_q with response_q below
        recvd_msgs = mq.receive_messages(s.RESPONSE_QUEUE, s.MAX_NUMBER_OF_MSGS_TO_FETCH, s.WAIT_TIME_SECONDS, False)

        for msg in recvd_msgs:
            if msg.body in image_set:
                logger.info(f"Found {msg.body} in image_set")
                msg_content = msg.body[6:]
                image_predictions[msg_content] = msg_content
                image_set.remove(msg.body)
                msg.delete()
            time.sleep(0.5)
    return image_predictions


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        image_set = set()
        uploaded_images = request.files.getlist('image_file')

        for image in uploaded_images:
            image_name = secure_filename(image.filename)
            if image_name != '':
                image_ext = os.path.splitext(image_name)[1]
                if image_ext not in app.config['UPLOAD_EXTENSIONS']:
                    abort(400)

                image_name = s3.get_uniq_filename(image_name)
                handle_image_upload(image, image_name)
                image_set.add(image_name)

        time.sleep(3)

        logger.info("Starting auto-scaler")
        created_app_servers = autoscaler.scale_out_app_tier()

        logger.info("Waiting for response from App-Tier")
        image_predictions = fetch_response_from_output_bucket(image_set)

        logger.info("Scaling-In app tier")
        autoscaler.scale_in_app_tier(created_app_servers)

        return render_template('index.html', preds=image_predictions)

    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)
