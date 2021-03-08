import logging
import os
from config import *
from aws import test_s3 as s3
from aws import msg_queue as mq
from aws import autoscaler
from flask import Flask, render_template, request, redirect, url_for, abort
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN)

app = Flask(__name__)
app.config['UPLOAD_EXTENSIONS'] = IMAGE_EXTENSIONS
app.config['UPLOAD_PATH'] = 'uploads'


def handle_image_upload(image, image_name):
    # logger.warning("Trying to upload image: %s to S3 input bucket: %s", image, INPUT_BUCKET)
    s3.upload_file_to_s3(image, INPUT_BUCKET)

    # logger.warning("Sending image name to SQS: %s", image_name)
    mq.send_message(REQUEST_QUEUE, image_name)


def fetch_from_response_queue(image_set):
    image_predictions = {}
    while image_set:
        # To Do: Update request_q with response_q below
        recvd_msgs = mq.receive_messages(REQUEST_QUEUE, MAX_NUMBER_OF_MSGS_TO_FETCH, WAIT_TIME_SECONDS, False)

        for msg in recvd_msgs:
            if msg.body in image_set:
                print(f"Found {msg.body} in image_set")
                image_predictions[msg.body] = msg.body
                image_set.remove(msg.body)

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

        logger.warning("Starting auto-scaler")
        autoscaler.scale_out_app_tier()

        logger.warning("Waiting for response from App-Tier")
        image_predictions = fetch_from_response_queue(image_set)

        logger.warning("Scaling-In app tier")
        autoscaler.scale_in_app_tier()

        return render_template('index.html', preds=image_predictions)

    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)
