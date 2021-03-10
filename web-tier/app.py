import logging
import os
import time
from settings import *
from aws import test_s3 as s3
from aws import msg_queue as mq
from aws import autoscaler
from flask import Flask, render_template, request, abort
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN)

app = Flask(__name__)
app.config['UPLOAD_EXTENSIONS'] = IMAGE_EXTENSIONS
app.config['UPLOAD_PATH'] = 'uploads'


def handle_image_upload(image, image_name):
    # logger.warning("Trying to upload image: %s to S3 input bucket: %s", image, INPUT_BUCKET)
    s3.upload_file_to_s3(image, image_name, INPUT_BUCKET)

    # logger.warning("Sending image name to SQS: %s", image_name)
    mq.send_message(REQUEST_QUEUE, image_name)


def fetch_response_from_output_bucket(image_set):
    image_predictions = {}
    while image_set:
        print("Current image set:", image_set)
        images_to_remove = list()

        for image_name in image_set:
            image_obj = s3.get_object(OUTPUT_BUCKET, image_name)
            if image_obj is not None:
                image_prediction = s3.deserialize(image_obj)
                if image_prediction is None:
                    image_prediction = s3.get_prediction_from_metadata(image_obj)

                image_predictions[image_name[6:]] = image_prediction
                images_to_remove.append(image_name)

        for image_name in images_to_remove:
            image_set.remove(image_name)

    return image_predictions


def fetch_from_response_queue(image_set):
    image_predictions = {}
    while image_set:
        # To Do: Update request_q with response_q below
        recvd_msgs = mq.receive_messages(RESPONSE_QUEUE, MAX_NUMBER_OF_MSGS_TO_FETCH, WAIT_TIME_SECONDS, False)

        for msg in recvd_msgs:
            if msg.body in image_set:
                print(f"Found {msg.body} in image_set")
                msg_content = msg.body[6:]
                image_predictions[msg_content] = msg_content
                image_set.remove(msg.body)
                msg.delete()

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

        logger.warning("Starting auto-scaler")
        autoscaler.scale_out_app_tier()

        logger.warning("Waiting for response from App-Tier")
        image_predictions = fetch_response_from_output_bucket(image_set)

        logger.warning("Scaling-In app tier")
        autoscaler.scale_in_app_tier()

        return render_template('index.html', preds=image_predictions)

    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)
