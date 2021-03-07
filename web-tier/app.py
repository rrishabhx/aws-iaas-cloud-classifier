import os
import time
from aws import constants as const
from aws import test_s3 as s3
from flask import Flask, render_template, request, redirect, url_for, abort
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_EXTENSIONS'] = ['.jpg', '.jpeg', '.png', '.gif']
app.config['UPLOAD_PATH'] = 'uploads'


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_images = request.files.getlist('image_file')

        for image in uploaded_images:
            image_name = secure_filename(image.filename)
            if image_name != '':
                image_ext = os.path.splitext(image_name)[1]
                if image_ext not in app.config['UPLOAD_EXTENSIONS']:
                    abort(400)

                print(f"Trying to upload image: {image_name} to S3 bucket: ", const.INPUT_BUCKET)
                s3.upload_file_to_s3(image, const.INPUT_BUCKET)

        return redirect(url_for('index'))

    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)
