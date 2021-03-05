from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_images = request.files.getlist('image_file')

        for image in uploaded_images:
            if image.filename != '':
                image.save(image.filename)

        return redirect(url_for('index'))

    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=True)
