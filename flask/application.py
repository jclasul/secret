from flask import Flask, render_template, request, url_for
import os
IMAGE_FOLDER = os.path.join('static')

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = IMAGE_FOLDER
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

@app.route('/image')
def show_index():
    l = os.listdir(IMAGE_FOLDER)[-1]
    print(l)
    PATH = os.path.join(IMAGE_FOLDER, l)
        
    return render_template("index.html",  user_image = PATH)

@app.route('/hello')
def get_balance():
    return 'Hello, World!'

# run the application
if __name__ == "__main__":  
    app.run(debug=False)