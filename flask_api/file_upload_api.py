from werkzeug.utils import secure_filename
from flask import Flask, request, jsonify
from config.error_logs import logger  # Assuming you have a logger module
from pyngrok import ngrok, conf
import configparser
import os

config = configparser.ConfigParser()

try:
    config.read('config/config.ini')
    ngrok_config_path = config.get('files', 'ngrok_config')
    upload_folder = config.get('folders', 'upload_folder')
    allowed_extensions = {'.txt'}
except Exception as e:
    logger.error(f"Error reading configuration: {e}")
    raise

if not os.path.exists(upload_folder):
    logger.error(f"Upload folder '{upload_folder}' not found or insufficient permissions.")
    raise FileNotFoundError(f"Upload folder '{upload_folder}' not found or insufficient permissions.")

try:
    conf.get_default().config_path = ngrok_config_path
    https_tunnel = ngrok.connect(bind_tls=True)
except Exception as e:
    logger.error(f"Error establishing ngrok connection: {e}")
    raise

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = upload_folder

@app.route('/')
def nothing():
    """
    This route returns a message indicating that everything works and provides information about the API endpoint.
    """
    return '''Nothing to see here. Everything works on api "/api/upload"'''

@app.route('/api/upload', methods=['POST'])
def api_upload_file():
    """
    This route handles file uploads.
    """
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400  # Return error if no file part in the request

        file = request.files['file']

        if file.filename == '':
            return jsonify({'error': 'No selected file'}), 400  # Return error if no file selected

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)  # Secure filename to prevent directory traversal attacks
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)  # Save the file to the upload folder
            return jsonify({'success': f'File {filename} uploaded successfully'})  # Return success message
        else:
            return jsonify({'error': 'Invalid file format'}), 400  # Return error if file format not allowed

    except FileNotFoundError:
        logger.error(f"Upload folder '{app.config['UPLOAD_FOLDER']}' not found or insufficient permissions.")
        return jsonify({'error': 'Upload folder not found or insufficient permissions.'}), 500
    except Exception as e:
        logger.error(f"Error in file upload API: {e}")  # Log any unexpected errors
        return jsonify({'error': 'An unexpected error occurred'}), 500  # Return error for unexpected errors

def allowed_file(filename):
    """
    Check if the file has an allowed extension.
    """
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

if __name__ == "__main__":
    ngrok_tunnel = ngrok.connect(5000)
    print(f'Public URL: {ngrok_tunnel.public_url}')
    try:
        app.run()  # Run the Flask application
    finally:
        ngrok.disconnect(ngrok_tunnel.public_url)
        ngrok.kill()
