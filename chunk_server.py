import os
import sys
import time
import requests
import threading
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration
MASTER_URL = "http://localhost:5000"
HOST = "localhost"
PORT = 5001  # Default, can be overridden
STORAGE_DIR = "./data_5001"

logging.basicConfig(level=logging.INFO)

def send_heartbeat():
    """Periodically send heartbeat to Master."""
    while True:
        try:
            my_url = f"http://{HOST}:{PORT}"
            requests.post(f"{MASTER_URL}/heartbeat", json={"url": my_url})
            logging.debug(f"Sent heartbeat from {my_url}")
        except Exception as e:
            logging.error(f"Failed to send heartbeat: {e}")
        time.sleep(5)

@app.route('/write_chunk', methods=['POST'])
def write_chunk():
    """Write chunk data to disk."""
    chunk_id = request.form.get('chunk_id')
    file_data = request.files.get('file')
    
    if not chunk_id or not file_data:
        return jsonify({"error": "Missing chunk_id or file"}), 400
    
    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    try:
        file_data.save(chunk_path)
        logging.info(f"Saved chunk {chunk_id} to {chunk_path}")
        return jsonify({"status": "ok"})
    except Exception as e:
        logging.error(f"Failed to write chunk: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/read_chunk', methods=['GET'])
def read_chunk():
    """Read chunk data from disk."""
    chunk_id = request.args.get('chunk_id')
    if not chunk_id:
        return jsonify({"error": "Missing chunk_id"}), 400
    
    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    if not os.path.exists(chunk_path):
        return jsonify({"error": "Chunk not found"}), 404
        
    try:
        with open(chunk_path, 'rb') as f:
            data = f.read()
        return data  # Flask automatically creates a response with correct content-type
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    if len(sys.argv) > 1:
        PORT = int(sys.argv[1])
    
    STORAGE_DIR = f"./data_{PORT}"
    if not os.path.exists(STORAGE_DIR):
        os.makedirs(STORAGE_DIR)
        
    # Start heartbeat thread
    threading.Thread(target=send_heartbeat, daemon=True).start()
    
    logging.info(f"Starting Chunk Server on port {PORT}, storage: {STORAGE_DIR}")
    app.run(host='0.0.0.0', port=PORT, debug=False)
