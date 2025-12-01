import logging
import threading
import time
import uuid
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration
REPLICATION_FACTOR = 3
HEARTBEAT_TIMEOUT = 10  # Seconds before a node is considered dead

# In-memory Metadata Store
# Files: {filename: {size: int, chunks: [chunk_id_1, chunk_id_2, ...]}}
file_metadata = {}

# Chunk Locations: {chunk_id: [node_url_1, node_url_2, ...]}
chunk_locations = {}

# Active Chunk Servers: {node_url: last_heartbeat_timestamp}
chunk_servers = {}

logging.basicConfig(level=logging.INFO)

def remove_inactive_servers():
    """Background task to remove servers that haven't sent a heartbeat."""
    while True:
        time.sleep(5)
        now = time.time()
        inactive_nodes = []
        for node, last_beat in list(chunk_servers.items()):
            if now - last_beat > HEARTBEAT_TIMEOUT:
                logging.warning(f"Node {node} is dead (no heartbeat).")
                inactive_nodes.append(node)
        
        for node in inactive_nodes:
            del chunk_servers[node]
            # In a real system, we would trigger re-replication here.

threading.Thread(target=remove_inactive_servers, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    """Receive heartbeat from chunk server."""
    data = request.json
    node_url = data.get('url')
    if node_url:
        chunk_servers[node_url] = time.time()
        return jsonify({"status": "ok"})
    return jsonify({"error": "Missing url"}), 400

@app.route('/upload', methods=['POST'])
def upload_request():
    """Client requests to upload a file. Returns list of chunks and where to write them."""
    data = request.json
    filename = data.get('filename')
    filesize = data.get('filesize')
    
    if not filename or not filesize:
        return jsonify({"error": "Missing filename or filesize"}), 400

    # Simple logic: 1 file = 1 chunk for this PoC (or split if we want to be fancy)
    # Let's do 1 chunk for simplicity first, or maybe split by 64MB.
    # For this demo, let's assume the client handles splitting logic or we just do 1 chunk per file.
    # Let's stick to 1 chunk = 1 file for the MVP to prove the point, 
    # but the data structure supports multiple.
    
    chunk_id = str(uuid.uuid4())
    
    # Select available chunk servers
    active_nodes = list(chunk_servers.keys())
    if len(active_nodes) < 1:
        return jsonify({"error": "No chunk servers available"}), 503
    
    # Select up to REPLICATION_FACTOR nodes
    selected_nodes = active_nodes[:REPLICATION_FACTOR]
    
    file_metadata[filename] = {
        "size": filesize,
        "chunks": [chunk_id]
    }
    chunk_locations[chunk_id] = selected_nodes
    
    return jsonify({
        "chunk_id": chunk_id,
        "nodes": selected_nodes
    })

@app.route('/download', methods=['GET'])
def download_request():
    """Client requests to download a file. Returns chunk locations."""
    filename = request.args.get('filename')
    if not filename or filename not in file_metadata:
        return jsonify({"error": "File not found"}), 404
    
    file_info = file_metadata[filename]
    chunks_info = []
    
    for chunk_id in file_info['chunks']:
        nodes = chunk_locations.get(chunk_id, [])
        chunks_info.append({
            "chunk_id": chunk_id,
            "nodes": nodes
        })
        
    return jsonify({
        "filename": filename,
        "chunks": chunks_info
    })

@app.route('/ls', methods=['GET'])
def list_files():
    return jsonify(list(file_metadata.keys()))

@app.route('/nodes', methods=['GET'])
def list_nodes():
    return jsonify(list(chunk_servers.keys()))

if __name__ == '__main__':
    # Run Master on port 5000
    app.run(host='0.0.0.0', port=5000, debug=False)
