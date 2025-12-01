import sys
import os
import requests
import argparse

MASTER_URL = "http://localhost:5000"

def upload_file(filepath):
    if not os.path.exists(filepath):
        print(f"File {filepath} not found.")
        return

    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)
    
    # 1. Request allocation from Master
    try:
        response = requests.post(f"{MASTER_URL}/upload", json={
            "filename": filename,
            "filesize": filesize
        })
        if response.status_code != 200:
            print(f"Error from Master: {response.text}")
            return
        
        data = response.json()
        chunk_id = data['chunk_id']
        nodes = data['nodes']
        
        print(f"Allocated chunk {chunk_id} on nodes: {nodes}")
        
        # 2. Upload data to Chunk Servers (Replication)
        # In a real system, we might pipeline this. Here we just push to all.
        with open(filepath, 'rb') as f:
            file_content = f.read()
            
        for node_url in nodes:
            print(f"Uploading to {node_url}...")
            try:
                files = {'file': (chunk_id, file_content)}
                res = requests.post(f"{node_url}/write_chunk", data={'chunk_id': chunk_id}, files=files)
                if res.status_code == 200:
                    print(f"Success: {node_url}")
                else:
                    print(f"Failed: {node_url} - {res.text}")
            except Exception as e:
                print(f"Failed to connect to {node_url}: {e}")

    except Exception as e:
        print(f"Error talking to Master: {e}")

def download_file(filename, output_path):
    # 1. Get location from Master
    try:
        response = requests.get(f"{MASTER_URL}/download", params={"filename": filename})
        if response.status_code != 200:
            print(f"Error from Master: {response.text}")
            return
        
        data = response.json()
        chunks = data['chunks']
        
        # 2. Read chunks
        # Assuming 1 chunk for now
        if not chunks:
            print("No chunks found.")
            return
            
        chunk_info = chunks[0]
        chunk_id = chunk_info['chunk_id']
        nodes = chunk_info['nodes']
        
        content = None
        for node_url in nodes:
            print(f"Trying to read from {node_url}...")
            try:
                res = requests.get(f"{node_url}/read_chunk", params={"chunk_id": chunk_id})
                if res.status_code == 200:
                    content = res.content
                    print(f"Successfully read from {node_url}")
                    break
            except Exception as e:
                print(f"Failed to read from {node_url}: {e}")
        
        if content is not None:
            with open(output_path, 'wb') as f:
                f.write(content)
            print(f"File downloaded to {output_path}")
        else:
            print("Failed to download file from any node.")

    except Exception as e:
        print(f"Error talking to Master: {e}")

def list_files():
    try:
        response = requests.get(f"{MASTER_URL}/ls")
        if response.status_code == 200:
            print("Files:", response.json())
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

def list_nodes():
    try:
        response = requests.get(f"{MASTER_URL}/nodes")
        if response.status_code == 200:
            print("Active Nodes:", response.json())
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="DFS Client")
    subparsers = parser.add_subparsers(dest='command')
    
    put_parser = subparsers.add_parser('put')
    put_parser.add_argument('file')
    
    get_parser = subparsers.add_parser('get')
    get_parser.add_argument('filename')
    get_parser.add_argument('output')
    
    subparsers.add_parser('ls')
    subparsers.add_parser('nodes')
    
    args = parser.parse_args()
    
    if args.command == 'put':
        upload_file(args.file)
    elif args.command == 'get':
        download_file(args.filename, args.output)
    elif args.command == 'ls':
        list_files()
    elif args.command == 'nodes':
        list_nodes()
    else:
        parser.print_help()
