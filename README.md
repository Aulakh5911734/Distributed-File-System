# Distributed-File-System

This project is a Python-based Distributed File System (DFS) where multiple clients can upload, download, and manage files through a server. I built this project to understand how distributed systems work, how data is transferred between nodes, and how file storage is handled in a networked environment.

About the Project

The goal of this project is to allow multiple users to interact with a central server and perform basic file operations, including:

Uploading files

Downloading files

Viewing the list of stored files

Deleting or updating files

The communication between clients and the server is implemented using Python socket programming.

How It Works

The server runs in the background and waits for incoming client connections.

When a client connects, it sends a specific request such as upload, download, list, or delete.

The server processes the request, performs the required file operation, and sends back a response.

The client receives the response and displays the result to the user.

Working on this project helped me understand how file transfers work at the socket level and how distributed systems manage data consistency.

Technologies Used
Programming Language

Python

Networking

Python's built-in socket library for client-server communication

File Handling

Python file I/O for reading, writing, and managing files on the server side

Architecture

Client-Server Model using TCP sockets

Project Structure
/DistributedFileSystem
│── server.py
│── client.py
│── utils.py (optional)
│── storage/          # Directory where uploaded files are stored
│── README.md

How to Run the Project
Step 1: Start the Server
python server.py

Step 2: Start the Client
python client.py


Follow the menu displayed in the client program to upload, download, or view files.

Features

Multiple clients can connect to the server

Upload and download functionality

Server-side file listing

Reliable TCP-based communication

Simple and easy-to-understand Python code structure

Future Improvements

I am planning to add the following features:

File replication across multiple nodes

Secure file transfer using encryption

Authentication for client access

A user-friendly graphical interface
