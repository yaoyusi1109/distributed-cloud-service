from dataclasses import dataclass   # use python3's dataclass feature
from fileshare_helpers import *     # for csci356 filesharing helper code
from multithread_logging import *   # for csci356 logging helper code
from smartsocket import *           # for SmartSocket class
import http_helpers as http         # for csci356 http helper code
import mimetypes                    # for guessing mime type of files
import os                           # for listing files, opening files, etc.
import random                       # for random.choice() and random numbers
import socket                       # for socket stuff
import ssl                          # for tls sockets (used by https)
import sys                          # for exiting and command-line args
import threading                    # for threading.Thread()
import time                         # for time.time()
import urllib.parse                 # for quoting and unquoting url paths

def extractPathParams(req_path, route_prefix):
    # return_param: dict: key string -> val string
    # example route_prefix: "register", "file" etc.
    full_prefix_len = len(route_prefix) + 2
    retval = {}
    param_string = req_path[full_prefix_len:]
    parsed = param_string.split("&")
    for x in parsed:
        key, val = x.split("=")
        retval[key] = val
    return retval

def send_ok(conn, content):
    logwarn("Responding with content")
    content_len = len(content)
    resp = "HTTP/1.1 200 OK\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    resp += "Connection: close\r\n"
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: text/plain\r\n"
    log(resp)
    log(content)
    conn.sock.sendall(resp.encode() + b"\r\n" + content.encode())

# Send an HTTP 302 TEMPORARY REDIRECT to bounce client towards the main page,
# with a status message embedded into the url (so the status message will
# display on the page).
def send_redirect_to_main_page(conn, status):
    logwarn("Responding with redirect to main page")
    if status is None:
        url = "/shared-files.html"
        content = "You should go to the main page please!"
    else:
        url = "/shared-files.html?status=%s" % (urllib.parse.quote(status))
        content = "Status of your last request... %s\n" % (status)
        content += "Now go back to the main page please!"
    content_len = len(content)

    resp = "HTTP/1.1 302 TEMPORARY REDIRECT\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: text/plain\r\n"
    resp += "Location: %s\r\n" % (url)
    log(resp)
    conn.sock.sendall(resp.encode() + b"\r\n" + content.encode())

# Send an HTTP 302 TEMPORARY REDIRECT to bounce client towards the main page,
# with a status message embedded into the url (so the status message will
# display on the page).
def redirect_to_other_server(conn, content, ip, port, req_path):
    logwarn("Responding with redirect to main page")
    url = 'http://' + ip + ":" + port + req_path
    content_len = len(content)

    resp = "HTTP/1.1 302 TEMPORARY REDIRECT\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: text/plain\r\n"
    resp += "Location: %s\r\n" % (url)
    log(resp)
    conn.sock.sendall(resp.encode() + b"\r\n" + content.encode())

def send_filenames_and_sizes(conn):
    resp = "HTTP/1.1 200 OK\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"

    local_file_names = os.listdir("./share/")  # list of shared user files we have locally
    num_local_files = len(local_file_names)

    content = ""
    for filename in local_file_names:
        size = os.path.getsize("./share/" + filename)
        content += filename
        content += ","
        content += str(size)
        content += "&"

    resp += "Content-Length: %d\r\n" % (len(content))
    resp += "Content-Type: text/plain\r\n"
    conn.sock.sendall(resp.encode() + b"\r\n" + content.encode())
    