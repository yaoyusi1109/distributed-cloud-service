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

# Send an HTTP 307 TEMPORARY REDIRECT to bounce client towards replica
def redirect_to_other_server(conn, content, ip, port, req_path):
    port = str(port)
    logwarn("Responding with redirect to main page")
    url = 'http://' + ip + ":" + port + req_path
    content_len = len(content)

    resp = "HTTP/1.1 307 Temporary Redirect\r\n"
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

# Given a filename of a shared file that is stored locally, get the data from
# the file.
def get_share_file_locally(filename):
    try:
        log("Opening locally-stored shared file '%s'..." % (filename))
        with open("./share/" + filename, "rb") as f:
            data = f.read()
            return data
    except OSError as err:
        logerr("problem opening shared file '%s' locally: %s" % (filename, err))
        return None

# Send a generic HTTP 404 NOT FOUND response to the client.
def send_404_not_found(conn):
    logwarn("Responding with 404 not found")
    content = "Sorry, the page you requested could not be found :)"
    content_len = len(content)

    resp = "HTTP/1.1 404 NOT FOUND\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: text/plain\r\n"
    log(resp)
    conn.sock.sendall(resp.encode() + b"\r\n" + content.encode())

# Send a shared file to the browser. This will first locate the file by checking
# if it is stored locally. If not found, we send a 404 NOT FOUND response. If
# the file is found, we send it back to the client. When the as_attachment
# parameter is True, then we include in the HTTP response a
# "Content-Disposition: attachment" header, which causes most browsers to bring
# up a "Save-As" popup, rather than displaying the file.
def send_share_file(conn, filename, as_attachment):
    # first, see if we can find the file on this local server
    filedata = get_share_file_locally(filename)

    # if not found, give up
    if filedata is None:
        send_404_not_found(conn)
        return

    # file was found, send it to browser
    mime_type = mimetypes.guess_type(filename)[0]
    if mime_type is None:
        mime_type = "application/octet-stream"

    content = filedata
    content_len = len(content)

    resp = "HTTP/1.1 200 OK\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"
    if as_attachment:
        resp += 'Content-Disposition: attachment; filename="%s"\r\n' % (filename)
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: %s\r\n" % (mime_type)
    log(resp)
    conn.sock.sendall(resp.encode() + b"\r\n" + content)

def send_static_local_file(conn, filename):
    log("Browser asked for a local, static file")
    try:
        with open("./static/" + filename, "rb") as f:
            filedata = f.read()
    except OSError as err:
        logerr("problem opening local file '%s': %s" % (filename, err))
        send_404_not_found(conn)
        return

    mime_type = mimetypes.guess_type(filename)[0]
    if mime_type is None:
        mime_type = "application/octet-stream"

    content = filedata
    content_len = len(content)

    resp = "HTTP/1.1 200 OK\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: %s\r\n" % (mime_type)
    log(resp)
    conn.sock.sendall(resp.encode() + b"\r\n" + content)