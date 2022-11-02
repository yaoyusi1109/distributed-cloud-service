#!/usr/bin/python3

# Author: K. Walsh <kwalsh@holycross.edu>
# Date: 15 October 2022
#
# A cloud file storage service implemented as a single centralized server, from
# scratch, in Python. Run it like this:
#   ./full-server.py 80 6000
# The first argument is the "front-end" port number, used for browser connections.
# The second argument is the "back-end" port number, used for debugging (and as
# an example of how to have two listening sockets).
#
# There should be a subfolder named "./share/" in the current directory. This is
# used to hold all files to be shared (e.g. uploaded files from users).

# There should be a subfolder named "./static/" in the current directory. This is
# used to hold a few permanent, static files like icons and css style sheets.
#
# This server listens on the given "back-end" port for incoming TCP connections.
# You can connect to this port using pretty much any plain TCP client (such as
# the linux "netcat" or "nc" programs, or "telnet"... for example try the
# command "nc localhost 6000", then just type in your requests. See below for
# details.)
#
# This server also listens on the given "front-end" port for incoming HTTP
# connections. It serves several static and dynamic pages and files on this
# port:
#    GET / (or GET /index.html)
#       -- Redirects browser to "/shared-files.html".
#    GET /dashboard.html
#       -- Dynamically generated html page showing with some statistics.
#    GET /shared-files.html
#       -- Dynamically generated html listing of all shared files.
#    GET /shared-files.html?status=Some+message+for+the+user"
#       -- Same thing, but also includes "Some message for the user" on the
#       generated html page.
#    GET /view/somefile.pdf
#       -- Sends back the file ./share/somefile.pdf, or a 404 NOT FOUND error.
#    GET /download/somefile.pdf
#       -- Same thing, but also includes an HTML header that will cause most
#       browsers to show "Save As" dialog box instead of displaying the file.
#    GET /favicon.ico, GET /fileshare.css, GET /other.xyz
#       -- Sends back the requested file, if found in the ./static/ directory.
#    POST /delete/whatever.pdf (with filename as part of the URL)
#       -- Cause the given shared file to be deleted from the ./share/
#       directory, then redirects back to the main page with a status message.
#    POST /delete (with filename encoded in an html form parameter)
#       -- Same thing, but the filename to be deleted is encoded in an HTML form
#       parameter, rather than as part of the URL.
#    POST /upload (expects filename(s) and file(s) as html multipart-encoded form parameters)
#       -- Takes the uploaded files out of the multipart-encoded form
#       parameters, and stores them in the ./share/ directory, then redirects
#       the browser back to the main page with a status message.
#    OTHERWISE
#       -- For anything else, a 404 NOT FOUND response is sent to the browser.
#
# Note: This code is not very "pythonic"; there are much more concise ways to
# write this code by using various python features like dicts and string
# interpolation. We also avoid use of any modules except for a few very basic
# things. My hope is that this makes it easier to understand everything that is
# happening even if you don't know much python.

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


####  Global Variables ####

my_name = None            # dns name (or IP address) of this server
my_frontend_port = None   # port number for the browser-facing listening socket
my_backend_port = None    # port number for backend-facing listening socket
my_region = None          # geographic region where this server is located

static_file_names = []    # list of static files stored in the ./static/ directory

file_updates = threading.Condition() # used to synchronize access to file-related variables
local_file_names = []     # list of shared files stored locally on this server
local_file_sizes = []     # size of each of those files

stats_updates = threading.Condition() # used to synchronize access to statistics variables
num_connections_so_far = 0  # how many browser connections we have handled so far
num_connections_now = 0     # how many browser connections we are handling right now
num_local_files = 0         # number of shared files stored locally on this server
num_uploads = 0             # how many uploads of shared files we have handled so far
num_downloads = 0           # how many downloads of shared files we have handled so far

# This last condition variable is used to signal that one of our listening sockets
# crashed, in which case it is time to close all sockets and exit the program.
crash_updates = threading.Condition()

#### Some helper code to add and remove shared user files from this server ####

# Given a filename, remove it from our local shared directory. This also updates
# our global variable lists and updates the statistics about how many files we
# have. Returns a user-friendly status message indicating success or failure.
def remove_file(filename):
    status = ""
    with file_updates:
        if filename not in local_file_names:
            status = "No such file '%s'." % (filename)
        else:
            i = local_file_names.index(filename)
            # remove from our lists
            del local_file_names[i]
            del local_file_sizes[i]
            try:
                os.remove("./share/" + filename)
                status = "Success, removed file '%s'." % (filename)
            except:
                status = "Problem removing file '%s'." % (filename)
            file_updates.notify_all()
    global num_local_files
    with stats_updates:
        num_local_files -= 1
        stats_updates.notify_all()
    return status

# Given a file and some data, adds this file to our local shared directory and
# our global variable lists. Also updates the statistics about how many files we
# have. Returns a user-friendly status message indicating success or failure.
def add_file(filename, data):
    status = ""
    with file_updates:
        if filename in local_file_names:
            status = "You have a file named '%s' already." % (filename)
        else:
            # Try to store the data in a file in our "./share/" directory
            try:
                with open("./share/" + filename, "wb") as f:
                    f.write(data)
                local_file_names.append(filename)
                local_file_sizes.append(len(data))
                file_updates.notify_all()
                status = "Success, added file '%s'." % (filename)
            except:
                status = "Problem storing data in local file named '%s'." % (filename)
    global num_local_files, num_uploads
    with stats_updates:
        num_local_files += 1
        num_uploads += 1
        stats_updates.notify_all()
    return status


#### Back-end code for diagnostics, debugging, and demonstration purposes ####

# Handle one connection from the backend. This will receive one line of text
# from the socket, parse the request, then send back a response.
# This is for demonstration purposes, to show how a single python program can be
# listening on two or more sockets simultaneously.
# This is also for diagnostic purposes. If you open a separate terminal window
# you can connect to this socket using the "netcat", "nc", or "telnet" programs.
# For example:
#   netcat localhost 6000  # connects to socket 6000
# You can then type various commands. Try it!
def handle_backend_connection(sock, peer_addr):
    logwarn("New connection to back-end diagnostic port")
    try:
        sock.sendall(("Hello! Welcome to the secret diagnostic port!\n").encode())
        sock.sendall(("  Your address is %s:%d\n" % (peer_addr)).encode())
        sock.sendall(("  my_name = '%s'\n" % (my_name)).encode())
        sock.sendall(("  my_region = '%s'\n" % (my_region)).encode())
        sock.sendall(("Here are the things I know how to do:\n").encode())
        sock.sendall(("  list-files    -- get list of local shared files\n").encode())
        sock.sendall(("  stats         -- get load statistics\n").encode())
        sock.sendall(("  bye           -- disconnect from diagnostic port\n").encode())
        sock.sendall(("  die           -- causes entire server to exit\n").encode()) 

        while True:
            sock.sendall(("What do you want to do?\n").encode())
            line = sock.recv_until(b"\n").decode().strip()
            log("You said: '%s'\n" % (line))
            if line == "list-files":
                files_and_sizes = gather_shared_file_list()
                n = len(files_and_sizes)
                sock.sendall(("There are %d shared files stored here:\n" % n).encode())
                for filename, filesize in files_and_sizes:
                    sock.sendall(("  %s (%d bytes)\n" % (filename, filesize)).encode())
            elif line.startswith("stats"):
                with stats_updates:
                    sock.sendall(("Here are some statistics:\n").encode())
                    sock.sendall(("   %6d http connections so far\n" % (num_connections_so_far)).encode())
                    sock.sendall(("   %6d http connections right now\n" % (num_connections_now)).encode())
                    sock.sendall(("   %6d shared files in this server's ./share/ folder\n" % (num_local_files)).encode())
                    sock.sendall(("   %6d shared files uploaded to this server\n" % (num_uploads)).encode())
                    sock.sendall(("   %6d shared files downloaded from this server\n" % (num_downloads)).encode())
            elif line.startswith("bye"):
                sock.sendall(b"See you later!\n")
                return
            elif line.startswith("die"):
                sock.sendall(b"This server is shutting down now!\n")
                with crash_updates:
                    crash_updates.notify_all()
            else:
                sock.sendall(("I don't understand '%s'\n" % (line)).encode())
    except Exception as err:
        logerr("Back-end connection failed: %s" % (err))
        raise err
    finally:
        logwarn("Closing back-end diagnostic port connection.")
        sock.close()

#### Front-end code for handling web requests ####

# Create a list of all known shared files, along with their sizes.
# This returns a list of (filename, size) pairs.
def gather_shared_file_list():
    # make a copy of the local file lists
    with file_updates:
        all_files = local_file_names.copy()
        all_sizes = local_file_sizes.copy()
    # merge the two lists into a single combined list of pairs, like
    #  [ (filename1, size1), (filename2, size2), (filename3, size3) ... ]
    return list(zip(all_files, all_sizes))

# Check to see if we have a shared file stored locally on this server.
def is_shared_file_stored_locally(filename):
    with file_updates:
        exists = filename in local_file_names
    return exists

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

# Given a socket listening on the backend port, wait for and accept connections
# from hackers, administrators, or whatever, and spawn a thread for each one to
# process the messages arriving on that connection. This code normally runs
# forever, but if it crashes, it will notify the crash_updates variable.
def accept_backend_connections(listening_sock):
    try:
        while True:
            c, a = listening_sock.accept()
            t = threading.Thread(target=handle_backend_connection, args=(SmartSocket(c), a))
            t.daemon = True
            t.start()
    except Exception as err:
        logerr("Back-end listening thread failed: %s" % (err))
        raise err
    finally:
        listening_sock.close()
        with crash_updates:
            crash_updates.notify_all()


#### Code for browser-facing communication using HTTP ####

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

# Send the dynamically-generated main page to the client.
def send_main_page(conn, status=None):
    logwarn("Responding with main page")
    listing = gather_shared_file_list()
    content = make_pretty_main_page(my_region, my_name, listing, status)
    content_len = len(content)

    resp = "HTTP/1.1 200 OK\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: text/html\r\n"
    log(resp)
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

# Send a static local file (like a css file) to the browser.
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

# Send a shared file to the browser. This will first locate the file by checking
# if it is stored locally. If not found, we send a 404 NOT FOUND response. If
# the file is found, we send it back to the client. When the as_attachment
# parameter is True, then we include in the HTTP response a
# "Content-Disposition: attachment" header, which causes most browsers to bring
# up a "Save-As" popup, rather than displaying the file.
def send_share_file(conn, filename, as_attachment):
    log("Browser asked for shared file")
    global num_downloads
    with stats_updates:
        num_downloads += 1
        stats_updates.notify_all()

    # first, see if we can find the file on this local server
    if is_shared_file_stored_locally(filename):
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

# Generate an html page with some diagnostics and statistics, and send
# it as a response to the client.
def send_dashboard_html(conn):
    logwarn("Responding with dashboard page")
    html = "<html><head><title>Non-replicated Cloud File Storage Service, by kwalsh</title></head>"
    html += "<body>"

    html += "<h1>Welcome to kwalsh's Cloud File Storage Service</h1>"

    html += "<p><a href=\"/dashboard.html\">REFRESH</a></p>"

    with stats_updates:
        html += "Here are some statistics:<br>"
        html += " %6d http connections so far<br>" % (num_connections_so_far)
        html += " %6d http connections right now<br>" % (num_connections_now)
        html += " %6d shared files stored this server's ./share/ folder<br>" % (num_local_files)
        html += " %6d shared files uploaded<br>" % (num_uploads)
        html += " %6d shared files downloaded<br>" % (num_downloads)

    html += "<p>Click <a href=\"/shared-files.html\">HERE</a> to go to the main page.</p>"
    html += "</body></html>"

    content = html
    content_len = len(content)

    resp = "HTTP/1.1 200 OK\r\n"
    resp += "Date: %s\r\n" % (http.http_date_now())
    if conn.keep_alive:
        resp += "Connection: keep-alive\r\n"
    else:
        resp += "Connection: close\r\n"
    resp += "Content-Length: %d\r\n" % (content_len)
    resp += "Content-Type: text/html\r\n"
    log(resp)
    conn.sock.sendall(resp.encode() + b"\r\n" + content.encode())


# Handle one browser connection. This will receive an HTTP request, handle it,
# and repeat this as long as the browser says to keep-alive. If there are any
# errors, or if the browser says to close, the connection is closed.
def handle_http_connection(conn):
    log("New browser connection from %s:%d" % (conn.client_addr))
    global num_connections_so_far, num_connections_now
    with stats_updates:
        num_connections_so_far += 1
        num_connections_now += 1
        stats_updates.notify_all()
    try:
        conn.keep_alive = True
        while conn.keep_alive:
            # handle one HTTP request from browser
            req = http.recv_one_request_from_client(conn.sock)
            if req is None:
                logerr("No request?! Something went terribly wrong, dropping connection.")
                break
            log(req)
            conn.num_requests += 1
            conn.keep_alive = req.keep_alive

            # GET /index.html
            # GET /
            if req.method == "GET" and req.path in ["/index.html", "/"]:
                send_redirect_to_main_page(conn, None)

            # GET /shared-files.html
            # GET /shared-files.html?status=Some+message+to+be+displayed+on_page
            elif req.method == "GET" and req.path == "/shared-files.html":
                status = None
                if "status" in req.params:
                    status = req.params["status"]
                send_main_page(conn, status)

            # GET /view/somefile.pdf
            elif req.method == "GET" and req.path.startswith("/view/"):
                send_share_file(conn, req.path[6:], False)

            # GET /download/somefile.pdf
            elif req.method == "GET" and req.path.startswith("/download/"):
                send_share_file(conn, req.path[10:], True)

            # GET /fileshare.css
            # GET /favicon.ico
            # GET /otherstaticfile.xyz
            elif req.method == "GET" and req.path.startswith("/") and req.path[1:] in static_file_names:
                send_static_local_file(conn, req.path[1:])

            # POST /delete (this version expects filename as an html form parameter)
            elif req.method == "POST" and req.path == "/delete":
                filename = req.form_content.get("filename", None)
                if filename is None:
                    logerr("Missing html form or 'filename' form field?")
                    send_redirect_to_main_page(conn, "Sorry, form with filename wasn't submitted.")
                else:
                    status = remove_file(filename)
                    send_redirect_to_main_page(conn, status)

            # POST /delete/whatever.pdf (this version expects filename as part of URL)
            elif req.method == "POST" and req.path.startswith("/delete/"):
                filename = req.path[8:]
                status = remove_file(filename)
                send_redirect_to_main_page(conn, status)

            # POST /upload (expects filename(s) and file(s) as html multipart-encoded form parameters)
            elif req.method == "POST" and req.path == "/upload":
                uploaded_files = req.form_content.get("files", None)
                if uploaded_files is None or len(uploaded_files) == 0:
                    logerr("Missing html form or 'file' form field?")
                    send_redirect_to_main_page(conn, "Sorry, form with file wasn't submitted.")
                else:
                    statuses = []
                    for upload in uploaded_files:
                        filename = upload.filename
                        contents = upload.data
                        status = add_file(filename, contents)
                        statuses.append(status)
                    combined_status = "<br>".join(statuses)
                    send_redirect_to_main_page(conn, combined_status)

            # GET /dashboard.html
            elif req.method == "GET" and req.path == "/dashboard.html":
                send_dashboard_html(conn)

            # None of the above, send 404 error
            else:
                logerr("Unrecognized HTTP request (%s %s)" % (req.method, req.path))
                send_404_not_found(conn)

            log("Done processing request, connection keep_alive is %s" % (conn.keep_alive))
    except Exception as err:
        logerr("Front-end connection failed: %s" % (err))
        raise err
    finally:
        log("Closing socket connection with %s:%d" % (conn.client_addr))
        with stats_updates:
            num_connections_now -= 1
            stats_updates.notify_all()
        conn.sock.close()

# Given a socket listening on the browser-facing front-end port, wait for and
# accept connections from browsers and spawn a thread to handle each connection.
# This code normally runs forever, but if it crashes, it will notify the
# crash_updates variable.
def accept_http_connections(listening_sock):
    try:
        while True:
            c, a = listening_sock.accept()
            conn = http.HTTPConnection(SmartSocket(c), a)
            t = threading.Thread(target=handle_http_connection, args=(conn,))
            t.daemon = True
            t.start()
    except Exception as err:
        logerr("Front-end listening thread failed: %s" % (err))
        raise err
    finally:
        listening_sock.close()
        with crash_updates:
            crash_updates.notify_all()

#### Code to start the full centralized (non-replicated) server ####

# Given some configuration parameters, this function:
#  - Creates a listening socket for the frontend port, and spawns a thread to
#    handle connections arriving at that socket from browsers.
#  - Creates a listening socket for the backend port, and spawns a thread to
#    handle connections arriving at that socket from hackers or whoever.
# If one of these threads crashes, we then close all the sockets and exit the
# program.
def run_full_server(name, region, frontend_port, backend_port):
    logwarn("Starting a fully centralized, non-replicated server.")
    log("Central server name: %s" % (name))
    log("Central server region: %s" % (region))
    log("Central server frontend port: %s" % (frontend_port))
    log("Central server backend port: %s" % (backend_port))

    global my_name, my_region, my_frontend_port, my_backend_port
    my_name = name
    my_region = region
    my_frontend_port = frontend_port
    my_backend_port = backend_port

    global static_file_names
    log("Scanning ./static/")
    static_file_names = os.listdir("./static/")  # list of static files we can serve
    log("This server can serve the following static files:\n%s\n" % ("\n".join(static_file_names)))

    global local_file_names, num_local_files, local_file_sizes
    log("Scanning ./share/")
    local_file_names = os.listdir("./share/")  # list of shared user files we have locally
    num_local_files = len(local_file_names)
    for f in local_file_names:
        local_file_sizes.append(os.path.getsize("./share/" + f))
    log("There are %d shared user files stored locally on this server." % (num_local_files))
    for i in range(num_local_files):
        log("   %10d  %s" % (local_file_sizes[i], local_file_names[i]))

    listening_addr = my_name
    if listening_addr == "localhost":
        listening_addr = "" # when IP isn't known, blank is better than "localhost"

    # There are 2 sockets and 2 threads
    s1 = None
    s2 = None
    try:
        # First socket is our backend socket listening for backend connections
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        addr1 = (listening_addr, backend_port)
        s1.bind(addr1)
        s1.listen(5)
        # Spawn thread to wait for and accept connections from hackers or whatever
        t1 = threading.Thread(target=accept_backend_connections, args=(s1,))
        t1.daemon = True
        t1.start()

        # Second socket is our frontend socket listening for browser connections
        s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s2.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        addr2 = (listening_addr, frontend_port)
        s2.bind(addr2)
        s2.listen(5)
        # Spwan thread to wait for and accept connections from browsers
        t2 = threading.Thread(target=accept_http_connections, args=(s2,))
        t2.daemon = True
        t2.start()

        logwarn("Waiting for one of our main threads or sockets to crash...")
        with crash_updates:
            crash_updates.wait()

    except Exception as err:
        logerr("Main initialization failed: %s" % (err))
        raise err
    finally:
        logerr("Some thread or socket crashed, closing all sockets...")
        if s1 is not None:
            s1.close()
        if s2 is not None:
            s2.close()
        logerr("Finished!")


# Main code for running this file directly from the command line
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python3 full-server.py frontend_port_num backend_port_num")
        sys.exit(1)
    name = "localhost"
    region = "Narnia"
    frontend_port = int(sys.argv[1])
    backend_port = int(sys.argv[2])
    run_full_server(name, region, frontend_port, backend_port)
