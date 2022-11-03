#!/usr/bin/python3

# Author: K. Walsh <kwalsh@holycross.edu>
# Date: 15 October 2022
#
# Code for a cloud file storage server replica. This talks to a central
# coordinator and/or other replicas and/or HTTP clients (browsers).

from helpers import *
from dataclasses import dataclass # use python3's dataclass feature
import threading                  # for threading.Thread()
import sys                        # for exiting and command-line args
from fileshare_helpers import *   # for csci356 filesharing helper code
from multithread_logging import * # for csci356 logging helper code
import requests
import gcp

# This data type represents a collection of information about some other
# replica. You can add or remove variables as you see fit. Use it like this:
#   x = Replica("1.2.3.4", "San Francisco, CA", 6000)
#   print(x)
#   print(x.region)
#   list_of_replicas = []
#   list_of_replicas.append(x)
@dataclass
class Replica:
    dnsname: str   # dns name or IP of the replica, used to open a socket to that replica
    region: str    # geographic region where that replica is located
    backend_portnum: int   # back-end port number which that replica is listening on

####  Global Variables ####

my_name = None            # dns name of this replica
my_frontend_port = None   # port number for the browser-facing listening socket
my_backend_port = None    # port number for peer-facing listening socket
my_region = None          # geographic region where this replica is located

global_condition = threading.Condition() # used to synchronize access to statistics variables
global_central_host = None
global_central_backend_port = None

# This condition variable is used to signal that some thread
# crashed, in which case it is time to cleanup and exit the program.
crash_updates = threading.Condition()

# Given a file and some data, adds this file to our local shared directory and
# our global variable lists. Also updates the statistics about how many files we
# have. Returns a user-friendly status message indicating success or failure.
def add_file(filename, data):
    status = ""
    try:
        with open("./share/" + filename, "wb") as f:
            f.write(data)
        status = "Success, added file '%s'." % (filename)
    except:
        status = "Problem storing data in local file named '%s'." % (filename)
    return status

def getCentralInfo():
    central_host, central_backend_port = None, None
    with global_condition:
        central_host = global_central_host
        central_backend_port = global_central_backend_port
        global_condition.notify_all()
    return central_host, central_backend_port
    
# Handle one browser connection. This will receive an HTTP request, handle it,
# and repeat this as long as the browser says to keep-alive. If there are any
# errors, or if the browser says to close, the connection is closed.
def handle_http_connection(conn):
    global global_central_host, global_central_backend_port

    log("New browser connection from %s:%d" % (conn.client_addr))
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
        
            # GET /index.html or PING
            if req.method == "GET" and req.path.startswith("/ping"):
                send_ok(conn, "central ping is working at replica")
            
            # POST /upload (expects filename(s) and file(s) as html multipart-encoded form parameters)
            elif req.method == "POST" and req.path.startswith("/upload"):
                params = extractPathParams(req.path, "upload")
                filtered_files = params["filelist"].split(",")
                for upload in uploaded_files:
                    if upload in filtered_files:
                        filename = upload.filename
                        contents = upload.data
                        add_file(filename, contents)
                central_host, central_backend_port = getCentralInfo()
                redirect_to_other_server(conn, "", central_host, central_backend_port, "/shared-files.html")

            elif req.method == "GET" and req.path.startswith("/filenames"):
                send_filenames_and_sizes(conn)
                    
    except Exception as err:
        logerr("Front-end connection failed: %s" % (err))
        raise err
    finally:
        log("Closing socket connection with %s:%d" % (conn.client_addr))
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

#### Top level code to start this replica ####

# Given some configuration parameters, this function:
#  - should do something, like open sockets and start threads
#  - should then simply wait forever, until something goes wrong
# If anything goes wrong, then do some cleanup and exit.
def run_replica_server(name, region, frontend_port, backend_port, central_host, central_port):
    logwarn("Starting replica server.")
    log("Replica name: %s" % (name))
    log("Replica region: %s" % (region))
    log("Replica frontend port: %s" % (frontend_port))
    log("Replica backend port: %s" % (backend_port))
    log("Central coordinator is on host %s port %s" % (central_host, central_port))

    myip = gcp.get_my_external_ip()

    url = 'http://' + central_host + ":" + central_backend_port + "/register?" + "ip=" + str(myip) + "&port=" + str(frontend_port)
    r = requests.get(url)
    r.raise_for_status()

    log("Registration at Central Coordinator completed")

    global my_name, my_region, my_frontend_port, my_backend_port
    my_name = name
    my_region = region
    my_frontend_port = frontend_port
    my_backend_port = backend_port
    global_central_host = central_host
    global_central_backend_port = central_backend_port

    listening_addr = my_name
    if listening_addr == "localhost":
        listening_addr = "" # when IP isn't known, blank is better than "localhost"

    s2 = None
    try:
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
        
        log("Waiting for something to crash...")
        with crash_updates:
            crash_updates.wait()
    except Exception as err:
        logerr("Main initialization failed: %s" % (err))
        raise err
    finally:
        log("Some thread crashed, cleaning up...")
        if s2 is not None:
            s2.close()
        log("Finished!")
        sys.exit(1)

# The code below is used when running this program from the command line. If
# another file imports this one (such as the cloud-drive.py file), then code
# won't run. Instead, the other file would call our run_replica_server(...)
# function directly, supplying appropriate parameters.
if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("usage: python3 replica.py name region frontend_portnum backend_portnum central_host central_backend_portnum")
        sys.exit(1)
    name = sys.argv[1]
    region = sys.argv[2]
    frontend_port = int(sys.argv[3])
    backend_port = int(sys.argv[4])
    central_host = sys.argv[5]
    central_backend_port = int(sys.argv[6])

    run_replica_server(name, region, frontend_port, backend_port, central_host, central_backend_port)

