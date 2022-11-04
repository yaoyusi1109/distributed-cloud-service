#!/usr/bin/python3

# Author: K. Walsh <kwalsh@holycross.edu>
# Date: 15 October 2022
#
# Code for a cloud file storage central coordinator. This talks to several
# replicas and sometimes also talks to HTTP clients (browsers).

from helpers import *
import random
import requests

from dataclasses import dataclass # use python3's dataclass feature
import threading                  # for threading.Thread()
import sys                        # for exiting and command-line args
from fileshare_helpers import *   # for csci356 filesharing helper code
from multithread_logging import * # for csci356 logging helper code
from collections import defaultdict

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

my_name = None            # dns name of this server
my_frontend_port = None   # port number for the browser-facing listening socket
my_backend_port = None    # port number for peer-facing listening socket
my_region = None          # geographic region where this server is located

stats_updates = threading.Condition() # used to synchronize access to statistics variables
num_connections_so_far = 0  # how many browser connections we have handled so far
num_connections_now = 0     # how many browser connections we are handling right now
num_local_files = 0         # number of shared files stored locally on this server
num_uploads = 0             # how many uploads of shared files we have handled so far
num_downloads = 0           # how many downloads of shared files we have handled so far

# val: replica_ip_port_tuple (ip,port)
replicaset = set()
# key: filename
# val: (ip,port)
locations = defaultdict(tuple)

# This condition variable is used to signal that some thread
# crashed, in which case it is time to cleanup and exit the program.
crash_updates = threading.Condition()

# Create a list of all known shared files, along with their sizes.
# This returns a list of (filename, size) pairs.
def gather_shared_file_list():
    global replicaset, locations

    all_files, all_sizes = [], []
    old_replicas_list = []
    new_replicas = set()
    new_locations = defaultdict(tuple)

    with stats_updates:
        old_replicas_list = list(replicaset)
        stats_updates.notify_all()

    for replica_ip_port_tuple in old_replicas_list:
        replica_ip = replica_ip_port_tuple[0]
        replica_port = replica_ip_port_tuple[1]
        url = 'http://' + replica_ip + ":" + replica_port + "/filenames"
        r = requests.get(url)
        log("GATHERING FILE LIST from %s" % url)
        if r.status_code == 200:
            new_replicas.add(replica_ip_port_tuple)
            allfiles_string = r.content.decode("utf-8")
            if allfiles_string == "":
                continue
            log("response string:%s" % r.content)
            parsed_files = allfiles_string.split('&')
            for file_string in parsed_files:
                if file_string in new_locations:
                    continue
                parsed_f = file_string.split(',')
                all_files.append(parsed_f[0])
                all_sizes.append(int(parsed_f[1]))
                new_locations[file_string] = (replica_ip, replica_port)
        else:
            logwarn("cannot connect to replica ip: %s", replica_ip)
    
    with stats_updates:
        replicaset = new_replicas
        locations = new_locations
        stats_updates.notify_all()
    
    return list(zip(all_files, all_sizes))

def getFileReplicaTuple(filename):
    gather_shared_file_list()
    replicaTuple = None
    with stats_updates:
        replicaTuple = locations[filename]
        stats_updates.notify_all()
    log("replica tuple returning info %s" % str(replicaTuple))
    return replicaTuple

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

# Handle one browser connection. This will receive an HTTP request, handle it,
# and repeat this as long as the browser says to keep-alive. If there are any
# errors, or if the browser says to close, the connection is closed.
def handle_http_connection(conn):
    global replicaset, locations, num_connections_so_far, num_connections_now

    static_file_names = os.listdir("./static/")  # list of static files we can serve

    log("New browser connection from %s:%d" % (conn.client_addr))
    with stats_updates:
        num_connections_so_far += 1
        num_connections_now += 1
        log("Current number of connections %s" % str(num_connections_now))
        stats_updates.notify_all()
    try:
        conn.keep_alive = True
        while conn.keep_alive:
            log("Waiting for next request")
            # handle one HTTP request from browser
            req = http.recv_one_request_from_client(conn.sock)
            if req is None:
                logerr("No request?! Something went terribly wrong, dropping connection.")
                break
            log(req)
            conn.num_requests += 1
            conn.keep_alive = req.keep_alive
        
            # GET /index.html
            if req.method == "GET" and req.path in ["/index.html", "/"]:
                log("Handler: GET /index.html")
                send_redirect_to_main_page(conn, None)

            # GET /shared-files.html
            # GET /shared-files.html?status=Some+message+to+be+displayed+on_page
            elif req.method == "GET" and req.path == "/shared-files.html":
                log("Handler: GET /shared-files.html")
                status = None
                if "status" in req.params:
                    status = req.params["status"]
                log("Begin trasmitting main page")
                send_main_page(conn, status)
                log("Main page send completed!!!")
            
            # GET FROM REPLICA /register
            elif req.method == "GET" and req.path.startswith("/register"):
                params = req.params
                ip = params["ip"]
                port = params["port"]
                with stats_updates:
                    replicaset.add((ip, port))
                    log("Registering replica ip:port %s" % (str(ip) + ":" + str(port)))
                    stats_updates.notify_all()
                send_ok(conn, "cool")

            elif req.method == "GET" and req.path.startswith("/") and req.path[1:] in static_file_names:
                send_static_local_file(conn, req.path[1:])
            
            # POST FROM CLIENT /upload
            elif req.method == "POST" and req.path == "/upload":
                uploaded_files = req.form_content.get("files", None)
                if uploaded_files is None or len(uploaded_files) == 0:
                    logerr("Missing html form or 'file' form field?")
                    send_redirect_to_main_page(conn, "Sorry, form with file wasn't submitted.")
                else:
                    # find a working replica
                    # refresh replicaset
                    gather_shared_file_list()
                    replicas_list = []
                    fileset = set()
                    with stats_updates:
                        fileset = set(locations.keys())
                        replicas_list = list(replicaset)
                        stats_updates.notify_all()

                    if len(replicas_list) == 0:
                        logerr("ERR!!!!!! All the replicas are dead!!!!!!!!!")
                        send_redirect_to_main_page(conn, "Sorry, all the replicas are dead.")
                        raise Exception("all replicas are dead!")
                    
                    filtered_file_names = []
                    for upload in uploaded_files[:]:
                        filename = upload.filename
                        if not filename in fileset:
                            filtered_file_names.append(filename)

                    replica_ip_port_tuple = random.choice(replicas_list)
                    replica_ip = replica_ip_port_tuple[0]
                    replica_port = replica_ip_port_tuple[1]
                    url = 'http://' + replica_ip + ":" + replica_port + "/ping"
                    r = requests.get(url)
                    if r.status_code != 200:
                        raise Exception("ping failure during upload !!!!!!")
                    redirect_to_other_server(conn, "", replica_ip, replica_port, "/upload?filelist=" + ','.join(filtered_file_names))

            # POST /delete (this version expects filename as an html form parameter)
            elif req.method == "POST" and req.path == "/delete":
                filename = req.form_content.get("filename", None)
                if filename is None:
                    logerr("Missing html form or 'filename' form field?")
                    send_redirect_to_main_page(conn, "Missing html form or 'filename' form field?")
                else:
                    replica_ip, replica_port = getFileReplicaTuple(filename)
                    redirect_to_other_server(conn, "", replica_ip, replica_port, req.path)
            
             # POST /delete/whatever.pdf (this version expects filename as part of URL)
            elif req.method == "POST" and req.path.startswith("/delete/"):
                filename = req.path[8:]
                replica_ip, replica_port = getFileReplicaTuple(filename)
                redirect_to_other_server(conn, "", replica_ip, replica_port, req.path)
            
            # GET /view/somefile.pdf
            elif req.method == "GET" and req.path.startswith("/view/"):
                filename = req.path[6:]
                replica_ip, replica_port = getFileReplicaTuple(filename)
                redirect_to_other_server(conn, "", replica_ip, replica_port, req.path)

            # GET /download/somefile.pdf
            elif req.method == "GET" and req.path.startswith("/download/"):
                filename = req.path[10:]
                replica_ip, replica_port = getFileReplicaTuple(filename)
                redirect_to_other_server(conn, "", replica_ip, replica_port, req.path)
            
            else:
                send_404_not_found(conn)

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

#### Top level code to start this central coordinator server  ####

# Given some configuration parameters, this function:
#  - should do something, like open sockets and start threads
#  - should then simply wait forever, until something goes wrong
# If anything goes wrong, then do some cleanup and exit.
def run_central_server(name, region, frontend_port, backend_port):
    logwarn("Starting central coordinator.")
    log("Central coordinator name: %s" % (name))
    log("Central coordinator region: %s" % (region))
    log("Central coordinator frontend port: %s" % (frontend_port))
    log("Central coordinator backend port: %s" % (backend_port))

    global my_name, my_region, my_frontend_port, my_backend_port
    my_name = name
    my_region = region
    my_frontend_port = frontend_port
    my_backend_port = backend_port
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
# won't run. Instead, the other file would call our run_central_server(...)
# function directly, supplying appropriate parameters.
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("usage: python3 central.py name region frontend_portnum backend_portnum")
        sys.exit(1)
    name = sys.argv[1]
    region = sys.argv[2]
    frontend_port = int(sys.argv[3])
    backend_port = int(sys.argv[4])

    run_central_server(name, region, frontend_port, backend_port)

