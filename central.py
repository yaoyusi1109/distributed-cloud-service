#!/usr/bin/python3
import socket
# Author: Yusi Yao, Mingyu Liu
# Date: 15 October 2022
#
# Code for a cloud file storage central coordinator. This talks to several
# replicas and sometimes also talks to HTTP clients (browsers).d

from dataclasses import dataclass # use python3's dataclass feature
import threading                  # for threading.Thread()
import sys                        # for exiting and command-line args
from fileshare_helpers import *   # for csci356 filesharing helper code
from multithread_logging import * # for csci356 logging helper code


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
my_frontend_port = "80"   # port number for the browser-facing listening socket
my_backend_port = "80"   # port number for peer-facing listening socket
my_region = "us-central1-a"          # geographic region where this server is located

# This condition variable is used to signal that some thread
# crashed, in which case it is time to cleanup and exit the program.
crash_updates = threading.Condition()

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

            # send the client to replica no matter the request
            send_redirect_to_replica(conn, None)

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

# send an HTTP 302 TEMPORARY REDIRECT to bounce client towards replica
def send_redirect_to_replica(conn, status):
    content = "sending to replica"
    content_len = len(content)
    # HARD CODE ID ADDRESS TO REPLICA MACHINE
    ip_ad = "34.67.225.32"
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

    try:# TOOD: something useful.
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

        #Accept http connection
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
        log("Waiting for something to crash...")
        with crash_updates:
            crash_updates.wait()

    except Expection as err:
        logerr("Main initialization failed: %s" % (err))
        raise err

    finally:
        log("Some thread crashed, cleaning up...")
        if s1 is not None:
            s1.close()
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

