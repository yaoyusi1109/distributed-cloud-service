#!/usr/bin/python3

import socket    # for socket stuff
import sys       # for sys.argv
import time      # for time.time()
import threading # for threading.Thread()

server_host = ""  # emptystring means "use any available network inerface"
server_port = 80


# Get a specific header value from a list of header key-value pairs. If the
# requested key is not found, None is returned instead.
def get_header_value(headers, key):
    for hdr in headers:
        if hdr.lower().startswith(key.lower() + ": "):
            val = hdr.split(" ", 1)[1]
            return val
    return None

# This checks whether the "Connection:" header has value "keep-alive".
def has_keepalive(headers):
    val = get_header_value(headers, "Connection")
    return val != None and val.lower() == "keep-alive"


def handle_one_http_request(c, data, n):
    while b"\r\n\r\n" not in data and b"\n\n" not in data:
        try:
            more_data = c.recv(4096)
            if not more_data:
                return (False, data)
            data = data + more_data
        except Exception as e:
            print(("** recv() error: " + str(e) + " **"))
            return (False, data)

    if b"\r\n\r\n" in data:
        request, unused_data = data.split(b"\r\n\r\n", 1)
    else:
        request, unused_data = data.split(b"\n\n", 1)

    request = request.decode()
    show(request, "request")
    
    # Split into first line and header lines
    lines = request.splitlines()
    first_line = lines[0]
    headers = lines[1:] if len(lines) > 0 else []
    # First line can be further split into words

    # Detect if the client wants to keep this connection alive.
    keep_alive = has_keepalive(headers)

    # Generate a response
    code, mime_type, body = ("200 OK", "text/plain", "Hello World")

    # Send the response
    c.sendall(("HTTP/1.1 " + code + "\r\n").encode())
    c.sendall(b"Server: kwalsh\r\n")
    c.sendall(("Date: " + time.strftime("%a, %d %b %Y %H:%M:%S %Z") + "\r\n").encode())
    if mime_type != None:
        c.sendall(("Content-Type: " + mime_type + "\r\n").encode())
        c.sendall(("Content-Length: " + str(len(body)) + "\r\n").encode())
    else:
        c.sendall(b"Content-Length: 0\r\n")
    if keep_alive:
        c.sendall(b"Connection: keep-alive\r\n")
    else:
        c.sendall(b"Connection: close\r\n")
    c.sendall(b"\r\n")
    c.sendall(body.encode())

    return (keep_alive, unused_data)

def handle_http_connection(c, client_addr):
    keep_alive = True
    data = b""
    n = 0
    try:
        while keep_alive:
            n += 1
            keep_alive, data = handle_one_http_request(c, data, n)
        if len(data) > 0:
            show(data, "leftover")
    except Exception as e:
        print(("** Connection Failed : " + str(e) + " **"))
    finally:
        try:
            c.close()
        except:
            pass
        print(("** Connection Closed : " + str(e) + " **"))

def show(s, title):
    m = "** %d bytes of %s **" % (len(s), title)
    s = s.replace("\r", "\\r")
    s = s.replace("\n", "\\n\n    > ")
    s = s.replace("\t", "\\t")
    if not s.endswith("\n"):
        s = s + "\n"
    print((m + "\n    > " + s))

# This remainder of this file is the main program

if len(sys.argv) >= 2:
    server_port = int(sys.argv[1])

server_addr = (server_host, server_port)
print("Starting web server")
print(("Listening on address", server_addr))
print("Ready for connections...")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(server_addr)
s.listen(5)

try:
    while True:
        c, client_addr = s.accept()
        t = threading.Thread(target=handle_http_connection, args=(c,client_addr))
        t.daemon = True
        t.start()
finally:
    print('Server is shutting down')
    s.close()

