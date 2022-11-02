# Author: K. Walsh <kwalsh@holycross.edu>
# Date: 15 October 2022

# Helper code for writing http servers.
# Intended usage:
#   import http_helpers
# or:
#   import http_helpers as http
# or:
#   from http_helpers import *

import urllib.parse
import traceback
import sys
import time
import string
from dataclasses import dataclass
from multithread_logging import *
from requests.structures import CaseInsensitiveDict

# HTTPConnection objects are used to hold information associated with a single HTTP
# connection socket, like the socket itself, statistics, a keep-alive flag, and
# any other pertenent information desired.
class HTTPConnection:
    def __init__(self, c, addr):
        self.sock = c             # the socket connected to the client
        self.client_addr = addr   # address of the client
        self.keep_alive = True    # whether this is a persistent connection
        self.num_requests = 0     # number of HTTP requests from client handled so far

## NOTE: This next data type isn't used anywhere, but it could be useful I guess?
## # HTTPResponse objects are used to hold information associated with a single
## # HTTP response that will be sent to a client. The code is required, and should
## # be something like "200 OK" or "404 NOT FOUND". The mime_type and body are
## # optional and can be None if not needed. If present, the mime_type should be
## # something like "text/plain" or "image/png", and the body should be a string or
## # raw bytes object containing contents appropriate for that mime type.
## class HTTPResponse:
##     def __init__(self, code, mime_type=None, body=None):
##         self.code = code
##         self.mime_type = mime_type
##         self.body = body

# An HTTPRequest object holds all data associated with one http request received
# from a client (i.e. web browser). It stores the raw request bytes, and it also
# has variables for all of the decoded parts of the request.
@dataclass
class HTTPRequest:
    # We save the entire first line and headers, for debugging and printing
    summary: str = None

    # The first three parts come straight from the first line of the HTTP request
    method: str = None       # "GET", "PUT", "POST", etc.
    urlpath: str = None      # "/my%20file.html?name=Binta%20Bah" or similar, with spaces encoded as "%20", etc.
    version: str = None      # "HTTP/1.1" or similar

    # The urlpath can have a '?' followed by parameters, like '/chat?name=Binta%20Bah',
    # and it also has spaces encoded as "%20" These next two are taken from urlpath,
    # but split in half and unquoted.
    path: str = None         # the part before the '?', unquoted. Example: "/my file.html"
    params: dict = None      # the parameters after '?', unquoted, as a dictionary. Example: { "name": "Binta Bah" }

    # After the first line of the HTTP request comes some headers
    headers: dict = None     # a python dictionary of all headers
    keep_alive: bool = False # taken from headers['Keep-Alive'], or False if missing header
    content_length: int = 0  # taken from headers['Content-Length'], or 0 if missing header

    # Most HTTP requests end here, but PUT and POST requests often have a body too.
    # The remainder of the variables are only relevant when method is "POST" or "PUT".
    content: bytes = b""     # raw content bytes from request, may be empty

    # For POST and PUT, if the content is text/plain or text/html, we decode it here.
    plaintext_content: str = "" # decoded plain text content

    # For POST and PUT, if the content is x-www-form-urlencoded or
    # multipart/form-data, we decode it here. The values are all MultipartData
    # objects, which contain bytes, the field name, and an optional filename and
    # mimetype.
    form_content: dict = None   # decoded content as dictionary of key-value pairs

    def __repr__(self):
        if len(self.content) > 0 and len(self.plaintext_content) > 0:
            return self.summary + self.plaintext_content
        elif len(self.content) > 0 and len(self.content) <= 500:
            try:
                return self.summary + self.content.decode()
            except:
                return self.summary + "[%d bytes of binary payload, not shown here]" % (len(self.content))
        elif len(self.content) > 0:
            return self.summary + "[%d bytes of payload, not shown here]" % (len(self.content))
        else:
            return self.summary

# MultipartFormData is used for the form_content of POST requests when the html
# form encoding is set to "multipart/form-data", used when uploading files.
@dataclass
class MultipartFormData:
    fieldname: str
    mimetype: str # optional
    filename: str # optional
    data: bytes

# Given a urlpath query string return a dictionary with all the key-value
# pairs, unquoted. For example:
#   d = parse_urlencoded_params("name=Binta%20Bah&fav=Blue")
#   print(d["name"])  # prints "Binta Bah"
#   print(d["fav"])   # prints "Blue"
def parse_urlencoded_params(paramstr):
    d = {}
    for part in paramstr.split("&"):
        if "=" in part:
            key, val = part.split("=", 1)
            key = urllib.parse.unquote_plus(key)
            val = urllib.parse.unquote_plus(val)
            if key.endswith("[]"):
                key = key[0:-2]
                if key not in d:
                    d[key] = []
                d[key].append(val)
            else:
                d[key] = val
    return d

# Given a multi-line string of HTTP headers, return a dictoinary with all the
# key-value pairs. The keys are usually simple strings like "Content-Type" or
# "Keep-Alive". The values are usually simple strings. If there are multiple
# headers with the same key, all of the resulting values are joined in a single
# comma-separated string.
# HTTP spec, but this shouldn't happen much in practice). For some headers,
# the value string can be more complicated and might need further decoding.
def parse_http_headers(headers):
    #d = CaseInsensitiveDictWithDefault()
    d = CaseInsensitiveDict()
    key = ""
    for line in headers.split("\r\n"):
        if len(line) == 0:
            break
        elif line[0] in " \t":
            # this line is a continuation of the previous key:value pair
            d[key] = d[key] + ", " + line.strip()
        else:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()
            if key in d:
                # this key was already seen before, add new value to previous
                d[key] = d[key] + ", " + val
            else:
                # this is a new key, put it in dictionary
                d[key] = val
    return d

# Given the Content-Type and contents of a multipart/form-data POST request,
# this parses the contents into a dictionary of key-value pairs. The
# The ctype paramater, taken from the request Content-Type header, should look
# like:
#    multipart/form-data; boundary=MARKER
# And the content will be a series of boundary-separated blocks, like:
#   --MARKER
#   Content-Type: application/pdf
#   Content-Disposition: form-data; name="field1"; filename="foo.pdf"
#
#   value1
#   --MARKER
#   Content-Type: text/plain
#   Content-Disposition: form-data; name="field2"
#
#   value2
#   --MARKER--
def parse_multipart_form_data(ctype, contents):
    # get the boundary marker from the ctype
    ctype_params = ctype.split(";", 1)[1].strip()
    if not ctype_params.lower().startswith("boundary="):
        logerr("POST request multipart/form-data is missing boundary marker")
        return {}
    boundary = ctype_params.split("=", 1)[1]
    # some browsers put quotes around the boundary marker, so remove them
    if boundary.startswith('"') and boundary.endswith('"'):
        boundary = boundary[1:-1]
    d = {}
    sep = b"--" + boundary.encode()
    section_start = sep + b"\r\n"
    message_end = sep + b"--\r\n"
    body = contents
    while body.startswith(section_start):
        # we are at start of a new section
        body = body[len(section_start):]
        headers, blankline, body = body.partition(b"\r\n\r\n")
        data, separator, body = body.partition(b"\r\n" + sep)
        body = sep + body
        # parse the section headers
        headers = parse_http_headers(headers.decode())
        # grab the mimetype header, if present
        mimetype = None
        if "Content-Type" in headers:
            mimetype = headers["Content-Type"]
        # grab the content-disposition header, which has the field name and filename
        name, filename = parse_content_disposition(headers["Content-Disposition"])
        log("Request has multipart segment with name '%s', filename '%s', mimetype '%s', body=%dbytes" % (name, filename, mimetype, len(data)))
        # if disp is None:
        #     continue
        # name = disp["name"]
        # filename = disp["filename"]
        # if "filename*" in disp:
        #     filename = disp["filename*"]
        p = MultipartFormData(fieldname=name, mimetype=mimetype, filename=filename, data=data)
        if name.endswith("[]"):
            # put multiple values of this field into an array
            name = name[0:-2]
            if name not in d:
                d[name] = []
            d[name].append(p)
        else:
            # only keep the last value of this field if it is repeated
            d[name] = p
    # sanity check for end of multipart data
    if body != message_end:
        logerr("missing multipart tailing separator")
    return d

# Given a multipart content disposition string, return the field name and
# filename after removing quotes and decoding the data.
# grab the content-disposition header, which should look like
#   form-data; name="field1"; filename="foo.pdf"
# Note: this doesn't handle the filename* parameter, which would be needed when
# filenames contain spaces or unusual characters.
def parse_content_disposition(disp):
    disp = disp.strip()
    if not disp.startswith("form-data;"):
        logerr("POST request multipart/form-data segment has bad Content-Disposition: " + disp)
        return "field", "unknown-file.txt"
    name = None
    i = disp.find('name="')
    if i >= 0:
        j = disp.find('"', i+6)
        if j < 0:
            logerr("POST request multipart/form-data segment has bad Content-Disposition: " + disp)
            return "field", "unknown-file.txt"
        name = disp[i+6:j]
    filename = "unknown-file.txt" # a default filename
    i = disp.find('filename="')
    if i >= 0:
        j = disp.find('"', i+10)
        if j < 0:
            logerr("POST request multipart/form-data segment has bad Content-Disposition: " + disp)
            return "field", "unknown-file.txt"
        filename = disp[i+10:j]
    return name, filename

# Given a socket connected to some http client, this function receives one HTTP
# request. It decodes the request and returns an HTTPRequest object containing
# all the data in the request. If anything goes wrong, it simply returns None to
# indicate failure.
def recv_one_request_from_client(client_sock):
    try:
        req = client_sock.recv_until(b"\r\n\r\n")
        if not req:
            logerr("Error receiving HTTP request: maybe connection was closed prematurely?")
            return None
    except Exception as err:
        logerr("Error receiving HTTP request: %s" % (str(err)))
        traceback.print_exception(*sys.exc_info())
        return None

    try:
        reqstring = req.decode() # convert bytes to string
        firstline, headers = reqstring.split("\r\n", 1)
        method, urlpath, version = firstline.split(" ", 2)

        # save the first few variables
        req = HTTPRequest()
        req.summary = reqstring
        req.method = method
        req.urlpath = urlpath
        req.version
        req.headers = parse_http_headers(headers)

        # decode the urlpath
        if "?" in urlpath:
            path, params = urlpath.split("?", 1)
            req.path = urllib.parse.unquote(path)
            req.params = parse_urlencoded_params(params)
        else:
            req.path = urllib.parse.unquote(urlpath)
            req.params = {}

        # grab the keepalive and content-length headers, and the content if present
        req.keep_alive = "Connection" in req.headers and req.headers["Connection"].lower() == "keep-alive"
        if "Content-Length" in req.headers:
            req.content_length = int(req.headers["Content-Length"])
            req.content = client_sock.recv_exactly(req.content_length)
        else:
            req.content_length = 0
            req.content = b""

        # for POST requests, decode the uploaded files and form data
        req.form_content = { }
        if req.content_length > 0 and "Content-Type" in req.headers:
            ctype = req.headers["Content-Type"]
            if "application/x-www-form-urlencoded" in ctype.lower():
                req.plaintext_content = req.content.decode()
                req.form_content = parse_urlencoded_params(req.plaintext_content)
            elif "multipart/form-data" in ctype.lower():
                req.form_content = parse_multipart_form_data(ctype, req.content)
            if "text/plain" in ctype.lower() or "text/html" in ctype.lower():
                req.plaintext_content = req.content.decode()

        return req

    except Exception as err:
        logerr("Error parsing HTTP request: %s\n%s" % (err, str(req)))
        traceback.print_exception(*sys.exc_info())
        return None

# Get the current date in the format needed for the HTTP "Date:" response header.
def http_date_now():
    return time.strftime("%a, %d %b %Y %H:%M:%S %Z")

# CaseInsensitiveDictWithDefault is just like dict, the built-in python
# dictionary type, but it ignores case for the keys, and when getting the value
# associated with a key it will default to None if that key is not found.
# Expected usage:
#   d = CaseInsensitiveDictWithDefault()
#   d["CoNTeNt-LENGtH"] = 25
#   d["CONNECTION"] = "Keep-Alive"
#   if d["Content-Length"] > 20: ... # this condition will be true
#   mimetype = d["Mime-Type"] # returns None, because key was not set
class CaseInsensitiveDictWithDefault(CaseInsensitiveDict):
    def __getitem__(self, key):
        # We allow fall-through here, so values default to None
        try:
            return super(CaseInsensitiveDictWithDefault, self).__getitem__(key)
        except KeyError:
            return None

# make_printable() does some conversions on a string so that it prints nicely
# on the console while still showing unprintable characters (like "\r") in 
# a sensible way.
printable = string.ascii_letters + string.digits + string.punctuation + " \r\n\t"
def make_printable(s):
    s = s.replace("\n", "\\n\n")
    s = s.replace("\t", "\\t")
    s = s.replace("\r", "\\r")
    s = s.replace("\r", "\\r")
    return ''.join(c if c in printable else r'\x{0:02x}'.format(ord(c)) for c in s)
