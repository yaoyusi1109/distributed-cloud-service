#!/usr/bin/python3

# Author: K. Walsh <kwalsh@holycross.edu>
# Date: 15 October 2022
#
# Cloud file storage service top level program.
#
# This program is meant to make it easier to start the central coordinator and
# replicas. For the central coordinator, we assume there is a function:
#
#    run_central_coordinator(dns_name, region,
#                            central_frontend_port, central_backend_port)
#
# And for the replicas, we assume there is a function
#
#    run_replica_server(dns_name, region,
#                       replica_frontend_port, replica_backend_port,
#                       central_host, central_backend_port)
#
# NOTE: If you need more or different parameters, you can change the code below.
# You might also need to change fabfile.py
#
# When executed (with a few command-line arguments), this program will
# figure out what our own DNS name and IP address are, figure out what
# geographic region we are in, figure out whether this local server should be
# the central coordinator or one of the replicas, then call the appropriate
# function with appropriate parameters.
#
# The command-line paramaters needed to make this work are:
#    - the dns name (or IP address) of the central server
#    - various port numbers, to pass as arguments to the above functions
# All the replicas get the exact same parameters, except for their own name and
# region.
#
# For example:
#
#   ./cloud-drive.py 34.94.207.48  8000  6000  8000 6000
#                         |         |     |     |    |
#            central_host-'         |     |     |    |
#          central_frontend_portnum-'     |     |    |
#                 central_backend_portnum-'     |    |
#                      replica_frontend_portnum-'    |
#                            replica_backend_portnum-'
#
# Note: in this example, the central coordinator and all the replicas will
# use 8000 for their front-end ports, and 6000 for their back-end ports.


import sys            # for sys.argv
import aws            # for aws.region_for_zone
import gcp            # for gcp.region_for_zone
import cloud          # for cloud.region_cities, etc.

# Get the central_host name and various port number arguments from the command line.
central_host = sys.argv[1]
central_frontend_port = int(sys.argv[2])
central_backend_port = int(sys.argv[3])
replica_frontend_port = int(sys.argv[4])
replica_backend_port = int(sys.argv[5])

# Figure out our own host name. 
try:
    # First try AWS meta-data service to figure out our own ec2 availability zone and region.
    dns_name = aws.get_my_dns_hostname()
    ipaddr = aws.get_my_external_ip()
    zone = aws.get_my_zone()
    region = aws.region_for_zone(zone)
except:
    # Next try GCP meta-data service.
    dns_name = gcp.get_my_internal_hostname()
    ipaddr = gcp.get_my_external_ip()
    zone = gcp.get_my_zone()
    region = gcp.region_for_zone(zone)

if dns_name == central_host or ipaddr == central_host:
    # If we are the central coordinator host...
    # then call some function that implements the front end and central
    # coordinator. 
    print(("Starting central coordinator at http://%s:%s/" % (dns_name, central_frontend_port)))
    from central import *
    run_central_coordinator(dns_name, region,
            central_frontend_port, central_backend_port)
else:
    # Otherwise, we are one of the replica server hosts...
    # then call some function that implements the replica server.
    print(("Starting replica server within region %s (city: %s, coordinates: %s)" % (
            region, cloud.region_cities[region], cloud.region_coords[region])))
    from replica import *
    run_replica_server(dns_name, region,
            replica_frontend_port, replica_backend_port,
            central_host, central_frontend_port)

