#!/usr/bin/python3
# aws.py
# Basic location data for Amazon Web Services EC2 datacenters.
#
# Author: K. Walsh <kwalsh@cs.holycross.edu>
# Date: 9 February 2017
#
# Amazon does not seem to publish information about the actual physical
# locations of their AWS EC2 datacenters, even though they sometimes use
# suggestive titles like "US East (N. Virginia)". 
##
# Each AWS "region" contains several "availability zones", and each
# "availability zone" contains several "data centers". The data centers are
# the physical buildings that house the enormous number of computers that make
# up EC2. The actual street addresses for the data centers are not public, and
# the buildings themselves can be a little difficult to find (see [1] for
# example). Even finding the approximate city or county involves a little
# guesswork. As best I am able, I've compiled below the name of the nearest
# major city where each region's datacenters seem to be housed, and the
# geographic latitude, longitude coordinates for those locations.
#
# Some of the informabion below comes from Turnkey Linux [2]. Those folks are using
# this information as part of their project, but they don't seem to cite their
# sources for this information.
#
# [1]: https://www.theatlantic.com/technology/archive/2016/01/amazon-web-services-data-center/423147/
# [2]: https://github.com/turnkeylinux/aws-datacenters/blob/master/input/datacenters

# Availability zones have names like us-east-1a or us-east-1b. To get the
# region name, we can just remove the last letter.
def region_for_zone(z):
    lastchar = z[len(z)-1]
    if lastchar >= 'a' and lastchar <= 'z':
        return z[0:len(z)-1]
    else:
        return z

# names of all AWS regions
regions = [
    "us-east-1", "us-east-2", "us-west-1", "us-west-2",
    "af-south-1",
    "ap-east-1", "ap-south-1", "ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
    "ca-central-1",
    "eu-central-1", "eu-west-1", "eu-west-2", "eu-west-3", "eu-south-1", "eu-north-1",
    "me-south-1",
    "sa-east-1",
]

# official english-language title of each AWS region
region_titles = {
    "af-south-1":        "Africa (Cape Town)",
    "ap-east-1":         "Asia Pacific (Hong Kong)",
    "ap-northeast-1":    "Asia Pacific (Tokyo)", 
    "ap-northeast-2":    "Asia Pacific (Seoul)", 
    "ap-south-1":        "Asia Pacific (Mumbai)", 
    "ap-southeast-1":    "Asia Pacific (Singapore)", 
    "ap-southeast-2":    "Asia Pacific (Sydney)", 
    "ca-central-1":      "Canada (Central)", 
    "eu-central-1":      "EU (Frankfurt)", 
    "eu-north-1":        "EU (Stockholm)",
    "eu-south-1":        "EU (Milan)",
    "eu-west-1":         "EU (Ireland)", 
    "eu-west-2":         "EU (London)", 
    "eu-west-3":         "EU (Paris)",
    "me-south-1":        "Middle East (Bahrain)",
    "sa-east-1":         "South America (Sao Paulo)", 
    "us-east-1":         "US East (N. Virginia)", 
    "us-east-2":         "US East (Ohio)", 
    "us-west-1":         "US West (N. California)", 
    "us-west-2":         "US West (Oregon)", 
}

# city where each AWS datacenter is located, approximately
region_cities = {
    "af-south-1":        "Cape Town",
    "ap-east-1":         "Hong Kong",
    "ap-northeast-1":    "Tokyo",
    "ap-northeast-2":    "Seoul",
    "ap-south-1":        "Mumbai",
    "ap-southeast-1":    "Singapore",
    "ap-southeast-2":    "Sydney",
    "ca-central-1":      "Montreal",
    "eu-central-1":      "Frankfurt",
    "eu-north-1":        "Stockholm",
    "eu-south-1":        "Milan",
    "eu-west-1":         "Dublin", # not sure where exactly
    "eu-west-2":         "London",
    "eu-west-3":         "Paris",
    "me-south-1":        "Bahrain", # not sure where exactly
    "sa-east-1":         "Sao Paulo",
    "us-east-1":         "Charlottesville",
    "us-east-2":         "Columbus",
    "us-west-1":         "Palo Alto",
    "us-west-2":         "Oregon", # not sure where exactly
}

# (latitude, longitude) where each AWS datacenter is located, approximately
region_coords = {
    "af-south-1":      (-33.93, 18.42),
    "ap-east-1":       (22.28, 114.26),
    "ap-northeast-1":  (35.41, 139.42),
    "ap-northeast-2":  (37.57, 126.98),
    "ap-south-1":      (19.08, 72.88),
    "ap-southeast-1":  (1.37, 103.80),
    "ap-southeast-2":  (-33.86, 151.20),
    "ca-central-1":    (45.50, -73.57),
    "eu-central-1":    (50.1167, 8.6833),
    "eu-north-1":      (59.39, 17.87),
    "eu-south-1":      (45.51, 9.24),
    "eu-west-1":       (53.35, -6.26),
    "eu-west-2":       (51.51, -0.13),
    "eu-west-3":       (48.93, 2.35),
    "me-south-1":      (26.15, 50.47),
    "sa-east-1":       (-23.34, -46.38),
    "us-east-1":       (38.13, -78.45),
    "us-east-2":       (39.96, -83.00),
    "us-west-1":       (37.44, -122.14),
    "us-west-2":       (46.15, -123.88),
}

def get_my_external_ip():
    import requests
    r = requests.get('http://169.254.169.254/latest/meta-data/public-ipv4')
    r.raise_for_status()
    return r.text

def get_my_dns_hostname():
    import requests
    r = requests.get('http://169.254.169.254/latest/meta-data/public-hostname')
    r.raise_for_status()
    return r.text

def get_my_zone():
    import requests
    r = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone/')
    r.raise_for_status()
    return r.text

# test code
if __name__ == "__main__":
    print(("There are %d Amazon Web Services regions." % (len(regions))))
    print(("%-16s %-26s %-36s %s, %s" % ("zone", "title", "city", "lat", "lon")))
    for r in regions:
        (lat, lon) = region_coords[r]
        print(("%-16s %-26s %-36s %0.2f, %0.2f" % (r, region_titles[r], region_cities[r], lat, lon)))

