#!/usr/bin/python3
# gcp.py
# Basic location data for Google Cloud Platform GCE datacenters.
#
# Author: K. Walsh <kwalsh@cs.holycross.edu>
# Date: 9 February 2017
#
# Google is a bit more public about the actual physical locations of their GCP
# GCE datacenters than Amazon, but the location data below is still only an
# approximation.
##
# Each GCP "region" contains several "availability zones", and each
# "availability zone" contains several "data centers". The data centers are
# the physical buildings that house the enormous number of computers that make
# up GCE.

# Availability zones have names like us-east1-a or us-east1-b. To get the
# region name, we can just remove the last letter and the dash .
def region_for_zone(z):
    lastchar = z[len(z)-1]
    penultimatechar = z[len(z)-2]
    if lastchar >= 'a' and lastchar <= 'z' and penultimatechar == '-':
        return z[0:len(z)-2]
    else:
        return z

# names of all GCP regions
regions = [
    "asia-east1", "asia-east2", "asia-northeast1", "asia-northeast2", "asia-northeast3", "asia-south1", "asia-southeast1", "asia-southeast2",
    "australia-southeast1",
    "europe-north1", "europe-west1", "europe-west2", "europe-west3", "europe-west4", "europe-west6",
    "northamerica-northeast1",
    "southamerica-east1",
    "us-central1", "us-east1", "us-east4", "us-west1", "us-west2", "us-west3", "us-west4",
]

# official english-language title of each GCP region
region_titles = {
    "asia-east1":               "Eastern Asia-Pacific (Taiwan)",
    "asia-east2":               "Eastern Asia-Pacific (Hong Kong)",
    "asia-northeast1":          "Northeastern Asia-Pacific (Tokyo)",
    "asia-northeast2":          "Northeastern Asia-Pacific (Osaka)",
    "asia-northeast3":          "Northeastern Asia-Pacific (Seoul)",
    "asia-south1":              "Southern Asia-Pacific (Mumbai)",
    "asia-southeast1":          "Southeastern Asia-Pacific (Singapore)",
    "asia-southeast2":          "Southeastern Asia-Pacific (Jakarta)",
    "australia-southeast1":     "Australia (Sydney)",
    "europe-north1":            "Northern Europe (Hamina)",
    "europe-west1":             "Western Europe (St. Ghislain)",
    "europe-west2":             "Western Europe (London)",
    "europe-west3":             "Western Europe (Frankfurt)",
    "europe-west4":             "Western Europe (Eemshaven)",
    "europe-west6":             "Western Europe (Zurich)",
    "northamerica-northeast1":  "North America (Montreal)",
    "southamerica-east1":       "South America (Sao Paulo)",
    "us-central1":              "Central US (Iowa)",
    "us-east1":                 "Eastern US (S. Carolina)",
    "us-east4":                 "Eastern US (N. Virginia)",
    "us-west1":                 "Western US (Oregon)",
    "us-west2":                 "Western US (Los Angeles)",
    "us-west3":                 "Western US (Salt Lake City)",
    "us-west4":                 "Western US (Las Vegas)",
}

# city where each GCP datacenter is located, approximately
region_cities = {
    "asia-east1":               "Changhua County",
    "asia-east2":               "Hong Kong",
    "asia-northeast1":          "Tokyo",
    "asia-northeast2":          "Osaka",
    "asia-northeast3":          "Seoul",
    "asia-south1":              "Mumbai",
    "asia-southeast1":          "Singapore",
    "asia-southeast2":          "Jakarta",
    "australia-southeast1":     "Sydney",
    "europe-north1":            "Hamina",
    "europe-west1":             "St. Ghislain",
    "europe-west2":             "London",
    "europe-west3":             "Frankfurt",
    "europe-west4":             "Eemshaven",
    "europe-west6":             "Zurich",
    "northamerica-northeast1":  "Montreal",
    "southamerica-east1":       "Sao Paulo",
    "us-central1":              "Council Bluffs",
    "us-east1":                 "Berkeley County",
    "us-east4":                 "N. Virginia",
    "us-west1":                 "The Dalles",
    "us-west2":                 "Los Angeles",
    "us-west3":                 "Salt Lake City",
    "us-west4":                 "Las Vegas",
}

# (latitude, longitude) where each GCP datacenter is located, approximately
region_coords = {
    "asia-east1":               (24.051796, 120.516135),
    "asia-east2":               (22.29, 114.27),
    "asia-northeast1":          (35.689488, 139.691706),
    "asia-northeast2":          (34.67, 135.44),
    "asia-northeast3":          (37.56, 126.96),
    "asia-south1":              (9.03, 72.84),
    "asia-southeast1":          (1.351231, 103.7073706),
    "asia-southeast2":          (-6.23, 106.79),
    "australia-southeast1":     (-33.77, 150.97),
    "europe-north1":            (60.53923, 27.1112792),
    "europe-west1":             (50.449109, 3.818376),
    "europe-west2":             (51.57, -0.24),
    "europe-west3":             (50.14, 8.58),
    "europe-west4":             (53.4257262, 6.8631489),
    "europe-west6":             (47.40, 8.40),
    "northamerica-northeast1":  (45.47, -73.77),
    "southamerica-east1":       (-23.57, -46.76),
    "us-central1":              (41.261944, -95.860833),
    "us-east1":                 (33.126062, -80.008775),
    "us-east4":                 (39.0115232, -77.4776423),
    "us-west1":                 (45.594565, -121.178682),
    "us-west2":                 (33.9181564, -118.4065411),
    "us-west3":                 (40.75, -112.17),
    "us-west4":                 (36.0639023, -115.2266872),
}

metadata_flavor = {'Metadata-Flavor' : 'Google'}

def get_my_internal_hostname():
    import requests
    r = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/name', headers = metadata_flavor)
    r.raise_for_status()
    return r.text

def get_my_external_ip():
    import requests
    r = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip', headers = metadata_flavor)
    r.raise_for_status()
    return r.text

def get_my_zone():
    import requests
    r = requests.get('http://metadata.google.internal/computeMetadata/v1/instance/zone', headers = metadata_flavor)
    r.raise_for_status()
    return r.text.split('/')[-1]

# test code
if __name__ == "__main__":
    print(("There are %d Google Cloud Platform regions." % (len(regions))))
    print(("%-26s %-40s %-20s %s, %s" % ("zone", "title", "city", "lat", "lon")))
    for r in regions:
        (lat, lon) = region_coords[r]
        print(("%-26s %-40s %-20s %0.2f, %0.2f" % (r, region_titles[r], region_cities[r], lat, lon)))

