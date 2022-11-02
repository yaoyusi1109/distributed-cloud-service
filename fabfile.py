# This is a basic fabric configuration file for deploying and running code on EC2.
#
# Author: kwalsh@cs.holycross.edu
# Date: January 25, 2015.
#
# To deploy code to all EC2 hosts, run this command on radius:
#   fab -P deploy
# To run the code on all EC2 hosts, run this command on radius:
#    fab -P start
# To do both, run this command on radius:
#    fab -P deploy start
#
# You can edit this file however you like, but update the instructions above if
# you do so that it is clear how to deploy and start your cloud storage service.

from fabric.api import hosts, run, env
from fabric.operations import put

# This is the cloud_sshkey needed to log in to other servers. If running fabric
# from your laptop, your key is probably named ~/.ssh/cloud_sshkey. But if
# you are running fabric from within the cloud, it is probably named
# ~/.ssh/id_rsa instead.
env.key_filename = '~/.ssh/cloud_sshkey'
# env.key_filename = '~/.ssh/id_rsa'

# This is the list of EC2 hosts. Add or remove from this list as you like.
env.hosts = [
        '34.94.207.48', # gcp
        '35.199.57.169', # gcp
        '54.242.85.178', #aws
        '52.87.83.20', #aws
        ]
# This is the host designated as the central coordator. Pick whichever server
# from all_hosts you like here. By default, just take the first one.
central_host = env.hosts[0]

# The deploy task copies all python files from local directory to every host.
# If you want to copy other files, you can modify this, or make a separate task
# for deploying the other files to specific hosts.
def deploy():
    put('*.py', '~/')

# The start task runs the cloud-drive.py command on every host.
def start():
    run('python3 cloud-drive.py ' + central_host + ' 8088')

