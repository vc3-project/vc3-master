#!/usr/bin/env python 

from vc3master.lib.clusters import ClustersMgr, Cluster
from vc3master.lib.sites import SitesMgr, Site
from vc3master.lib.applications import ApplicationsMgr, Application

import argparse

parser = argparse.ArgumentParser()

subparsers = parser.add_subparsers(help='commands')

# A create-cluster command
create_parser = subparsers.add_parser('create-cluster', help='Create a cluster')
create_parser.add_argument('-c', default=False, action='store', dest='cluster_specs', help='file with cluster specifications',)
create_parser.add_argument('-r', default=False, action='store', dest='resource_specs', help='file with resources specifications',)
create_parser.add_argument('-u', default=False, action='store', dest='user_specs', help='file with users specifications',)
create_parser.add_argument('-p', default=False, action='store', dest='policy', help='file with policies',)

# A prepare-site command
create_parser = subparsers.add_parser('prepare-site', help='Install a new site')

# A launch-app-service command
create_parser = subparsers.add_parser('launch-app-service', help='Launches a enw application service')



opts = parser.parse_args()
print 
print opts.cluster_specs
print opts.resource_specs
print opts.policy
