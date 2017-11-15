#!/usr/bin/env python

import ConfigParser
import StringIO
from base64 import b64encode

from vc3master.task import VC3Task
from vc3infoservice.infoclient import InfoConnectionFailure

import pluginmanager as pm
import traceback

import re
from novaclient import client as novaclient

class HandleHeadNodes(VC3Task):
    '''
    Plugin to manage the head nodes lifetime.
     
    '''

    def __init__(self, parent, config, section):
        super(HandleHeadNodes, self).__init__(parent, config, section)
        self.client = parent.client

        self.openstack_username = 'secret'
        self.openstack_password = 'secret'
        self.openstack_user_domain_name    = 'default'
        self.openstack_project_domain_name = 'default'
        self.openstack_auth_url = 'http://10.32.70.9:5000/v3'

        self.nova = novaclient.Client(
                version             = '2.0',
                username            = self.openstack_username,
                password            = self.openstack_password,
                user_domain_name    = self.openstack_user_domain_name,
                project_domain_name = self.openstack_project_domain_name,
                auth_url            = self.openstack_auth_url)

        self.node_image           = '26b176c9-7219-4295-8bb0-a87ef200dca5'
        self.node_flavor          = '15d4a4c3-3b97-409a-91b2-4bc1226382d3'
        self.node_master_key_name = 'lancre-b'
        self.node_security_groups = ['ssh', 'default']
        self.node_network_name    = u'Campus - Private 1'

        self.log.debug("HandleHeadNodes VC3Task initialized.")

    def runtask(self):
        self.log.info("Running task %s" % self.section)
        self.log.debug("Polling master....")

        try:
            requests = self.client.listRequests()
            n = len(requests) if requests else 0
            self.log.debug("Processing %d requests" % n)
            if requests:
                for r in requests:
                    try:
                        self.process_request(r)
                    except Exception, e:
                        self.log.warning("Request %s had an exception (%s)", r.name, e)
                        self.log.debug(traceback.format_exc(None))
        except InfoConnectionFailure, e:
            self.log.warning("Could not read requests from infoservice. (%s)", e)

    def process_request(self, request):
        next_state  = None
        reason      = None

        self.log.debug("Processing headnode for '%s'", request.name)

        if request.state == 'cleanup':
            self.terminate_server(request)
        elif request.state == 'validated':
            # other than cleanup, headnodes are only checked when the request has been validated.
            # here we want to check the state of something like a headnode entity
            if request.headnode is not None:
                return
            else:
                self.create_server(request)
        else:
            return


        try:
            self.client.storeRequest(request)
        except Exception, e:
            self.log.warning("Storing the new request state failed. (%s)", e)
            self.log.warning(traceback.format_exc(None))


    def terminate_server(self, request):
        try:
            server = self.nova.servers.find(name=request.name)
            self.log.debug('Teminating headnode at %s for request %s', request.headnode, request.name)
            server.delete()
        except Exception, e:
            self.log.warning('Could not find headnode for request %s (%s)', request.name, e)

        # here we want to update the state of headnode entity or alike
        request.headnode = None

    def create_server(self, request):

        server = self.boot_server(request)
        if not server:
            raise('Could not boot headnode for request %s', request.name)

        if server.status != 'ACTIVE':
            self.log.debug('Waiting for headnode for request %s to come online', request.name)
            return

        request.headnode = self.__get_ip(request, server)
        if not request.headnode:
            self.terminate_server(request)
            raise('Could not get ip for headnode for request %s', request.name)

        self.log.info('Headnode for %s running at %s', request.name, request.headnode)

        self.initialize_server(request)


    def boot_server(self, request):
        try:
            server = self.nova.servers.find(name=request.name)
            self.log.info('Found headnode at %s for request %s', request.headnode, request.name)
            return server
        except Exception, e:
            pass

        self.log.info('Booting new headnode at %s for request %s', request.headnode, request.name)
        server = self.nova.servers.create(name = request.name, image = self.node_image, flavor = self.node_flavor, key_name = self.node_master_key_name, security_groups = self.node_security_groups)
        return server


    def initialize_server(self, request):
        self.log.info('Initializing new server at %s for request %s', request.headnode, request.name)
        pass

    def __get_ip(self, request, server):
        try:
            self.log.warning(server.networks.keys())
            for ip in server.networks[self.node_network_name]:
                if re.match('\d+\.\d+\.\d+\.\d+', ip):
                    return ip
        except Exception, e:
            self.log.warning("Could not find ip in network '%s' for request %s: %s", self.node_network_name, request.name, e)
            raise e

        return None

