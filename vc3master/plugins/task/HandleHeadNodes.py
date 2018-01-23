#!/usr/bin/env python


from vc3master.task import VC3Task
from vc3infoservice.infoclient import InfoConnectionFailure, InfoEntityMissingException

from base64 import b64encode
import pluginmanager as pm
import traceback

import json
import os
import re
import subprocess

from novaclient import client as novaclient

class HandleHeadNodes(VC3Task):
    '''
    Plugin to manage the head nodes lifetime.
     
    '''

    def __init__(self, parent, config, section):
        super(HandleHeadNodes, self).__init__(parent, config, section)
        self.client = parent.client
        self.config = config

        nova_conf = {
                'version' : '2.0',
                'username' : self.config.get(section, 'username'),
                'password' : self.config.get(section, 'password'),
                'user_domain_name' : self.config.get(section, 'user_domain_name'),
                'project_domain_name' : self.config.get(section, 'project_domain_name'),
                'auth_url' : self.config.get(section, 'auth_url'),
                }

        self.nova = novaclient.Client( **nova_conf );

        self.node_image            = self.config.get(section, 'node_image')
        self.node_flavor           = self.config.get(section, 'node_flavor')
        self.node_user             = self.config.get(section, 'node_user')
        self.node_network_id       = self.config.get(section, 'node_network_id')
        self.node_private_key_file = os.path.expanduser(self.config.get(section, 'node_private_key_file'))
        self.node_public_key_name  = self.config.get(section, 'node_public_key_name')

        self.ansible_path       = os.path.expanduser(self.config.get(section, 'ansible_path'))
        self.ansible_playbook   = self.config.get(section, 'ansible_playbook')

        self.ansible_debug_file = os.path.expanduser(self.config.get(section, 'ansible_debug_file')) # temporary for debug, only works for one node at a time
        self.ansible_debug      = open(self.ansible_debug_file, 'a')

        groups = self.config.get(section, 'node_security_groups')
        self.node_security_groups = groups.split(',')

        self.initializers = {}

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
                        raise e
        except InfoConnectionFailure, e:
            self.log.warning("Could not read requests from infoservice. (%s)", e)

    def process_request(self, request):
        next_state  = None
        reason      = None

        self.log.debug("Processing headnode for '%s'", request.name)

        if request.headnode:
            try:
                headnode = self.client.getNodeset(request.headnode)

                if headnode.state == 'terminated':
                    return

                if request.state == 'cleanup' or request.state == 'terminated':
                    self.terminate_server(request, headnode)

                if headnode.state == 'new':
                    self.create_server(request, headnode)

                if headnode.state == 'booting' and self.check_if_online(request, headnode):
                    self.initialize_server(request, headnode)

                if headnode.state == 'initializing' and self.check_if_done_init(request, headnode):
                    self.report_running_server(request, headnode)

            except InfoEntityMissingException:
                self.log.error("Could not find headnode information for %s", request.name)
                return
        else:
            # Request has not yet created headnode nodeset spec, so we simply return.
            return

        try:
            self.client.storeNodeset(headnode)
        except Exception, e:
            self.log.warning("Storing the new request state failed. (%s)", e)
            self.log.warning(traceback.format_exc(None))


    def terminate_server(self, request, headnode):
        try:
            if headnode.state == 'terminated':
                return

            if self.initializers[request.name]:
                try:
                    proc = self.initializers[request.name]
                    self.initializers[request.name] = None
                    proc.terminate()
                except Exception, e:
                    self.log.warning('Exception while killing initializer for %s: %s', request.name, e)

            server = self.nova.servers.find(name=request.name)
            self.log.debug('Teminating headnode %s for request %s', request.headnode, request.name)
            server.delete()

        except Exception, e:
            self.log.warning('Could not find headnode instance for request %s (%s)', request.name, e)

        headnode.state = 'terminated'


    def create_server(self, request, headnode):
        server = self.boot_server(request, headnode)

        if not server:
            headnode.state = 'failure'
            self.log.warning('Could not boot headnode for request %s', request.name)
        else:
            headnode.state = 'booting'
            self.log.debug('Waiting for headnode for request %s to come online', request.name)

    def check_if_online(self, request, headnode):
        ip = self.__get_ip(request)

        if ip is None:
            return False

        try:
            subprocess.check_call([
                'ssh',
                '-o',
                'UserKnownHostsFile=/dev/null',
                '-o',
                'StrictHostKeyChecking=no',
                '-o',
                'ConnectTimeout=10',
                '-i',
                self.node_private_key_file,
                '-l',
                self.node_user,
                ip,
                '--',
                '/bin/date'])

            self.log.info('Headnode for %s running at %s', request.name, ip)

            return True
        except subprocess.CalledProcessError:
            return False

    def boot_server(self, request, headnode):
        try:
            server = self.nova.servers.find(name=request.name)
            self.log.info('Found headnode at %s for request %s', request.headnode, request.name)
            return server
        except Exception, e:
            pass

        self.log.info('Booting new headnode for request %s...', request.name)
        server = self.nova.servers.create(name = request.name, image = self.node_image, flavor = self.node_flavor, key_name = self.node_public_key_name, security_groups = self.node_security_groups, nics = [{'net-id' : self.node_network_id}])

        return server


    def initialize_server(self, request, headnode):

        # if we are already initializing this headnode
        if self.initializers.has_key(request.name):
            return

        self.log.info('Initializing new server at %s for request %s', request.headnode, request.name)

        headnode.state = 'initializing'
        headnode.url   = self.__get_ip(request) # HACK! using .url instead of a more appripiate field.

        os.environ['ANSIBLE_HOST_KEY_CHECKING']='False'

        extra_vars  = {}
        extra_vars['request_name']         = request.name
        extra_vars['setup_user_name']      = self.node_user
        extra_vars['condor_password_file'] = self.condor_password_filename(request)
        extra_vars['production_keys']      = self.get_members_keys(request)

        # passing extra-vars as a command line argument for now. That won't
        # scale well, we want to write those vars to a file instead.
        pipe = subprocess.Popen(
                ['ansible-playbook',
                    self.ansible_playbook,
                    '--extra-vars',
                    json.dumps(extra_vars),
                    '--key-file',
                    self.node_private_key_file,
                    '--inventory',
                    headnode.url + ',',
                    ],
                cwd = self.ansible_path,
                stdout=self.ansible_debug,
                stderr=self.ansible_debug,
                )
        self.initializers[request.name] = pipe

    def check_if_done_init(self, request, headnode):
        try:
            pipe = self.initializers[request.name]
            pipe.poll()

            self.ansible_debug.flush()

            if pipe.returncode is None:
                return False

            # the process is done when there is a returncode
            self.initializers[request.name] = None

            if pipe.returncode != 0:
                self.log.warning('Error when initializing headnode for request %s. Exit status: %d', request.name, pipe.returncode)
                headnode.state = 'failure'
            return True

        except Exception, e:
            self.log.warning('Error for headnode initializers for request %s (%s)', request.name, e)
            headnode.state = 'failure'

    def report_running_server(self, request, headnode):
        try:
            headnode.app_sectoken = self.read_encoded(self.condor_password_filename(request))
            headnode.state = 'running'
        except Exception, e:
            self.log.warning('Cound not read condor password file for request %s (%s)', request.name, e)
            self.log.debug(traceback.format_exc(None))
            headnode.state = 'failure'

    def condor_password_filename(self, request):
        # file created by ansible
        return '/tmp/condor_password.' + request.name

    def read_encoded(self, filename):
        with open(filename, 'r') as f:
            contents = f.read()
            return self.client.encode(contents)

    def get_members_names(self, request):
        members = None

        if request.project:
            project = self.client.getProject(request.project)
            if project:
                members = project.members

        if not members:
            members = []
            self.log.warning('Could not find user names for request %s.')

        return members

    def get_members_keys(self, request):
        members    = self.get_members_names(request)

        keys = {}
        for member in members:
            user = self.client.getUser(member)

            if not user or not user.sshpubstring:
                self.log.warning('Could not find ssh key for user %s')
            else:
                keys[member] = user.sshpubstring
        return keys

    def __get_ip(self, request):
        try:
            server = self.nova.servers.find(name=request.name)

            if server.status != 'ACTIVE':
                return None

        except Exception, e:
            self.log.warning('Could not find headnode for request %s (%s)', request.name, e)
            raise e

        try:
            for network in server.networks.keys():
                for ip in server.networks[network]:
                    if re.match('\d+\.\d+\.\d+\.\d+', ip):
                        return ip
        except Exception, e:
            self.log.warning("Could not find ip for request %s: %s", request.name, e)
            raise e

        return None

