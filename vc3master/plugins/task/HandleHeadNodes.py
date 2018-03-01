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
import time

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

        self.node_prefix           = self.config.get(section, 'node_prefix')

        self.node_image            = self.config.get(section, 'node_image')
        self.node_flavor           = self.config.get(section, 'node_flavor')
        self.node_user             = self.config.get(section, 'node_user')
        self.node_network_id       = self.config.get(section, 'node_network_id')
        self.node_private_key_file = os.path.expanduser(self.config.get(section, 'node_private_key_file'))
        self.node_public_key_name  = self.config.get(section, 'node_public_key_name')

        self.node_max_no_contact_time    = int(self.config.get(section, 'node_max_no_contact_time'))
        self.node_max_initializing_count = int(self.config.get(section, 'node_max_initializing_count'))

        self.ansible_path       = os.path.expanduser(self.config.get(section, 'ansible_path'))
        self.ansible_playbook   = self.config.get(section, 'ansible_playbook')

        self.ansible_debug_file = os.path.expanduser(self.config.get(section, 'ansible_debug_file')) # temporary for debug, only works for one node at a time
        self.ansible_debug      = open(self.ansible_debug_file, 'a')

        groups = self.config.get(section, 'node_security_groups')
        self.node_security_groups = groups.split(',')

        self.initializers = {}

        # keep las succesful contact to node, to check against node_max_no_contact_time.
        self.last_contact_times = {}

        # number of times we have tries to initialize a node. After node_max_initializing_count, declare failure.
        self.initializing_count = {}

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
        self.log.debug("Processing headnode for '%s'", request.name)

        headnode    = None
        next_state  = None
        reason      = None

        if not request.headnode:
            # Request has not yet indicated the name it wants for the headnode,
            # so we simply return.
            return

        try:
            headnode = self.client.getNodeset(request.headnode)
        except InfoEntityMissingException:
            pass
        except InfoConnectionFailure:
            return

        try:
            if headnode is None:
                if request.state == 'initializing':
                    headnode = self.create_headnode_nodeset(request)
                elif request.state == 'cleanup' or request.state == 'terminated':
                    # Nothing to do, the headnode has been cleaned-up
                    return
                else:
                    # Something went wrong, the headnode should still be there.
                    self.log.error("Could not find headnode information for %s", request.name)
                    return

            next_state, reason = headnode.state, headnode.state_reason

            if request.state == 'cleanup' or request.state == 'terminated':
                (next_state, reason) = self.state_terminating(request, headnode)

            if next_state == 'new':
                (next_state, reason) = self.state_new(request, headnode)

            if next_state == 'booting': 
                (next_state, reason) = self.state_booting(request, headnode)

            if next_state == 'initializing':
                (next_state, reason) = self.state_initializing(request, headnode)

            if next_state == 'running':
                (next_state, reason) = self.state_running(request, headnode)

            if (next_state != 'terminated') and (next_state != 'failure'):
                (next_state, reason) = self.check_timeout(request, next_state, reason)

        except Exception, e:
            self.log.debug("Error while processing headnode: %s", e)
            self.log.warning(traceback.format_exc(None))

            if headnode:
                (next_state, reason) = ('failure', 'Internal error: %s' % e)
            else:
                raise

        headnode.state        = next_state
        headnode.state_reason = reason

        try:
            if headnode.state == 'terminated':
                self.delete_headnode_nodeset(request)
            else:
                self.client.storeNodeset(headnode)
        except Exception, e:
            self.log.warning("Storing the new headnode state failed. (%s)", e)
            self.log.warning(traceback.format_exc(None))

    def state_terminating(self, request, headnode):
        try:
            if headnode.state != 'terminated':
                if self.initializers.get(request.name, None):
                    try:
                        proc = self.initializers[request.name]
                        proc.terminate()
                    except Exception, e:
                        self.log.warning('Exception while killing initializer for %s: %s', request.name, e)

                server = self.nova.servers.find(name=self.vm_name(request))
                self.log.debug('Teminating headnode %s for request %s', request.headnode, request.name)
                server.delete()

                self.initializers.pop(request.name, None)
                self.last_contact_times.pop(request.name, None)
                self.initializing_count.pop(request.name, None)
        except Exception, e:
            self.log.warning('Could not find headnode instance for request %s (%s)', request.name, e)
        finally:
            return ('terminated', 'Headnode succesfully terminated')

    def state_new(self, request, headnode):
        self.log.info('Creating new nodeset %s for request %s', request.headnode, request.name)

        server = self.boot_server(request, headnode)

        if not server:
            self.log.warning('Could not boot headnode for request %s', request.name)
            return ('failure', 'Could not boot headnode.', request.name)
        else:
            self.log.debug('Waiting for headnode for request %s to come online', request.name)
            return ('booting', 'Headnode is booting up.')

    def state_booting(self, request, headnode):
        if not headnode.app_host:
            headnode.app_host = self.__get_ip(request)
            self.last_contact_times[request.name] = time.time()

        if self.check_if_online(request, headnode):
            return ('initializing', 'Headnode is being configured.')
        else: 
            self.log.debug('Headnode for %s could not yet be used for login.', request.name)
            return ('booting', 'Headnode is booting up.')


    def state_initializing(self, request, headnode):
        self.initialize_server(request, headnode)

        (next_state, state_reason) = self.check_if_done_init(request, headnode)

        if self.check_if_online(request, headnode):
            self.last_contact_times[request.name] = time.time()

        if next_state == 'running':
            self.log.info('Done initializing server %s for request %s', request.headnode, request.name)
            self.report_running_server(request, headnode)
        elif next_state != 'failure':
            self.log.debug('Waiting for headnode for %s to finish initialization.', request.name)
        return (next_state, state_reason)


    def state_running(self, request, headnode):
        if self.check_if_online(request, headnode):
            self.last_contact_times[request.name] = time.time()

        return ('running', 'Headnode is ready to be used.')


    def check_if_online(self, request, headnode):
        if headnode.app_host is None:
            self.log.debug('Headnode for %s does not have an address yet.', request.name)
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
                headnode.app_host,
                '--',
                '/bin/date'])

            self.log.info('Headnode for %s running at %s', request.name, headnode.app_host)

            return True
        except subprocess.CalledProcessError:
            self.log.debug('Headnode for %s running at %s could not be accessed.', request.name, headnode.app_host)
            return False

    def check_timeout(self, request, next_state, reason):
        now = time.time()

        diff = 0

        if self.last_contact_times.has_key(request.name):
            diff = now - self.last_contact_times[request.name]
        else:
            return (next_state, reason)

        if diff > self.node_max_no_contact_time:
            self.log.warning('Headnode for %s could not be contacted after %d seconds. Declaring failure.', request.name, self.node_max_no_contact_time)
            return ('failure', 'Headnode could no be contacted after %d seconds.' % self.node_max_no_contact_time)
        elif diff > self.node_max_no_contact_time/2:
            self.log.warning('Headnode for %s could not be contacted! (waiting for %d seconds before declaring failure)', request.name, self.node_max_no_contact_time - diff)
            reason = reason + " (Headnode could not be contacted. This may be a transient error. Waiting for {:.0f} seconds before declaring failure.)".format(self.node_max_no_contact_time - diff)
            return (next_state, reason)
        else:
            return (next_state, reason)

    def boot_server(self, request, headnode):
        try:
            server = self.nova.servers.find(name = self.vm_name(request))
            self.log.info('Found headnode at %s for request %s', request.headnode, request.name)
            return server
        except Exception, e:
            pass

        self.log.info('Booting new headnode for request %s...', request.name)
        server = self.nova.servers.create(name = self.vm_name(request), image = self.node_image, flavor = self.node_flavor, key_name = self.node_public_key_name, security_groups = self.node_security_groups, nics = [{'net-id' : self.node_network_id}])

        return server


    def initialize_server(self, request, headnode):

        # if we already initialized this headnode
        if self.initializers.has_key(request.name):
            return

        self.initializing_count[request.name] = self.initializing_count.get(request.name, 0) + 1

        self.log.info("Trying to initialize headnode for request %s for the %d/%d time." % (request.name, self.initializing_count[request.name], self.node_max_initializing_count))

        os.environ['ANSIBLE_HOST_KEY_CHECKING']='False'

        extra_vars  = {}
        extra_vars['request_name']         = request.name
        extra_vars['setup_user_name']      = self.node_user
        extra_vars['condor_password_file'] = self.condor_password_filename(request)
        extra_vars['production_keys']      = self.get_members_keys(request)
        extra_vars['builder_options']      = self.get_builder_options(request)

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
                    headnode.app_host + ',',
                    ],
                cwd = self.ansible_path,
                stdout=self.ansible_debug,
                stderr=self.ansible_debug,
                )
        self.initializers[request.name] = pipe
        self.last_contact_times[request.name] = time.time()

    def check_if_done_init(self, request, headnode):
        try:
            pipe = self.initializers[request.name]
            pipe.poll()

            self.ansible_debug.flush()

            if pipe.returncode is None:
                return ('initializing', 'Headnode is being configured.')

            # the process is done when there is a returncode
            self.initializers.pop(request.name, None)

            if pipe.returncode != 0:
                self.log.warning('Error when initializing headnode for request %s. Exit status: %d', request.name, pipe.returncode)
                
                if self.initializing_count[request.name] >= self.node_max_initializing_count:
                    self.log.warning("Could not initialize headnode after %d tries." % (self.node_max_initializing_count,))
                    return ('failure', 'Headnode could not be configured.')
                else:
                    # Make another go at initializing...
                    return ('initializing', 'Headnode is being configured.')

            return ('running', 'Headnode is ready to be used.')

        except Exception, e:
            self.log.warning('Error for headnode initializers for request %s (%s)', request.name, e)
            return ('failure', 'Headnode could not be configured.')

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

    def get_builder_options(self, request):
        packages = []
        for env_name in request.environments:
            env = self.client.getEnvironment(env_name)
            if env.packagelist:
                packages.extend(env.packagelist)
        return " ".join([ "--require %s" % p for p in packages ])

    def create_headnode_nodeset(self, request):
        self.log.debug("Creating new headnode spec '%s'", request.headnode)

        headnode = self.client.defineNodeset(
                name = request.headnode,
                owner = request.owner,
                node_number = 1,
                app_type = 'htcondor',     # should depend on the given nodeset!
                app_role = 'head-node', 
                environment = None,
                description = 'Headnode nodeset automatically created: ' + request.headnode,
                displayname = request.headnode)

        self.last_contact_times[request.name] = time.time()

        return headnode

    def delete_headnode_nodeset(self, request):
        if request.headnode:
            try:
                headnode = self.client.getNodeset(request.headnode)
                self.log.debug("Deleting headnode spec '%s'", request.headnode)
                self.client.deleteNodeset(request.headnode)
            except Exception, e:
                self.log.debug("Could not delete headnode nodeset '%s'." % (request.headnode,))
                self.log.debug(traceback.format_exc(None))

    def __get_ip(self, request):
        try:
            server = self.nova.servers.find(name=self.vm_name(request))

            if server.status != 'ACTIVE':
                self.log.debug("Headnode for request %s is not active yet.", request.name)
                return None

        except Exception, e:
            self.log.warning('Could not find headnode for request %s (%s)', request.name, e)
            return None

        try:
            for network in server.networks.keys():
                for ip in server.networks[network]:
                    if re.match('\d+\.\d+\.\d+\.\d+', ip):
                        return ip
        except Exception, e:
            self.log.warning("Could not find ip for request %s: %s", request.name, e)
            raise e

        return None

    def vm_name(self, request):
        return self.node_prefix + request.name

