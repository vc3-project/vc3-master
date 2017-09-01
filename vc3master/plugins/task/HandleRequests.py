#!/usr/bin/env python

import ConfigParser
import StringIO
from base64 import b64encode

import os
import json

from vc3master.task import VC3Task

import pluginmanager as pm

class HandleRequests(VC3Task):
    '''
    Plugin to manage the life cycle of all requests.
     
    '''

    def __init__(self, parent, config, section):
        super(HandleRequests, self).__init__(parent, config, section)
        self.client = parent.client
        self.log.debug("HandleRequests VC3Task initialized.")

    def runtask(self):
        self.log.info("Running task %s" % self.section)
        self.log.debug("Polling master....")
        requests = self.client.listRequests()
        n = len(requests) if requests else 0
        self.log.debug("Processing %d requests" % n)
        if requests:
            for r in requests:
                self.process_request(r)

    def process_request(self, request):
        next_state  = None
        reason      = None

        self.log.debug("Processing request '%s'", request.name)

        if  request.state == 'new': 
            # nexts: validated, terminating
            (next_state, reason) = self.state_new(request)

        if request.state == 'validated':
            # nexts: validated, pending, terminating
            (next_state, reason) = self.state_validated(request)

        if request.state == 'pending':
            # nexts: pending, growing, running, terminating
            # to growing until at least one element of the request is fulfilled
            (next_state, reason) = self.state_pending(request)

        if request.state == 'growing':
            # nexts: growing, shrinking, running, terminating
            # to running until all elements of the request are fulfilled
            (next_state, reason) = self.state_growing(request)

        if request.state == 'running':
            # nexts: shrinking, running, terminating
            # waits for action = terminate
            (next_state, reason) = self.state_running(request)

        if request.state == 'shrinking':
            # nexts: shrinking, terminating
            # to terminating 
            (next_state, reason) = self.state_shrinking(request)

        if request.state == 'terminating':
            # waits until everything has been cleanup
            (next_state, reason) = self.state_terminating(request)

        if request.state == 'terminated':
            self.log.debug('request %s is done' % request.name)
            (next_state, reason) = (request.state, None)

        if request.action and request.action == 'terminate':
            if next_state in ['new', 'validated', 'pending']:
                (next_state, reason) = ('terminating', 'received terminate action')
            elif next_state in ['growing', 'running']:
                (next_state, reason) = ('shrinking', 'received terminate action')

        if reason:
            request.state_reason = reason

        if next_state is not request.state:
            try:
                self.log.debug("request '%s'  state '%s' -> %s'", request.name, request.state, next_state)
                request.state = next_state

                self.add_queues_conf(request) 
                self.add_auth_conf(request)

                self.client.storeRequest(request)

            except Exception, e:
                self.log.warning("Storing the new request state failed.")
                raise e

    def state_by_cluster(self, request):

        if request.cluster_state == 'new':
            if request.statusinfo:
                request.cluster_state = 'configured'
            else:
                return ('validated', 'Waiting for factory to configure itself.')

        if not request.statusinfo:
            return ('terminating', 'Status of request went away.')

        running = 0
        for pool in request.statusinfo:
            try:
                running += request.statusinfo[pool]['running']
            except KeyError:
                self.log.warn("Pool '" + pool + "' did not define a running field.")

        self.log.debug("Request '" + request.name + "' has " + str(running) + " jobs running")

        if request.cluster_state == 'configured':
            if running > 0:
                request.cluster_state = 'running'
            else:
                return ('pending', 'Waiting for factory to start filling the request.')

        total_of_nodes = self.total_jobs_requested(request)

        if request.cluster_state == 'running':
            if total_of_nodes == 0 and running == 0:
                request.cluster_state = 'terminating'
            if total_of_nodes > running:
                return ('growing', 'Factory is fulfilling the request.')
            elif total_of_nodes < running:
                return ('shrinking', 'Factory is draining the request.')
            else:
                return ('running', 'Factory completely fulfilled the request')

        if request.cluster_state == 'terminating':
            return ('terminating', 'cluster in terminating state')

        if request.cluster_state == 'failure':
            return ('terminating', 'Failure: cluster reported a failure: %s.' % 'because of reasons')

        return ('terminating', 'Failure: cluster reached an unkown state: %s.' % 'because of reasons')

    def request_is_valid(self, request):
        return True

    def state_new(self, request):
        '''
        Validates all new requests. 
        '''
        self.log.debug('processing new request %s' % request.name)

        if self.request_is_valid(request):
            try:
                if self.add_queues_conf(request) and self.add_auth_conf(request):
                    return ('validated', None)
            except VC3InvalidRequest, e:
                #raise e
                self.log.warning("Invalid Request: %s" % str(e))
                return ('terminated', 'Invalid request: %s' % e.reason)

        return ('terminated', 'Failure: invalid request')

    def state_validated(self, request):
        self.log.debug('waiting for factory to configure %s' % request.name)
        return self.state_by_cluster(request)

    def state_pending(self, request):
        self.log.debug('waiting for factory to start fullfilling request %s' % request.name)
        return self.state_by_cluster(request)

    def state_growing(self, request):
        self.log.debug('waiting for factory to fullfill request %s' % request.name)

        action = request.action
        if action == 'terminate':
            return ('shrinking', 'Explicit termination requested.')

        # what follows is for action = 'run'
        return self.state_by_cluster(request)

    def state_running(self, request):
        self.log.debug('request %s is running' % request.name)

        action = request.action
        if action == 'terminate':
            self.log.debug("termination requested. Generating queues and auth...")
            return ('shrinking', 'Explicit termination requested.')
        return self.state_by_cluster(request)

    def state_shrinking(self, request):
        self.log.debug('request %s is shrinking' % request.name)

        action = request.action
        if action and action is not 'terminate':
            # ignoring action... do something here?
            pass

        return self.state_by_cluster(request)

    def state_terminating(self, request):
        self.log.debug('request %s is terminating' % request.name)

        action = request.action
        if action and action is not 'terminate':
            # ignoring action... do something here?
            pass

        if self.is_everything_cleaned_up(request):
            return ('terminated', None)

        return self.state_by_cluster(request)


    def add_queues_conf(self, request):
        config = ConfigParser.RawConfigParser()

        for a in request.allocations:
            self.generate_queues_section(config, request, a)

        conf_as_string = StringIO.StringIO()
        config.write(conf_as_string)

        request.queuesconf = b64encode(conf_as_string.getvalue())
        return request.queuesconf

    def add_auth_conf(self, request):
        config = ConfigParser.RawConfigParser()

        for a in request.allocations:
            self.generate_auth_section(config, request, a)

        conf_as_string = StringIO.StringIO()
        config.write(conf_as_string)

        request.authconf = b64encode(conf_as_string.getvalue())
        return request.authconf

    def generate_queues_section(self, config, request, allocation_name):
        '''
            request.allocations = [ alloc1, alloc2 ]
                   .cluster.nodesets = [ nodeset1, nodeset2 ]                                       
             nodeset.node_number   # total number to launch. 
        '''
        allocation = self.client.getAllocation(allocation_name)
        if not allocation:
            raise VC3InvalidRequest("Allocation '%s' has not been declared." % allocation_name, request = request)

        resource = self.client.getResource(allocation.resource)
        if not resource:
            raise VC3InvalidRequest("Resource '%s' has not been declared." % allocation.resource, request = request)
        
        cluster = self.client.getCluster(request.cluster)
        if not cluster:
            raise VC3InvalidRequest("Cluster '%s' has not been declared." % cluster.name, request = request)

        if len(cluster.nodesets) < 1:
            raise VC3InvalidRequest("No nodesets have been added to Cluster '%s' " % cluster.name, request = request)

        for nodeset_name in cluster.nodesets:
            self.log.debug("retriving nodeset %s for cluster %s " % (nodeset_name, cluster.name))
            nodeset = self.client.getNodeset(nodeset_name)
            if not nodeset:
                raise VC3InvalidRequest("Nodeset '%s' has not been declared." % nodeset_name, request = request)
            self.log.debug("Retrieved %s for name %s" % ( nodeset, nodeset_name))
            self.add_nodeset_to_queuesconf(config, request, resource, allocation, cluster, nodeset)

    def add_nodeset_to_queuesconf(self, config, request, resource, allocation, cluster, nodeset):
        node_number  = self.jobs_to_run_by_policy(request, nodeset)
        section_name = request.name + '.' + nodeset.name + '.' + allocation.name

        self.log.debug("Information finalized for queues configuration section [%s]. Creating config." % section_name)

        config.add_section(section_name)
        config.set(section_name, 'sched.keepnrunning.keep_running', node_number)

        if resource.accesstype == 'batch':
            config.set(section_name, 'batchsubmitplugin',           'CondorSSH')
            config.set(section_name, 'batchsubmit.condorssh.user',  allocation.accountname)
            config.set(section_name, 'batchsubmit.condorssh.batch', resource.accessflavor)
            config.set(section_name, 'batchsubmit.condorssh.host',  resource.accesshost)
            config.set(section_name, 'batchsubmit.condorssh.port',  str(resource.accessport))
            config.set(section_name, 'batchsubmit.condorssh.authprofile', allocation.name)
            config.set(section_name, 'executable',          '%(builder)s')
            if nodeset.environment:
                self.add_environment_to_queuesconf(config, request, section_name, nodeset.environment)
        elif resource.accesstype == 'cloud':
            config.set(name, 'batchsubmitplugin',          'CondorEC2')
        elif resource.accesstype == 'local':
            config.set(name, 'batchsubmitplugin',          'CondorLocal')
            config.set(name, 'executable',          '%(builder)s')
            if nodeset.environment:
                self.add_environment_to_queuesconf(config, request, section_name, nodeset.environment)
        else:
            raise VC3InvalidRequest("Unknown resource access type '%s'" % str(resource.accesstype), request = request)

        self.log.debug("Completed filling in config for allocation %s" % allocation.name)


    def jobs_to_run_by_policy(self, request, nodeset):
        # For now no policies. Just calculated static-balanced 
        self.log.debug("Calculating nodes to run...")
        node_number = 0
        if request.action == 'terminate':
            node_number = 0
            self.log.debug("Action is terminate. Setting keepnrunning to 0")
        else:
            numalloc = len(request.allocations)
            total_to_run = int(nodeset.node_number)
            node_number = total_to_run / numalloc
            self.log.debug("With %d allocations and nodeset.node_number %d this allocation should run %d" % (numalloc, total_to_run, node_number))
        return node_number

    def generate_auth_tokens(self, principle):
        """ 
        Generate SSH priv/pub keys and base64 encode them
        """ 
        self.log.info("Generating or retrieving SSH keys for %s", principle)
        self.ssh = self.parent.parent.ssh
        (pub, priv) = self.ssh.getkeys(principle)
        self.log.debug("public key: %s", pub)
        encoded_pub = b64encode(pub)
        encoded_priv = b64encode(priv)
        return encoded_pub, encoded_priv
         

    def generate_auth_section(self, config, request, allocation_name):

        name = allocation_name
        config.add_section(name)

        allocation = self.client.getAllocation(allocation_name)
        if not allocation:
            raise VC3InvalidRequest("Allocation '%s' has not been declared." % allocation_name, request = request)

        if allocation.pubtoken is None:
            raise VC3InvalidRequest("Allocation '%s' doesn't have pub token." % allocation_name, request = request)

        if allocation.privtoken is None:
            raise VC3InvalidRequest("Allocation '%s' doesn't have pub token." % allocation_name, request = request)

        resource = self.client.getResource(allocation.resource)
        if not resource:
            raise VC3InvalidRequest("Resource '%s' has not been declared." % allocation.resource, request = request)

        if resource.accessmethod == 'ssh':
            # credible always generates rsa keys
            #allocation.sectype = 'ssh-rsa'
            #(allocation.pubtoken, allocation.privtoken) = self.generate_auth_tokens(name)
            
            config.set(name, 'plugin',        'SSH')
            config.set(name, 'ssh.type',  allocation.sectype)
            config.set(name, 'ssh.publickey', allocation.pubtoken)
            config.set(name, 'ssh.privatekey', allocation.privtoken)
        elif resource.accesstype == 'gsissh':
            raise NoImplementedError
        else:
            raise VC3InvalidRequest("Unknown resource access method '%s'" % str(resource.accessmethod), request = request)


    def add_environment_to_queuesconf(self, config, request, section_name, environment_name):
        if environment_name is None:
            return

        environment = self.client.getEnvironment(environment_name)
        if environment is None:
            raise VC3InvalidRequest("Unknown environment '%s' for '%s'" % (environment_name, section_name), request = request)

        config.set(section_name, 'vc3.environment', environment.name)

        if len(environment.packagelist) < 1:
            self.log.warning("Environment '%s' did not define a package" % (environment.name))
            return

        vs    = [ "VC3_REQUESTID='%s'" % request.name, "VC3_QUEUE='%s'" % section_name]
        for k in environment.envmap:
            vs.append("%s=%s" % (k, environment.envmap[k]))

        reqs  = ' '.join(['--require %s' % x for x in environment.packagelist])
        vars  = ' '.join(['--var %s' % x for x in vs])

        s  = vars + ' ' + reqs

        if environment.builder_extra_args:
            s += ' ' + ' '.join(environment.builder_extra_args)

        if environment.command:
            s += ' -- ' + environment.command

        config.set(section_name, 'executable.arguments', s)

    def is_everything_cleaned_up(self, request):
        ''' TO BE FILLED '''
        return True

    def total_jobs_requested(self, request):
        cluster = self.client.getCluster(request.cluster)

        if len(cluster.nodesets) < 1:
            raise VC3InvalidRequest("No nodesets have been added to Cluster '%s' " % cluster.name, request = request)

        if request.action == 'terminate':
            return 0

        total_jobs = 0
        for nodeset_name in cluster.nodesets:
            nodeset = self.client.getNodeset(nodeset_name)
            if not nodeset:
                raise VC3InvalidRequest("Nodeset '%s' has not been declared." % nodeset_name, request = request)
            total_jobs += nodeset.node_number

        return total_jobs


class VC3InvalidRequest(Exception):
    def __init__(self, reason, request = None):
        self.reason  = reason
        self.request = request

    def __str__(self):
        return str(self.reason)

