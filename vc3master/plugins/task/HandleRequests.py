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


    def runtask(self):
        self.log.info("Running task %s" % self.section)

        self.log.debug("Polling master....")

        requests = self.client.listRequests()

        n = len(requests) if requests else 0
        self.log.debug("Processing %d requests" % n)

        self.process_requests(requests)

    def process_requests(self, requests):
        if requests:
            for r in requests:
                self.process_request(r)

    def process_request(self, request):
        next_state  = None
        reason      = None

        self.log.debug("Processing request '%s'", request.name)

        if   request.state == 'new': 
            # nexts: validated, terminated
            (next_state, reason) = self.state_new(request)

        if request.state == 'validated':
            # nexts: validated, configured, terminating
            # waits for cluster_state = configured | running
            (next_state, reason) = self.state_validated(request)

        if request.state == 'configured':
            # nexts: configured, pending, terminating
            # waits for action = run
            (next_state, reason) = self.state_configured(request)

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

        if reason:
            request.state_reason = reason

        if next_state is not request.state:
            try:
                self.log.debug("request '%s'  state '%s' -> %s'", request.name, request.state, next_state)
                request.state = next_state
                self.client.storeRequest(request)

            except Exception, e:
                self.log.warning("Storing the new request state failed.")
                raise e

    def state_by_cluster(self, request, valid):

        cluster_state = request.cluster_state

        if cluster_state not in valid:
            return ('terminating', "Failure: cluster reported invalid state '%s' when request was in state '%s'" % (cluster_state, request.state))

        if cluster_state == 'new':
            return ('validated', 'Waiting for factory to configure itself.')

        if cluster_state == 'configured':
            return ('pending', 'Waiting for factory to start filling the request.')

        if cluster_state == 'growing':
            return ('growing', 'Factory is fulfilling the request.')

        if cluster_state == 'shrinking':
            return ('shrinking', 'Factory is draining the request.')

        if cluster_state == 'running':
            return ('running', 'Factory completely fulfilled the request')

        if cluster_state == 'terminated':
            return ('terminating', 'cluster reported terminated state')

        if cluster_state == 'failure':
            return ('terminating', 'Failure: cluster reported a failure: %s.' % 'because of reasons')

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
                raise e
                return ('terminated', 'Invalid request: %s' % e.reason)

        return ('terminated', 'Failure: invalid request')

    def state_validated(self, request):
        self.log.debug('validating request %s' % request.name)

        # validate request here

        return self.state_by_cluster(request, ['new', 'configured'])

    def state_configured(self, request):
        # nexts: configured, pending, terminating
        # waits for action = run

        self.log.debug('waiting for run action for request %s' % request.name)

        action = request.action
        if not action:
            return ('configured', 'Waiting for run action.')

        if action == 'run':
            return ('pending', None)

        if action == 'terminate':
            return ('terminating', 'Explicit termination requested.')

        return ('terminating', "Failure: invalid '%s' action" % str(action))

    def state_pending(self, request):
        self.log.debug('waiting for factory to start fullfilling request %s' % request.name)
        return self.state_by_cluster(request, ['configured', 'growing', 'running'])

    def state_growing(self, request):
        self.log.debug('waiting for factory to fullfill request %s' % request.name)

        action = request.action
        if action and (action not in ['run', 'terminate']):
            raise(Exception(str(action)))
            return ('shrinking', "Failure: once started, action should be one of run|terminate")

        if action == 'terminate':
            return ('shrinking', 'Explicit termination requested.')

        # what follows is for action = 'run'
        return self.state_by_cluster(request, ['growing', 'running'])

    def state_running(self, request):
        self.log.debug('request %s is running' % request.name)

        action = request.action
        if action and action not in ['run', 'terminate']:
            return ('shrinking', "Failure: once started, action should be one of run|terminate")

        if action == 'terminate':
            return ('shrinking', 'Explicit termination requested.')

        # what follows is for action = 'run'
        return self.state_by_cluster(request, ['running'])

    def state_shrinking(self, request):
        self.log.debug('request %s is shrinking' % request.name)

        action = request.action
        if action and action is not 'terminate':
            # ignoring action... do something here?
            pass

        return self.state_by_cluster(request, ['shrinking', 'terminated'])

    def state_terminating(self, request):
        self.log.debug('request %s is terminating' % request.name)

        action = request.action
        if action and action is not 'terminate':
            # ignoring action... do something here?
            pass

        if self.is_everything_cleaned_up(request):
            return ('terminated', None)

        return self.state_by_cluster(request, ['shrinking', 'terminated'])


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

        name = request.name + '.' + allocation_name
        config.add_section(name)

        allocation = self.client.getAllocation(allocation_name)
        if not allocation:
            raise VC3InvalidRequest("Allocation '%s' has not been declared." % allocation_name, request = request)

        resource = self.client.getResource(allocation.resource)
        if not resource:
            raise VC3InvalidRequest("Resource '%s' has not been declared." % allocation.resource, request = request)

        if resource.accesstype == 'batch':
            config.set(name, 'batchsubmitplugin',          'CondorSSH')
            config.set(name, 'batchsubmit.condorssh.user', allocation.accountname)
            config.set(name, 'condorssh.batch',            resource.accessflavor)
            config.set(name, 'condorssh.host',             resource.accesshost)
            config.set(name, 'condorssh.port',             str(resource.accessport))
            config.set(name, 'condorssh.authprofile',      name)
            config.set(name, 'executable',                 'vc3-builder')
            config.set(name, 'executable.args',            self.environment_args(request))
        elif resource.accesstype == 'cloud':
            config.set(name, 'batchsubmitplugin',          'CondorEC2')
        else:
            raise VC3InvalidRequest("Unknown resource access type '%s'" % str(resource.accesstype), request = request)

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

        name = request.name + '.' + allocation_name
        config.add_section(name)

        allocation = self.client.getAllocation(allocation_name)
        if not allocation:
            raise VC3InvalidRequest("Allocation '%s' has not been declared." % allocation_name, request = request)

        resource = self.client.getResource(allocation.resource)
        if not resource:
            raise VC3InvalidRequest("Resource '%s' has not been declared." % allocation.resource, request = request)
        # credible always generates rsa keys
        allocation.sectype = 'rsa'
        (allocation.pubtoken, allocation.privtoken) = self.generate_auth_tokens(name)

        self.log.debug("allocation sectype: %s", allocation.sectype)
        self.log.debug("allocation privtoken: %s", allocation.privtoken)
        self.log.debug("allocation pubtoken: %s", allocation.pubtoken)

        if resource.accessmethod == 'ssh':
            config.set(name, 'plugin',        'SSH')
            config.set(name, 'ssh.type',      allocation.sectype)
            config.set(name, 'ssh.publickey', allocation.pubtoken)
            config.set(name, 'ssh.privatekey', allocation.privtoken)
        elif resource.accesstype == 'gsissh':
            raise NoImplementedError
        else:
            raise VC3InvalidRequest("Unknown resource access method '%s'" % str(resource.accessmethod), request = request)


    def environment_args(self, request):

        environments = []
        self.log.debug("Retrieving environments: %s" % request.environments)
        for ename in request.environments:
            eo = self.client.getEnvironment(ename)
            if eo is not None:
                environments.append(eo)
            else:
                self.log.debug("Failed to retrieve environment %s" % ename)

        packages = []
        for e in environments:
            packages.extend(e.packagelist)

        if len(packages) < 1:
            self.log.warning("No environment defined a package list for Request")

        vs    = [ "VC3_REQUESTID='%s'" % request.name, ]

        for e in environments:
            for k in e.envmap:
                vs.append("%s=%s" % (k, e.envmap[k]))

        reqs  = ' '.join(['--require %s' % x for x in packages])
        vars  = ' '.join(['--var %s' % x for x in vs])

        s  = vars + ' ' + reqs

        return s

    def is_everything_cleaned_up(self, request):
        ''' TO BE FILLED '''
        return True


class VC3InvalidRequest(Exception):
    def __init__(self, reason, request = None):
        self.reason  = reason
        self.request = request

    def __str__(self):
        return str(self.reason)

