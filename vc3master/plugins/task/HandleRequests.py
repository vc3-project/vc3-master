#!/usr/bin/env python

import ConfigParser
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

        self.log.debug("Processing %d requests" % len(requests))

        self.process_requests(requests)

    def process_requests(self, requests):
        for r in requests:
            self.process_request(r)

    def process_request(self, request):
        next_state  = None
        reason      = None

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
            (next_state, reason) = (state, None)

        if reason:
            request.state_reason = reason
        request.state = next_state

        if not client.storeRequest(request):
            raise Exception("Storing the new request state failed.")

    def state_by_cluster(self, request, valid):

        cluster_state = request.cluster_state

        if cluster_state not in valid:
            return ('terminating', "Failure: cluster reported invalid state '%s'")

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

        if self.request_is_valid(request):
            return ('validated', None)
        return ('terminated', 'Failure: invalid request')

    def state_validated(self, request):
        return self.state_by_cluster('new', 'configured')

    def state_configured(self, request):
        # nexts: configured, pending, terminating
        # waits for action = run

        action = request.action

        if not action:
            return ('configured', 'Waiting for run action.')

        if action == 'run':
            return ('pending', None)

        if action == 'terminate':
            return ('terminating', 'Explicit termination requested.')

        return ('terminating', "Failure: invalid '%s' action" % str(action))

    def state_pending(self, request):
        return self.state_by_cluster(request, ['configured', 'growing', 'running'])

    def state_growing(self, request):
        action = request.action

        if action not in ['run', 'terminate']:
            return ('shrinking', "Failure: once started, action should be one of run|terminate")

        if action == 'terminate':
            return ('shrinking', 'Explicit termination requested.')

        # what follows is for action = 'run'
        return self.state_by_cluster(request, ['growing', 'running'])

    def state_running(self, request):
        action = request.action

        if action not in ['run', 'terminate']:
            return ('shrinking', "Failure: once started, action should be one of run|terminate")

        if action == 'terminate':
            return ('shrinking', 'Explicit termination requested.')

        # what follows is for action = 'run'
        return self.state_by_cluster(request, ['running'])

    def state_shrinking(self, request):
        action = request.action

        if action is not 'terminate':
            # ignoring action... do something here?
            pass

        return self.state_by_cluster(request, ['shrinking', 'terminated'])

    def state_terminating(self, request):
        action = request.action

        if action is not 'terminate':
            # ignoring action... do something here?
            pass

        if self.is_everything_cleaned_up(request):
            return ('terminated', None)

        return self.state_by_cluster(request, ['shrinking', 'terminated'])


