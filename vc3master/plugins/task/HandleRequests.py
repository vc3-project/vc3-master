#!/usr/bin/env python

import ConfigParser
import os
import json

from vc3master.task import VC3Task
from vc3infoservice.infoclient import InfoClient 

import pluginmanager as pm

class HandleRequests(VC3Task):
    '''
    Plugin to manage the life cycle of all requests.
     
    '''

    def __init__(self, parent, config, section):
        super(HandleRequests, self).__init__(parent, config, section)

        self.dynamic = pm.getplugin(parent=self, 
                                    paths=['vc3', 'plugins', 'dynamic'], 
                                    name='HandleRequests',
                                    config=self.config, 
                                    section='requests-plugin')

        # current requests being tracked, key-ed by requestid
        self.requestids     = {}

    def runtask(self):
        self.log.info("Running task %s" % self.section)

        self.log.debug("Polling master....")
        doc = self.parent.parent.infoclient.getdocument('request')
        if doc:
            self.log.debug("Got Request doc. Processing...")
            self.process_requests(doc)
        else:
            self.log.debug("No request doc.")


    def process_requests(self, doc):
        try:
            ds = json.loads(doc)
        except Exception as e:
            raise e

        try:
            requests = ds['request']
        except KeyError:
            # no requests available
            return

        for requestid in requests:
            self.process_request(requests[requestid])

    def process_request(self, request):
        name           = request['name']
        state          = request['state']

        next_state = None
        reason     = None

        if   state == 'new': 
            # nexts: validated, terminated
            (next_state, reason) = self.state_new(request)

        elif state == 'validated':
            # nexts: validated, configured, terminating
            # waits for cluster_state = configured | running
            (next_state, reason) = self.state_validated(request)

        elif state == 'configured':
            # nexts: configured, pending, terminating
            # waits for action = run
            (next_state, reason) = self.state_configured(request)

        elif state == 'pending':
            # nexts: pending, growing, running, terminating
            # to growing until at least one element of the request is fulfilled
            (next_state, reason) = self.state_pending(request)

        elif state == 'growing':
            # nexts: growing, shrinking, running, terminating
            # to running until all elements of the request are fulfilled
            (next_state, reason) = self.state_growing(request)

        elif state == 'running':
            # nexts: shrinking, running, terminating
            # waits for action = terminate
            (next_state, reason) = self.state_running(request)

        elif state == 'shrinking':
            # nexts: shrinking, terminating
            # to terminating 
            (next_state, reason) = self.state_shrinking(request)

        elif state == 'terminating':
            # waits until everything has been cleanup
            (next_state, reason) = self.state_terminating(request)

        elif state == 'terminated':
            (next_state, reason) = (state, None)
            pass

        else:
            raise Exception("request '%s' has invalid state '%s'", name, str(state))

        return self.update_state(request, next_state, reason)

    def update_state(self, request, next_state, reason):
        if next_state is None:
            raise Exception('Next state was not set! This should not have happened.')

        if reason:
            request['state_reason'] = reason

        obj = { 'request' : { request['name'] : request } }
        self.parent.parent.infoclient.storedocumentobject(obj, 'request')

    def state_by_cluster(self, request, valid):
        if 'cluster' not in request:
            return ('terminating', 'Failure: could not find cluster definition.')

        if 'state' not in request['cluster']:
            return ('terminating', "Failure: once configured, state of cluster should be set explicitly.")

        cluster_state = request['cluster']['state']
        # or: cluster_state = self.figure_out_cluster_state(request['cluster'])

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

        action = request.get('action', None)

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
        action = request.get('action', None)

        if action not in ['run', 'terminate']:
            return ('shrinking', "Failure: once started, action should be one of run|terminate")

        if action == 'terminate':
            return ('shrinking', 'Explicit termination requested.')

        # what follows is for action = 'run'
        return self.state_by_cluster(request, ['growing', 'running'])

    def state_running(self, request):
        action = request.get('action', None)

        if action not in ['run', 'terminate']:
            return ('shrinking', "Failure: once started, action should be one of run|terminate")

        if action == 'terminate':
            return ('shrinking', 'Explicit termination requested.')

        # what follows is for action = 'run'
        return self.state_by_cluster(request, ['running'])

    def state_shrinking(self, request):
        action = request.get('action', None)

        if action is not 'terminate':
            # ignoring action
            pass

        return self.state_by_cluster(request, ['shrinking', 'terminated'])

    def state_terminating(self, request):
        action = request.get('action', None)

        if action is not 'terminate':
            # ignoring action
            pass

        if self.is_everything_cleaned_up(request):
            return ('terminated', None)

        return self.state_by_cluster(request, ['shrinking', 'terminated'])


    def figure_out_cluster_state(self, cluster):
        # THIS ASSUMES ALL COMPONENTS OF THE CLUSTER HAVE THE SAME LIFETIME
        states = []
        for component_key in cluster:
            component = cluster[component_key]
            states.append(component.get('state', 'unknown'))

        if 'failure' in states:
            return 'failure'

        if all( [ 'running' == x for x in states ] ):
            return 'running'

        if any( [ 'running' == x for x in states ] ):
            return 'growing'

        return None


