#!/usr/bin/env python

import ConfigParser
import os
import json

from vc3.task import VC3Task
from vc3.infoclient import InfoClient 

import pluginmanager as pm



class HandleGenericLocalExecute(VC3Task):
    '''
    Plugin to transition Requests from 'new' to 'initialized' state.
     
    '''

    def __init__(self, parent, config, section):
        super(HandleGenericLocalExecute, self).__init__(parent, config, section)

        # current sites being served, key-ed by requestid
        self.requestids     = {}

    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)
        self.log.debug("Polling master....")
        doc = self.parent.parent.infoclient.getdocument('request')
        if doc:
            self.log.debug("Got Request doc. Processing...")
            self.process_requests(doc)
        else:
            self.log.debug("No request doc.")

    def prepare_conf(self, requestid, request):
        conf = ConfigParser.RawConfigParser()
        conf.add_section(requestid)
        conf.set(requestid, 'requestid', requestid)

        return conf

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
            self.process_request(requestid, requests[requestid])

        # terminate site requests that are no longer present
        sites_to_delete = []
        for requestid in self.requestids:
            if not requestid in requests:
                self.requestids[requestid].terminate()
                sites_to_delete.append(requestid)

        # because deleting from current iterator is bad juju
        for requestid in sites_to_delete:
            del self.requestids[requestid]

    def process_request(self, requestid, request):
        if not requestid in self.requestids:
            if 'action' in request:
                action = request['action']
                if action == 'spawn':
                    conf = self.prepare_conf(requestid, request)
                    self.requestids[requestid] = Execute(parent = self, config = conf, section = requestid)
            else:
                self.log.info("Malformed request for '%s' : no action specified." % (requestid,))


