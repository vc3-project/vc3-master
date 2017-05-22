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

        self.dynamic = pm.getplugin(parent=self, 
                                    paths=['vc3', 'plugins', 'dynamic'], 
                                    name='Execute',
                                    config=self.config, 
                                    section='execute-plugin')

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
        conf = ConfigParser.SafeConfigParser()
        conf.add_section('core')
        conf.set('core', 'requestid', requestid)

        confFileNameBase = os.path.expanduser('~/var/confs/')

        if not os.path.isdir(confFileNameBase):
            os.makedirs(confFileNameBase)

        confFileName = os.path.join(confFileNameBase, requestid + '.local.core.conf')
        with open(confFileName, 'w') as f:
            conf.write(f)

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
                    self.requestids[requestid] = self.dynamic.start(config = conf)
            else:
                self.log.info("Malformed request for '%s' : no action specified." % (requestid,))


