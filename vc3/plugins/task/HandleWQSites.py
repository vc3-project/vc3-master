#!/usr/bin/env python

import json

from vc3.task import VC3Task
from pluginmanager import PluginManager
from vc3.infoclient import InfoClient 


class HandleWQSites(VC3Task):
    '''
    Plugin to transition Requests from 'new' to 'initialized' state.
     
    '''
    
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
        for requestid in self.current_sites:
            if not requestid in requests:
                self.current_sites[requestid].terminate()
                sites_to_delete.append(requestid)

        # because deleting from current iterator is bad juju
        for requestid in sites_to_delete:
            del self.current_sites[requestid]

    def process_request(self, requestid, request):
        if not requestid in self.current_sites:
            if 'action' in request:
                action = request['action']
                if action == 'spawn':
                    def launch_core():
                        # probably this should we handle by the execute plugin
                        cmd = ['vc3-core', '--requestid', requestid]
                        subprocess.check_call(cmd)
                    #launch_core() # for testing
                    #sys.exit(1)
                    self.current_sites[requestid] = Process(target = launch_core)
                    self.current_sites[requestid].start()
            else:
                self.log.info("Malformed request for '%s' : no action specified." % (requestid,))


