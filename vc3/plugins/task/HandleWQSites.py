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
        self.log.debug("Running task %s" % self.section)
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

        for site_name in requests:
            self.process_request(site_name, requests[site_name])

        # terminate site requests that are no longer present
        sites_to_delete = []
        for site_name in self.current_sites:
            if not site_name in requests:
                self.current_sites[site_name].terminate()
                sites_to_delete.append(site_name)

        # because deleting from current iterator is bad juju
        for site_name in sites_to_delete:
            del self.current_sites[site_name]

    def process_request(self, site_name, request):
        if not site_name in self.current_sites:
            if 'action' in request:
                action = request['action']
                if action == 'spawn':
                    def launch_core():
                        # probably this should we handle by the execute plugin
                        cmd = ['vc3-core', '--requestid', site_name]
                        subprocess.check_call(cmd)
                    #launch_core() # for testing
                    #sys.exit(1)
                    self.current_sites[site_name] = Process(target = launch_core)
                    self.current_sites[site_name].start()
            else:
                self.log.info("Malformed request for '%s' : no action specified." % (site_name,))