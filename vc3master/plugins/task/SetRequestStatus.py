#!/usr/bin/env python

import ConfigParser
import StringIO
from base64 import b64encode

import os
import json

from vc3master.task import VC3Task

import pluginmanager as pm


class SetRequestStatus(VC3Task):
    '''
    Plugin to merge in a single "status" attribute
    the information in the dictionary "statusraw"
    for a given Request object
    '''

    def __init__(self, parent, config, section):
        '''
        parent is object of class VC3TaskSet 
        parent of VC3TaskSet is VC3Master
        '''
        super(HandleRequests, self).__init__(parent, config, section)
        self.client = parent.client
        self.log.debug("SetRequestStatus VC3Task initialized.")

    def runtask(self):
        '''
        base class method to perform actions for this plugin.
        It loops over the list of requests and calls method
                process_request
        '''
        self.log.info("Running task %s" % self.section)
        self.log.debug("Polling master....")
        request_l = self.client.listRequests()
        n = len(request_l) if request_l else 0
        self.log.debug("Processing %d requests" % n)
        if request_l:
            for request in request_l:
                self.process_request(request)

    def process_request(self, request):
        '''
        merges the content of Request attribute statusraw into a single dictionary
        and set attribute statusinfo with that aggreagated result

        Example: 
        if statusraw looks like this 
        
             {'factory1': {'nodeset1': {'queue1': {'running': 1, 'idle': 5}},
                           'nodeset2': {'queue2': {'running': 3, 'idle': 6},
                                        'queue3': {'running': 3, 'idle': 7}
                                       }
                          },
              'factory2': {'nodeset1': {'queue4': {'running': 8, 'idle': 1}}}
             }
        
        the output of this methid, status, must be like this
        
              {'nodeset1': {'running': 9, 'idle': 6}, 
               'nodeset2': {'running': 6, 'idle': 13}
              }
        

        :param Request request: the Request object being processed
        '''

        self.log.debug('Starting')

        status = {}
        
        for factory, nodeset_l in request.statusraw.items():
            self.log.debug('processing factory %s' %factory)
            for nodeset, queue_l in nodeset_l.items():
                self.log.debug('processing nodeset %s' %nodeset)
                if nodeset not in status.keys():
                    self.log.debug('adding new nodeset %s to the dictionary keys' %nodeset)
                    status[nodeset] = {}
                for queue, jobstatus_l in queue_l.items():
                    self.log.debug('processing queue %s' %queue)
                    for jobstatus, number in jobstatus_l.items():
                        self.log.debug('adding %s to status %s'%(number, jobstatus))
                        if jobstatus not in status[nodeset].keys():
                            status[nodeset][jobstatus] = number
                        else:
                            status[nodeset][jobstatus] += number



