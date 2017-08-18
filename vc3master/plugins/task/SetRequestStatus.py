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
        merge the content of statusraw into a single dictionary
        '''
        statusraw = request.statusraw
