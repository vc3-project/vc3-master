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
        super(HandleRequests, self).__init__(parent, config, section)
        self.client = parent.client
        self.log.debug("SetRequestStatus VC3Task initialized.")

    def runtask(self):
        self.log.info("Running task %s" % self.section)
        self.log.debug("Polling master....")
        requests = self.client.listRequests()
        n = len(requests) if requests else 0
        self.log.debug("Processing %d requests" % n)
        if requests:
            for r in requests:
                self.process_request(r)
