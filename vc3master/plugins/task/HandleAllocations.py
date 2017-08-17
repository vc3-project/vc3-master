#!/usr/bin/env python

import ConfigParser
import StringIO
from base64 import b64encode

import os
import json

from vc3master.task import VC3Task

import pluginmanager as pm

class HandleAllocations(VC3Task):
    '''
    Plugin to manage the life cycle of all Allocations.
     
    '''
    def __init__(self, parent, config, section):
        super(HandleAllocations, self).__init__(parent, config, section)
        self.client = parent.client
        self.log.debug("HandleAllocations VC3Task initialized.")

    def runtask(self):
        self.log.info("Running task %s" % self.section)
        self.log.debug("Polling master....")
        allocations = self.client.listAllocations()
        n = len(allocations) if allocations else 0
        self.log.debug("Processing %d allocations" % n)
        if allocations:
            for a in allocations:
                self.process_allocation(a)
    
    def process_allocation(self, allocation):
        next_state  = None
        reason      = None

        self.log.debug("Processing allocation '%s'", allocation.name)
    
    
    
    
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
    