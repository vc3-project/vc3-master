#!/usr/bin/env python

import ConfigParser
import StringIO
from base64 import b64encode

import os
import json
import traceback

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
        if allocation.state == 'new': 
            # nexts: validated, invalid
            (next_state, reason) = self.state_new(allocation)

        if allocation.state == 'authconfigured':
            # nexts: configured, pending, terminating
            # waits for action = run
            (next_state, reason) = self.state_authconfigured(allocation)
        
        if allocation.state == 'validated':
            # nexts: validated, configured, terminating
            # waits for cluster_state = configured | running
            (next_state, reason) = self.state_validated(allocation)

        if allocation.state == 'exhausted':
            # nexts: validated, configured, terminating
            # waits for cluster_state = configured | running
            (next_state, reason) = self.state_exhausted(allocation)

        if allocation.state == 'invalid':
            # nexts: configured, pending, terminating
            # waits for action = run
            (next_state, reason) = self.state_invalid(allocation)    
        
        if next_state is not allocation.state:
            
            try:
                self.log.debug("allocation '%s'  state '%s' -> %s'", allocation.name, allocation.state, next_state)
                allocation.state = next_state
                self.client.storeAllocation(allocation)

            except Exception, e:
                self.log.warning("Storing the new Allocation state failed.")
                raise e


    def state_new(self, allocation):
        '''
        Generates auth info. 
        '''
        self.log.debug('processing new allocation %s' % allocation.name)
        try:
            self.generate_auth_tokens(allocation)
            return ('authconfigured', None)
        except Exception, e:
            self.log.error("Exception during auth generation %s"% str(e))
            self.log.error(traceback.format_exc(None))
            return ('invalid', 'Invalid allocation: %s' % e.reason)
        return ('invalid', 'Failure: invalid allocation.')

    def state_authconfigured(self, allocation):
        '''
        Confirms that allocation can be submitted to...
        '''
        return ('validated', None)
    
    def state_validated(self, allocation):
        '''
        Confirms that allocation is not exhausted. 
        '''
        return ('validated', None)    

    def state_invalid(self, allocation):
        '''
        Tries to fix allocation.  
        '''
        return ('invalid', None)  
    
    
    def generate_auth_tokens(self, allocation):
        """ 
        Generate SSH priv/pub keys and base64 encode them, placing them in allocation 
        """ 
        self.log.info("Generating or retrieving SSH keys for %s", allocation.name)
        self.ssh = self.parent.parent.ssh
        (pub, priv) = self.ssh.getkeys(allocation.name)
        self.log.debug("public key: %s", pub)
        encoded_pub = b64encode(pub)
        encoded_priv = b64encode(priv)
        allocation.sectype = "ssh-rsa"
        allocation.pubtoken = encoded_pub
        allocation.privtoken = encoded_priv
        
    