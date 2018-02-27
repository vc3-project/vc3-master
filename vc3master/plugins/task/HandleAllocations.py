#!/usr/bin/env python

import ConfigParser
import StringIO
from base64 import b64encode, b64decode

import os
import json
import traceback
import subprocess
import tempfile

from vc3master.task import VC3Task
from vc3infoservice.infoclient import InfoConnectionFailure, InfoEntityMissingException

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

        try:
            allocations = self.client.listAllocations()
            n = len(allocations) if allocations else 0
            self.log.debug("Processing %d allocations" % n)
            if allocations:
                for a in allocations:
                    self.process_allocation(a)
        except InfoConnectionFailure, e:
            self.log.warning("Could not read allocations from infoservice. (%s)", e)
    
    def process_allocation(self, allocation):
        next_state  = allocation.state
        reason      = None

        self.log.debug("Processing allocation '%s'", allocation.name)
        if next_state == 'new': 
            # nexts: configured, failure
            (next_state, reason) = self.state_new(allocation)

        if next_state == 'validation_failure':
            # nexts: failure, configured
            (next_state, reason) = self.state_validation_failure(allocation)
        
        if next_state == 'configured':
            # nexts: configured, ready, validation_failure, failure
            (next_state, reason) = self.state_configured(allocation)

        if next_state == 'ready':
            # nexts: ready
            (next_state, reason) = self.state_ready(allocation)

        if next_state == 'failure':
            # nexts: failure
            (next_state, reason) = self.state_failure(allocation)    

        if (next_state is not allocation.state) or (reason is not allocation.state_reason):
            try:
                self.log.debug("allocation '%s'  state '%s' -> %s'", allocation.name, allocation.state, next_state)
                allocation.state        = next_state
                allocation.state_reason = reason
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
            return ('configured', 'Waiting for allocation to be validated.')
        except Exception, e:
            self.log.error("Exception during auth generation %s"% str(e))
            self.log.error(traceback.format_exc(None))
            return ('failure', 'Invalid allocation: %s' % e.reason)
        return ('failure', 'Failure: invalid allocation.')

    def state_configured(self, allocation):
        '''
        Confirms that allocation can be submitted to.
        '''
        self.log.debug('Validating allocation %s' % allocation.name)

        if allocation.action != 'validate':
            return ('configured', 'To validate allocation, please click on the allocation profile name %s to follow the instructions to copy the allocation credentials to the corresponding resource.')

        allocation.action = None

        try:
            resource = self.client.getResource(allocation.resource)

            if resource.accessmethod == 'ssh':
                self.log.debug('Attempting to contact %s to validate allocation %s' % (resource.accesshost, allocation.name))
                self.validate(allocation, resource) # raises exception on failure
                self.log.debug('Allocation %s has been validated.' % (allocation.name,))
                return ('ready', 'Allocation credentials were used succesfully to login into the resource.' % allocation.resource)
            else:
                self.log.debug('Cannot yet validate using %s' % resource.accessmethod)
                return ('ready', 'Only resources that can be contacted through ssh can be validated at this time.')

        except subprocess.CalledProcessError, e:
            self.log.debug('Allocation %s could not be validated: %s' % (allocation.name, e))
            return ('validation_failure', 'Could not validate allocation.')
        except InfoConnectionFailure, e:
            allocation.action = 'validate'
            return ('configured', 'Could not validate allocation because of a transient connectivity error. Trying again.')
        except Exception, e:
            self.log.debug('Allocation %s could not be validated: %s' % (allocation.name, e))
            return ('failure', 'There was an internal error: %s' % e)

    def state_ready(self, allocation):
        return ('ready', 'Allocation is ready to be used.')    

    def state_failure(self, allocation):
        return ('failure', allocation.state_reason)  

    def state_validation_failure(self, allocation):
        if allocation.action == 'validate':
            return ('configured', 'Retrying to validate allocation')
        else:
            return ('validation_failure', allocation.state_reason)

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
        
    def validate(self, allocation, resource):
        with tempfile.NamedTemporaryFile(mode='w+b', bufsize=0, delete=False) as fh:
            fh.write(b64decode(allocation.privtoken))
            fh.seek(0)
            fh.flush()

            os.chmod(fh.name, 0400)

            subprocess.check_call([
                'ssh', 
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'ConnectTimeout=10',
                '-i', fh.name,
                '-l', allocation.accountname,
                '-p', resource.accessport,
                resource.accesshost, '--', '/bin/date'])



