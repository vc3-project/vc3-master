#!/usr/bin/env python
#
#  Poll for resources in state 'new'
#  IF SSH or MFA:
#      -- Create SSH keypair
#      -- generate pairing code
#      -- Create resource tool command line.  
#      -- Store Resrouce in state 'ready-for-init'
#
#

from vc3.task import VC3Task

class InitResources(VC3Task):
    '''
    Plugin to transition Resources from 'new' to 'read-to-init' state.
     
    '''
    
    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)