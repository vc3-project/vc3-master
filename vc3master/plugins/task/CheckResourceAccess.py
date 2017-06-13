#!/usr/bin/env python


from vc3.task import VC3Task

class CheckResourceAccess(VC3Task):
    '''
    Plugin to confirm that all Allocations are actually able to reach their specified Resource. 
    -- For SSH targets this means do an SSH login.
    -- For MFA targets this means check infoservice for recent query from factory. 
    -- For Cloud targets this means do an API call with the specified credential to check auth. 
     
    '''
    
    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)