#!/usr/bin/env python
# 
#
from vc3.task import VC3Task

class InitInstanceAuth(VC3Task):
    '''
    Plugin to do first-time overall VC3 instance setup. 
    Confirm that CA exists, create host cert for this server. 
     
    '''
    
    
    def runtask(self):
        '''
        '''
        self.log.debug("Running task %s" % self.section)