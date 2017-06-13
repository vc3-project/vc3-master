#!/usr/bin/env python
# 
#  This Task plugin searches for new user vcluster Requests
#
#  - poll for requests in state 'new'
#  --  determine need for core
#  if CORE:
#     -- generate factory config  
#     -- launch core instance
#     -- confirm core launched 
#     -- store request in state 'initialized'
#
from vc3.task import VC3Task

class InitRequests(VC3Task):
    '''
    Plugin to transition Requests from 'new' to 'initialized' state.
     
    '''
    
    
    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)