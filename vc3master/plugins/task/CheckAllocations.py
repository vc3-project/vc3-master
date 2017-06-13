#!/usr/bin/env python




from vc3.task import VC3Task

class CheckAllocations(VC3Task):
    '''
    Plugin to do consistency/sanity checks on Allocations.
     
    '''
    
    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)