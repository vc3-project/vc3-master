#
# Classes and interfaces for periodic tasks/processing.
#  

import threading

class VC3TaskRunner(threading.thread):
    '''
    This object contains Tasks to be run sequentiall every 
    
    '''
    def __init__(self, config, section):
        '''
        
        '''



class VC3Task(object):
    '''
    
    '''
    def __init__(self, parent, config, section):
        self.log = logging.getlogger()    
        self.parent = parent
        self.config = config
        self.section = section    
                
    def runtask(self):
        
        raise NotImplementedError