#
# Dynamic plugin to simply execute on local host via command line. 
#

class Execute(object):

    def __init__(self, parent, config, section):
        self.log = logging.getlogger()    
        self.parent = parent
        self.config = config
        self.section = section    
        
    