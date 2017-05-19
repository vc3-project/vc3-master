#
# Dynamic plugin to simply execute on local host via command line. 
#
import logging

import subprocess
from multiprocessing import Process

class Execute(object):

    def __init__(self, parent, config, section):
        self.log = logging.getLogger()    
        self.parent = parent
        self.config = config
        self.section = section    
        self.log.debug("Execute dynamic plugin initialized.")
    
