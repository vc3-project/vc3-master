'''
Dynamic plugin to launch VM via Condor-G

'''

import logging

class CondorCloud(object):

    def __init__(self, parent, config, section):
        self.log = logging.getLogger()    
        self.parent = parent
        self.config = config
        self.section = section    
        self.running = []
        self.log.debug("Execute dynamic plugin initialized.")
        
        
    def launch(self, name, config=None, cmd=None):
        '''
        Launches a VM on configured cloud via Condor-G. 
        Labels the instance with label <name>.
        Triggers command <cmd> on boot via cloud-init. 
          
        '''
        
        
    def terminate(self, name):
        '''
        Terminates the VM with label <name>
        
        '''