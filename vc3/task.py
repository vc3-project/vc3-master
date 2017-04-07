#
# Classes and interfaces for periodic tasks/processing.
#  

import logging
import threading
import time
from pluginmanager import PluginManager

class VC3TaskSet(threading.Thread):
    '''
    This object contains Tasks to be run sequentiall every 
    
    '''
    def __init__(self, config, section):
        '''
        
        '''
        self.log = logging.getLogger()
        threading.Thread.__init__(self) # init the thread
        self.config = config
        self.section = section
        self.polling_interval = int(self.config.get(self.section, 'polling_interval')) 
        pil = self.config.get(self.section, 'taskplugins').split(',')
        self.pluginstrs=[]
        for pis in pil:
            self.pluginstrs.append(pis.strip())       
        self.tasks = []
        pm = PluginManager()
        for pn in self.pluginstrs:
            self.log.debug("Loading task plugin %s" % pn)
            p = pm.getplugin(parent=self,
                             paths=['vc3','plugins','task'],
                             name=pn,
                             config=self.config,
                             section=self.section)
            self.tasks.append(p)
        self.log.debug("Task plugins initialized.")


    def run(self):
        '''
        '''
        self.log.debug("Running Taskset %s" % self.section)
        for p in self.tasks:            
            p.runtask()
        self.log.debug("Sleeping for %s seconds..." % self.polling_interval)    
        time.sleep(self.polling_interval)

class VC3Task(object):
    '''
    
    '''
    def __init__(self, parent, config, section):
        self.log = logging.getLogger()    
        self.parent = parent
        self.config = config
        self.section = section    
                
    def runtask(self):
        '''
        '''
        raise NotImplementedError