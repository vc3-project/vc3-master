#
# Classes and interfaces for periodic tasks/processing.
#  

import logging
import threading
import datetime
import time

import pluginmanager as pm

class VC3TaskSet(threading.Thread):
    '''
    This object contains Tasks to be run sequentiall every 
    
    '''
    def __init__(self, parent, config, section):
        '''
        Contains one or more task plugins, which are run sequentially every <polling_interval> seconds. 
        '''
        self.log = logging.getLogger()
        threading.Thread.__init__(self) # init the thread
        self.stopevent = threading.Event()
        self.thread_loop_interval = 1
        self.parent = parent
        self.config = config
        self.section = section
        self.polling_interval = int(self.config.get(self.section, 'polling_interval')) 
        pil = self.config.get(self.section, 'taskplugins').split(',')
        self.pluginstrs=[]
        for pis in pil:
            self.pluginstrs.append(pis.strip())       
        self.tasks = []
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
        tdinterval = datetime.timedelta(seconds = self.polling_interval)
        lastrun = datetime.datetime(2000, 12, 1)
        while not self.stopevent.isSet():
            #self.log.debug("Checking interval...")
            if datetime.datetime.now() - lastrun > tdinterval:
                for p in self.tasks:            
                    p.runtask()
                lastrun = datetime.datetime.now()
                self.log.debug("Waiting for %s seconds..." % self.polling_interval)    
            time.sleep(self.thread_loop_interval)


    def join(self,timeout=None):
        if not self.stopevent.isSet():
            self.log.debug('joining thread')
            self.stopevent.set()
            #self._join()
            #threading.Thread.join(self, timeout)



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
