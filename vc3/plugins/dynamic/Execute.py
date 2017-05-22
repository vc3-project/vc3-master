#
# Dynamic plugin to simply execute on local host via command line. 
#
import ConfigParser
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

        self.processes = {}
        

    def start(self, dynConfig, secname):
        requestid = self.dynConfig.get(secname, 'requestid')
        if requestid in self.processes:
            if self.processes[requestid].is_alive():
                raise Exception('A local core is already running with requestid: ' + requestid)

        def launch_core():
            cmd = ['vc3-core', '--requestid', requestid]
            subprocess.check_call(cmd)

        p = Process(target = launch_core)
        p.start()
        self.processes[requestid] = p

    def wait(self, requestid, timeout = None):
        self.processes[requestid].join(timeout)
        return self.processes[requestid].exitcode

    def terminate(self, requestid):
        if self.processes[requestid].is_alive():
            self.processes[requestid].terminate(timeout)

    def is_alive(self, requestid):
        return self.processes[requestid].is_alive()
    


