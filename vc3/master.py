#!/usr/bin/env python 

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2017 John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "0.9.1"
__maintainer__ = "John Hover"
__email__ = "jhover@bnl.gov"
__status__ = "Production"


import logging
import logging.handlers
import os
import platform
import pwd
import random
import json
import string
import socket
import sys
import threading
import time
import traceback

from multiprocessing import Process

from optparse import OptionParser
from ConfigParser import ConfigParser

# Since script is in package "vc3" we can know what to add to path for 
# running directly during development
(libpath,tail) = os.path.split(sys.path[0])
sys.path.append(libpath)

from vc3.plugin import PluginManager
from vc3.infoclient import InfoClient 

class VC3Master(object):
    
    def __init__(self, config):
        self.log = logging.getLogger()
        self.log.debug('VC3Master class init...')

        self.config = config
        self.certfile = os.path.expanduser(config.get('netcomm','certfile'))
        self.keyfile = os.path.expanduser(config.get('netcomm', 'keyfile'))
        self.chainfile = os.path.expanduser(config.get('netcomm','chainfile'))

        self.builder_path = config.get('dynamic','builder_path')
        
        self.log.debug("certfile=%s" % self.certfile)
        self.log.debug("keyfile=%s" % self.keyfile)
        self.log.debug("chainfile=%s" % self.chainfile)
        
        self.infoclient = InfoClient(config)    
        self.log.debug('VC3Master class done.')

        self.whitelist = ['work-queue-catalog', 'some-other-service']
        self.current_sites = {}
        
    def run(self):
        self.log.debug('Master running...')
        while True:
            self.log.debug("Master polling....")
            doc = self.infoclient.getdocument('request')
            self.process_requests(doc)
            time.sleep(5)
       
    def process_requests(self, doc):
        try:
            ds = json.loads(doc)
        except Exception as e:
            raise e

        try:
            requests = ds['request']
        except KeyError:
            # no requests available
            return

        for site_name in requests:
            self.process_request(site_name, requests[site_name])

        # terminate site requests that are no longer present
        sites_to_delete = []
        for site_name in self.current_sites:
            if not site_name in requests:
                self.current_sites[site_name].terminate()
                sites_to_delete.append(site_name)

        # because deleting from current iterator is bad juju
        for site_name in sites_to_delete:
            del self.current_sites[site_name]

    def process_request(self, site_name, request):
        if not site_name in self.current_sites:
            self.current_sites[site_name] = VC3SiteRequest(self.log, self.builder_factory())
        self.current_sites[site_name].perform(request, self.whitelist)

    def builder_factory(self):
        def builder(service):
            cmd  = self.builder_path + '/vc3-builder'
            cmd += ' --install   ' + self.builder_path + 'vc3-dev'
            cmd += ' --home      ' + self.builder_path + 'vc3-dev-home'
            cmd += ' --make-jobs 4'
            cmd += ' --require   vc3-dev'
            cmd += ' --require '
            cmd += service
            os.system(cmd)
        return builder
            

class VC3SiteRequest(object):
    def __init__(self, log, builder_factory):
        self.builder_factory = builder_factory
        self.log             = log
        self.processes       = {}

    def perform(self, request, whitelist):
        for service_name in request:
            if not service_name in whitelist:
                continue

            action = None
            try:
                action = request[service_name]['action']
            except KeyError:
                action = "not-specified"

            self.log.info(action)
            self.log.info(request)

            if not action == 'spawn':
                continue

            if not service_name in self.processes or not self.processes[service_name].is_alive():
                self.processes[service_name] = self.execute(service_name)

        # terminate site requests that are no longer present
        self.terminate_old_services(request)

    # probably this goes into the Execute plugin.
    def execute(self, service_name):
        p = Process(target = self.builder_factory, args = (service_name,))
        self.log.info('Starting service ' + service_name)
        p.start()
        return p

    def terminate_old_services(self,request):
        '''
        Terminate those services that no longer appear on the request.
        '''
        services_to_delete = []
        for service_name in self.processes:

            action = None
            try:
                action = request[service_name]['action']
            except KeyError:
                action = "not-specified"

            if (not service_name in request) or (action == 'off'):
                self.log.info('Terminating service ' + service_name)

                self.processes[service_name].terminate()
                self.processes[service_name].join(60)

                services_to_delete.append(service_name)
        for service_name in services_to_delete:
            del self.processes[service_name]

    def terminate(self):
        '''
        Like an 'empty' request, to vacate all running services.
        '''
        return self.terminate_old_services({})

class VC3MasterCLI(object):
    """class to handle the command line invocation of service. 
       parse the input options,
       setup everything, and run VC3Master class
    """
    def __init__(self):
        self.options = None 
        self.args = None
        self.log = None
        self.config = None

        self.__presetups()
        self.__parseopts()
        self.__setuplogging()
        self.__platforminfo()
        self.__checkroot()
        self.__createconfig()

    def __presetups(self):
        '''
        we put here some preliminary steps that 
        for one reason or another 
        must be done before anything else
        '''

    
    def __parseopts(self):
        parser = OptionParser(usage='''%prog [OPTIONS]
vc3-master is a information store for VC3

This program is licensed under the GPL, as set out in LICENSE file.

Author(s):
John Hover <jhover@bnl.gov>
''', version="%prog $Id: master.py 3-3-17 23:58:06Z jhover $" )

        parser.add_option("-d", "--debug", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.DEBUG, 
                          help="Set logging level to DEBUG [default WARNING]")
        parser.add_option("-v", "--info", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.INFO, 
                          help="Set logging level to INFO [default WARNING]")
        parser.add_option("--console", 
                          dest="console", 
                          default=False,
                          action="store_true", 
                          help="Forces debug and info messages to be sent to the console")
        parser.add_option("--quiet", dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.WARNING, 
                          help="Set logging level to WARNING [default]")
        parser.add_option("--conf", dest="confFiles", 
                          default="/etc/vc3/vc3-master.conf",
                          action="store", 
                          metavar="FILE1[,FILE2,FILE3]", 
                          help="Load configuration from FILEs (comma separated list)")
        parser.add_option("--log", dest="logfile", 
                          default="stdout", 
                          metavar="LOGFILE", 
                          action="store", 
                          help="Send logging output to LOGFILE or SYSLOG or stdout [default <syslog>]")
        parser.add_option("--runas", dest="runAs", 
                          #
                          # By default
                          #
                          default=pwd.getpwuid(os.getuid())[0],
                          action="store", 
                          metavar="USERNAME", 
                          help="If run as root, drop privileges to USER")
        parser.add_option("--builder", dest="builder_path", 
                          #
                          # By default
                          #
                          default='vc3-builder',  # by default, hope the builder is in the PATH
                          action="store", 
                          help="service bootstrapper")
        (self.options, self.args) = parser.parse_args()

        self.options.confFiles = self.options.confFiles.split(',')

    def __setuplogging(self):
        """ 
        Setup logging 
        """
        self.log = logging.getLogger()
        if self.options.logfile == "stdout":
            logStream = logging.StreamHandler()
        else:
            lf = os.path.expanduser(self.options.logfile)
            logdir = os.path.dirname(lf)
            if not os.path.exists(logdir):
                os.makedirs(logdir)
            runuid = pwd.getpwnam(self.options.runAs).pw_uid
            rungid = pwd.getpwnam(self.options.runAs).pw_gid                  
            os.chown(logdir, runuid, rungid)
            logStream = logging.FileHandler(filename=lf)    

        # Check python version 
        major, minor, release, st, num = sys.version_info
        if major == 2 and minor == 4:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d : %(message)s'
        else:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        formatter = logging.Formatter(FORMAT)
        formatter.converter = time.gmtime  # to convert timestamps to UTC
        logStream.setFormatter(formatter)
        self.log.addHandler(logStream)

        # adding a new Handler for the console, 
        # to be used only for DEBUG and INFO modes. 
        if self.options.logLevel in [logging.DEBUG, logging.INFO]:
            if self.options.console:
                console = logging.StreamHandler(sys.stdout)
                console.setFormatter(formatter)
                console.setLevel(self.options.logLevel)
                self.log.addHandler(console)
        self.log.setLevel(self.options.logLevel)
        self.log.info('Logging initialized at level %s.' % self.options.logLevel)


    def _printenv(self):

        envmsg = ''        
        for k in sorted(os.environ.keys()):
            envmsg += '\n%s=%s' %(k, os.environ[k])
        self.log.debug('Environment : %s' %envmsg)

    def __platforminfo(self):
        '''
        display basic info about the platform, for debugging purposes 
        '''
        self.log.info('platform: uname = %s %s %s %s %s %s' %platform.uname())
        self.log.info('platform: platform = %s' %platform.platform())
        self.log.info('platform: python version = %s' %platform.python_version())
        self._printenv()

    def __checkroot(self): 
        """
        If running as root, drop privileges to --runas' account.
        """
        starting_uid = os.getuid()
        starting_gid = os.getgid()
        starting_uid_name = pwd.getpwuid(starting_uid)[0]

        hostname = socket.gethostname()
        
        if os.getuid() != 0:
            self.log.info("Already running as unprivileged user %s at %s" % (starting_uid_name, hostname))
            
        if os.getuid() == 0:
            try:
                runuid = pwd.getpwnam(self.options.runAs).pw_uid
                rungid = pwd.getpwnam(self.options.runAs).pw_gid
                os.chown(self.options.logfile, runuid, rungid)
                
                os.setgid(rungid)
                os.setuid(runuid)
                os.seteuid(runuid)
                os.setegid(rungid)

                self._changehome()
                self._changewd()

                self.log.info("Now running as user %d:%d at %s..." % (runuid, rungid, hostname))
                self._printenv()

            
            except KeyError, e:
                self.log.error('No such user %s, unable run properly. Error: %s' % (self.options.runAs, e))
                sys.exit(1)
                
            except OSError, e:
                self.log.error('Could not set user or group id to %s:%s. Error: %s' % (runuid, rungid, e))
                sys.exit(1)

    def _changehome(self):
        '''
        Set environment HOME to user HOME.
        '''
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir 
        os.environ['HOME'] = runAs_home
        self.log.debug('Setting up environment variable HOME to %s' %runAs_home)


    def _changewd(self):
        '''
        changing working directory to the HOME directory of the new user,
        '''
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir
        os.chdir(runAs_home)
        self.log.debug('Switching working directory to %s' %runAs_home)


    def __createconfig(self):
        """Create config, add in options...
        """
        if self.options.confFiles != None:
            try:
                self.config = ConfigParser()
                self.config.read(self.options.confFiles)
            except Exception, e:
                self.log.error('Config failure')
                sys.exit(1)
        
        #self.config.set("global", "configfiles", self.options.confFiles)
           
    def run(self):
        """
        Create Daemon and enter main loop
        """

        try:
            self.log.info('Creating Daemon and entering main loop...')
            vc3m = VC3Master(self.config)
            vc3m.run()
            
        except KeyboardInterrupt:
            self.log.info('Caught keyboard interrupt - exitting')
            sys.exit(0)
        except ImportError, errorMsg:
            self.log.error('Failed to import necessary python module: %s' % errorMsg)
            sys.exit(1)
        except:
            self.log.error('''Unexpected exception!''')
            # The following line prints the exception to the logging module
            self.log.error(traceback.format_exc(None))
            print(traceback.format_exc(None))
            sys.exit(1)          

if __name__ == '__main__':
    mcli = VC3MasterCLI()
    mcli.run()

