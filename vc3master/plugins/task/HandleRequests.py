#!/usr/bin/env python

import ConfigParser
import StringIO
from base64 import b64encode
import socket

import os
import json
import math
from datetime import datetime

from vc3master.task import VC3Task
from vc3infoservice.infoclient import InfoConnectionFailure,InfoEntityMissingException

import pluginmanager as pm
import traceback

class HandleRequests(VC3Task):
    '''
    Plugin to manage the life cycle of all requests.
     
    '''

    def __init__(self, parent, config, section):
        super(HandleRequests, self).__init__(parent, config, section)
        self.client = parent.client
        self.log.debug("HandleRequests VC3Task initialized.")

    def runtask(self):
        self.log.info("Running task %s" % self.section)
        self.log.debug("Polling master....")

        try:
            requests = self.client.listRequests()
            n = len(requests) if requests else 0
            self.log.debug("Processing %d requests" % n)
            if requests:
                for r in requests:
                    try:
                        self.process_request(r)
                    except VC3InvalidRequest, e:
                        self.log.warning("Request %s is not valid. (%s)", r.name, e)
                        r.state = 'failure'
                        r.state_reason = 'Request invalid: ' + str(e)
                    except Exception, e:
                        self.log.warning("Request %s had a exception (%s)", r.name, e)
                        self.log.debug(traceback.format_exc(None))
                        r.state = 'failure'
                        r.state_reason = str(e)

                    try:
                        self.client.storeRequest(r)
                    except Exception, e:
                        self.log.warning("Storing the new request state failed. (%s)", e)
                        self.log.warning(traceback.format_exc(None))

        except InfoConnectionFailure, e:
            self.log.warning("Could not read requests from infoservice. (%s)", e)

    def process_request(self, request):
        next_state  = None
        reason      = None

        self.log.debug("Processing request '%s'", request.name)

        (next_state, reason) = (request.state, request.state_reason)

        headnode = self.getHeadNode(request)

        if not self.is_finishing_state(next_state):
            if self.request_has_expired(request):
                (next_state, reason) = ('terminating', 'virtual cluster has expired')
            elif headnode and headnode.state == 'failure':
                (next_state, reason) = ('failure', 'There was a failure with headnode: %s \nPlease terminate the virtual cluster.' % headnode.state_reason)

        if request.action and request.action == 'terminate':
            if not self.is_finishing_state(next_state) or request.state == 'failure':
                (next_state, reason) = ('terminating', 'received terminate action')

        nodesets           = self.getNodesets(request)
        request.statusinfo = self.compute_job_status_summary(request.statusraw, nodesets, next_state)

        if next_state == 'terminated':
            (next_state, reason) = self.state_terminated(request)

        if  next_state == 'new': 
            # nexts: initializing, terminating
            (next_state, reason) = self.state_new(request)

        if  next_state == 'initializing':
            # nexts: initializing, pending, terminating
            (next_state, reason) = self.state_initializing(request, headnode)

        if next_state == 'pending':
            # nexts: pending, running, terminating
            (next_state, reason) = self.state_pending(request)

        if next_state == 'running':
            # nexts: running, terminating
            (next_state, reason) = self.state_running(request)

        if next_state == 'terminating':
            # waits until everything has been cleanup
            (next_state, reason) = self.state_terminating(request)

        if next_state == 'cleanup':
            # waits until everything has been cleanup
            (next_state, reason) = self.state_cleanup(request)

        if next_state is not request.state or reason is not request.state_reason:
            self.log.debug("request '%s'  state '%s' -> %s (%s)'", request.name, request.state, next_state, str(reason))
            request.state = next_state
            request.state_reason = reason
        else:
            self.log.debug("request '%s' remained in state '%s'", request.name, request.state)

        if self.is_configuring_state(request.state):
            self.add_queues_conf(request, nodesets)
            self.add_auth_conf(request)
        else:
            request.queuesconf = None
            request.authconf = None

    def is_configuring_state(self, state):
        if self.is_initializing_state(state):
            return False

        if state == 'terminated':
            return False

        return True

    def is_initializing_state(self, state):
        return state in ['new', 'initializing']

    def is_finishing_state(self, state):
        return state in ['failure', 'terminating', 'cleanup', 'terminated']

    def request_is_valid(self, request):

        bad_reasons = []

        try:
            if not request.project:
                bad_reasons.append("Virtual cluster '%s' does not belong to any project." % request.name)
            else:
                try:
                    project = self.client.getProject(request.project)

                    if not (request.allocations and len(request.allocations) > 0):
                        bad_reasons.append("Virtual cluster did not define any allocations.")

                    if not (project.allocations and len(project.allocations) > 0):
                        bad_reasons.append("Project '%s' did not define any allocations." % (request.project, ))

                    if request.allocations and project.allocations:
                        for a in request.allocations:
                            if a not in project.allocations:
                                bad_reasons.append("Allocation '%s' does not belong to project '%s'." % (a, request.project))

                    if project.members and len(project.members) > 0:
                        if request.owner not in project.members:
                            bad_reasons.append("User '%s' that created the virtual cluster does not belong to project '%s'." % (request.owner, request.project))
                        for member_name in project.members:
                            try:
                                member = self.client.getUser(member_name)
                                if not member.sshpubstring:
                                    bad_reasons.append("User '%s' in project '%s' does not have a ssh-key." % (member_name, request.project))
                                elif not self.client.validate_ssh_pub_key(member.sshpubstring):
                                    bad_reasons.append("User '%s' in project '%s' has an invalid ssh-key." % (member_name, request.project))
                            except InfoEntityMissingException:
                                bad_reasons.append("User '%s' in project '%s' is not defined." % (member_name, request.project))
                    else:
                        bad_reasons.append("Project '%s' did not define any members." % (request.project,))
                except InfoEntityMissingException:
                    bad_reasons.append("Project '%s' is not defined." % (request.project,))
        except Exception, e:
            bad_reasons.append("There was an error while validating virtual cluster request: %s." % (e,))

        if not bad_reasons:
            # fill-in the desired headnode name. This will eventually come from
            # the nodesets when the request is first created.
            request.headnode = 'headnode-for-' + request.name
            return True
        else:
            raise VC3InvalidRequest(' '.join(bad_reasons))

    def state_new(self, request):
        '''
        Validates all new requests. 
        '''
        self.log.debug('processing new request %s' % request.name)

        try:
            if self.request_is_valid(request):
                return ('initializing', 'Waiting for cluster components to come online.')
        except VC3InvalidRequest, e:
            self.log.warning("Invalid Request: %s" % str(e))
            return ('failure', 'Please terminate the cluster.\nInvalid virtual cluster specification:\n%s' % str(e))

    def state_initializing(self, request, headnode):
        if not headnode:
            self.log.debug('waiting for headnode to come online for %s' % request.name)
            return ('initializing', 'Headnode is being created.')
        if headnode.state == 'running':
            return ('pending', 'Requesting compute workers.')
        if headnode.state == 'failure':
            return ('failure', 'Error while initializing the headnode: %s' % headnode.state_reason)

        # in any other case, use the state from the headnode:
        return ('initializing', headnode.state_reason)


    def state_pending(self, request):
        self.log.debug('waiting for factory to start fulfilling request %s' % request.name)

        running = self.job_count_with_state(request, 'running')
        idle    = self.job_count_with_state(request, 'idle')
        err     = self.job_count_with_state(request, 'error')

        if (running is None) or (idle is None) or (err is None):
            return ('pending', 'Requesting compute workers')
        elif running > 0:
            return ('running', 'Growing virtual cluster.')
        else:
            return ('pending', 'Waiting for compute workers to start running.')

    def state_running(self, request):
        total_of_nodes = self.total_jobs_requested(request)
        running        = self.job_count_with_state(request, 'running')
        idle           = self.job_count_with_state(request, 'idle')

        if (running is None) or (idle is None):
            self.log.warning('Failure: status of request %s went away.' % request.name)
            return ('terminating', 'Failure: status of virtual cluster %s went away.' % request.name)
        elif total_of_nodes > (running + idle):
            return ('running', 'Requesting %d more compute worker(s).' % (total_of_nodes - running))
        elif total_of_nodes < (running + idle):
            return ('running', 'Requesting %d less compute worker(s).' % (running - total_of_nodes))
        elif total_of_nodes == running:
            return ('running', 'All requested compute workers are running.')
        else:
            return ('running', 'Waiting for %d queued compute workers.' % (idle,))

    def state_terminating(self, request):
        self.log.debug('request %s is terminating' % request.name)
        running = self.job_count_with_state(request, 'running')
        idle    = self.job_count_with_state(request, 'idle')

        if (running is None) or (idle is None):
            self.log.warning('Failure: status of request %s went away.' % request.name)
            return ('cleanup', 'Failure: status of virtual cluster %s went away.' % request.name)
        elif (running + idle) == 0:
            return ('cleanup', 'All compute workers have been terminated.')
        elif request.action == 'terminate':
            return ('terminating', 'terminate action is being processed.')
        else:
            return ('terminating', 'Terminating all compute workers.')


    def state_cleanup(self, request):
        self.log.debug('collecting garbage for request %s' % request.name)

        # to fill cleanup here!
        if self.is_everything_cleaned_up(request):
            return ('terminated', 'Virtual cluster terminated succesfully')

        return ('cleanup', 'Waiting for headnode/others to terminate')


    def state_terminated(self, request):
        if request.action and request.action == 'relaunch':
            request.action = 'new'
            return ('new', 'relaunching cluster')
        else:
            self.log.debug('request %s is done' % request.name)
            return ('terminated', 'Virtual cluster terminated succesfully')


    def add_queues_conf(self, request, nodesets):
        '''
            request.allocations = [ alloc1, alloc2 ]
                   .cluster.nodesets = [ nodeset1, nodeset2 ]                                       
             nodeset.node_number   # total number to launch. 
        '''
        config = ConfigParser.RawConfigParser()

        try:
            for allocation_name in request.allocations:
                self.generate_queues_section(config, request, nodesets, allocation_name)

            conf_as_string = StringIO.StringIO()
            config.write(conf_as_string)

            request.queuesconf = b64encode(conf_as_string.getvalue())
            return request.queuesconf
        except Exception, e:
            self.log.error('Failure to generate queuesconf: %s', e)
            self.log.debug(traceback.format_exc(None))
            request.queuesconf = None
            raise e

    def add_auth_conf(self, request):
        config = ConfigParser.RawConfigParser()

        try:
            for allocation_name in request.allocations:
                self.generate_auth_section(config, request, allocation_name)

            conf_as_string = StringIO.StringIO()
            config.write(conf_as_string)

            request.authconf = b64encode(conf_as_string.getvalue())
            return request.authconf
        except Exception, e:
            self.log.error('Failure generating auth.conf: %s', e)
            request.authconf = None
            return None

    def generate_queues_section(self, config, request, nodesets, allocation_name):
        allocation = self.client.getAllocation(allocation_name)
        if not allocation:
            raise VC3InvalidRequest("Allocation '%s' has not been declared." % allocation_name, request = request)

        resource = self.client.getResource(allocation.resource)
        if not resource:
            raise VC3InvalidRequest("Resource '%s' has not been declared." % allocation.resource, request = request)

        resource_nodesize = self.client.getNodeinfo(resource.nodeinfo)
        if not resource_nodesize:
            raise VC3InvalidRequest("Resource node size '%s' has not been declared." % resource.nodeinfo, request = request)
        
        for nodeset in nodesets:
            self.add_nodeset_to_queuesconf(config, request, resource, resource_nodesize, allocation, nodeset)

    def __get_ip(self, request):
        try:
            server = self.nova.servers.find(name=self.vm_name(request))

            if server.status != 'ACTIVE':
                self.log.debug("Headnode for request %s is not active yet.", request.name)
                return None

        except Exception, e:
            self.log.warning('Could not find headnode for request %s (%s)', request.name, e)
            return None

        try:
            for network in server.networks.keys():
                for ip in server.networks[network]:
                    if re.match('\d+\.\d+\.\d+\.\d+', ip):
                        return ip
        except Exception, e:
            self.log.warning("Could not find ip for request %s: %s", request.name, e)
            raise e

        return None


    def add_nodeset_to_queuesconf(self, config, request, resource, resource_nodesize, allocation, nodeset):
        node_number  = self.jobs_to_run_by_policy(request, allocation, nodeset)
        section_name = request.name + '.' + nodeset.name + '.' + allocation.name

        self.log.debug("Information finalized for queues configuration section [%s]. Creating config." % section_name)

        config.add_section(section_name)

        cores  = (resource_nodesize and resource_nodesize.cores)      or 1
        disk   = (resource_nodesize and resource_nodesize.storage_mb) or 1024
        memory_per_core = (resource_nodesize and resource_nodesize.memory_mb)  or 1024
    
        

        if resource.accesstype == 'batch':
            config.set(section_name, 'batchsubmitplugin',           'CondorSSHRemoteManager')
            config.set(section_name, 'batchsubmit.condorsshremotemanager.user',  allocation.accountname)
            config.set(section_name, 'batchsubmit.condorsshremotemanager.batch', resource.accessflavor)
            config.set(section_name, 'batchsubmit.condorsshremotemanager.method', resource.accessmethod)
            config.set(section_name, 'batchsubmit.condorsshremotemanager.host',  resource.accesshost)
            config.set(section_name, 'batchsubmit.condorsshremotemanager.port',  str(resource.accessport))
            config.set(section_name, 'batchsubmit.condorsshremotemanager.authprofile', allocation.name)

            config.set(section_name, 'batchsubmit.condorsshremotemanager.condor_attributes.+Nonessential', 'True')
            # CMS Connect resources work with singularity CMS images. Only RHEL7 is supported on spark clusters for now.
            if resource.name == 'cms-connect' and (nodeset.app_type in ['spark', 'jupyter+spark']):
                config.set(section_name, 'batchsubmit.condorsshremotemanager.condor_attributes.+remote_cerequirements', 'required_os=="rhel7"')
            config.set(section_name, 'batchsubmit.condorsshremotemanager.condor_attributes.request_cpus',   cores)
            config.set(section_name, 'batchsubmit.condorsshremotemanager.condor_attributes.request_disk',   disk   * 1024)
            config.set(section_name, 'batchsubmit.condorsshremotemanager.condor_attributes.request_memory', memory_per_core * cores)
            # Workaround to (apparently) a bosco bug dealing with multicore
            config.set(section_name, 'batchsubmit.condorsshremotemanager.condor_attributes.+remote_nodenumber', cores)
            config.set(section_name, 'batchsubmit.condorsshremotemanager.condor_attributes.+remote_SMPGranularity', cores)

            config.set(section_name, 'executable',                  '%(builder)s')

            # for now, remove all jobs non-peacefully:
            config.set(section_name, 'batchsubmit.condorsshremotemanager.overlay.peaceful', 'no')
            # if nodeset.app_peaceful is not None:
            #     if nodeset.app_peaceful:
            #         config.set(section_name, 'batchsubmit.condorsshremotemanager.overlay.peaceful', 'yes')
            #     else:
            #         config.set(section_name, 'batchsubmit.condorsshremotemanager.overlay.peaceful', 'no')

            if nodeset.app_killorder is not None:
                    config.set(section_name, 'batchsubmit.condorsshremotemanager.overlay.killorder', nodeset.app_killorder)

            if nodeset.app_role == 'worker-nodes':
                try:
                    headnode = self.client.getNodeset(request.headnode)
                    config.set(section_name, 'shared_secret_file', request.name + 'secret')
                    config.set(section_name, 'shared_secret', headnode.app_sectoken)
                except Exception, e:
                    self.log.warning("Could not get headnode shared secret for request '%s'. Continuing without password (this probably won't work).", request.name )

                # Use dynamic resizing.
                # This is currently broken.
                # configure APF to resize the VC based on the # of jobs in queue
                # scalefactor = 1 / float(len(request.allocations))

                # config.set(section_name, 'wmsstatusplugin', 'Condor')
                # config.set(section_name, 'wmsqueue', 'ANY')
                # config.set(section_name, 'wmsstatus.condor.scheddhost', headnode.app_host)
                # config.set(section_name, 'wmsstatus.condor.collectorhost', headnode.app_host)
                # config.set(section_name, 'schedplugin', 'Ready, Scale, KeepNRunning, MaxToRun')
                # config.set(section_name, 'sched.scale.factor', scalefactor)
                # config.set(section_name, 'sched.maxtorun.maximum', node_number)

                # Use static size
                config.set(section_name, 'schedplugin', 'KeepNRunning')
                config.set(section_name, 'sched.keepnrunning.keep_running', node_number)


        elif resource.accesstype == 'cloud':
            config.set(section_name, 'batchsubmitplugin',          'CondorEC2')
        elif resource.accesstype == 'local':
            config.set(section_name, 'batchsubmitplugin',          'CondorLocal')
            config.set(section_name, 'batchsubmit.condorlocal.condor_attributes.should_transfer_files', 'YES')
            config.set(section_name, 'batchsubmit.condorlocal.condor_attributes.initialdir', '$ENV(TMP)')

            config.set(section_name, 'batchsubmit.condorlocal.condor_attributes.request_cpus',   cores)
            config.set(section_name, 'batchsubmit.condorlocal.condor_attributes.request_disk',   disk   * 1024)
            config.set(section_name, 'batchsubmit.condorlocal.condor_attributes.request_memory', memory_per_core * cores)
            # Workaround to (apparently) a bosco bug dealing with multicore
            config.set(section_name, 'batchsubmit.condorlocal.condor_attributes.+remote_nodenumber',   cores)

            config.set(section_name, 'executable',                 '%(builder)s')
        else:
            raise VC3InvalidRequest("Unknown resource access type '%s'" % str(resource.accesstype), request = request)

        self.add_environment_to_queuesconf(config, request, section_name, nodeset, resource, resource_nodesize)

        self.log.debug("Completed filling in config for allocation %s" % allocation.name)


    def jobs_to_run_by_policy(self, request, allocation, nodeset):
        self.log.debug("Calculating nodes to run...")
        node_number = 0

        if self.is_finishing_state(request.state):
            node_number = 0
            self.log.debug("Request in finishing state. Setting keepnrunning to 0")
        else:
            # For now no policies. Just calculated static-balanced 
            node_number = self.jobs_to_run_by_static_balanced(request, allocation, nodeset)
        return node_number
    
    def jobs_to_run_by_static_balanced(self, request, allocation, nodeset):
        numalloc     = len(request.allocations)
        total_to_run = self.total_jobs_requested(request)
        raw          = int(math.floor(float(total_to_run) / numalloc))
        total_raw    = raw * numalloc

        # since using floor, it is always the case that total_raw <= total_to_run
        # we compensate for any difference in the allocation last in the list
        node_number = raw
        diff        = total_to_run - total_raw

        if allocation.name == request.allocations[-1]:
            node_number += diff

        self.log.debug("With %d allocations and nodeset.node_number %d this allocation should run %d" % (numalloc, total_to_run, node_number))
        return node_number

    def generate_auth_tokens(self, principle):
        """ 
        Generate SSH priv/pub keys and base64 encode them
        """ 
        self.log.info("Generating or retrieving SSH keys for %s", principle)
        self.ssh = self.parent.parent.ssh
        (pub, priv) = self.ssh.getkeys(principle)
        self.log.debug("public key: %s", pub)
        encoded_pub = b64encode(pub)
        encoded_priv = b64encode(priv)
        return encoded_pub, encoded_priv
         

    def generate_auth_section(self, config, request, allocation_name):

        name = allocation_name
        config.add_section(name)

        allocation = self.client.getAllocation(allocation_name)
        if not allocation:
            raise VC3InvalidRequest("Allocation '%s' has not been declared." % allocation_name, request = request)

        if allocation.privtoken is None:
            raise VC3InvalidRequest("Allocation '%s' doesn't have priv token." % allocation_name, request = request)

        resource = self.client.getResource(allocation.resource)
        if not resource:
            raise VC3InvalidRequest("Resource '%s' has not been declared." % allocation.resource, request = request)

        if resource.accessmethod == 'ssh':
            # credible always generates rsa keys
            #allocation.sectype = 'ssh-rsa'
            #(allocation.pubtoken, allocation.privtoken) = self.generate_auth_tokens(name)
            config.set(name, 'plugin',   'SSH')
            config.set(name, 'ssh.type',  allocation.sectype)
            config.set(name, 'ssh.publickey', allocation.pubtoken)
            config.set(name, 'ssh.privatekey', allocation.privtoken)
        elif resource.accessmethod == 'gsissh':
            config.set(name, 'plugin',   'GSISSH')
            config.set(name, 'ssh.privatekey', allocation.privtoken)
        elif resource.accessmethod == 'local':
            # nothing special is needed
            config.set(name, 'plugin',        'Noop')
        else:
            raise VC3InvalidRequest("Unknown resource access method '%s'" % str(resource.accessmethod), request = request)


    def add_pilot_to_queuesconf(self, config, request, section_name, nodeset, resource, nodesize):

        try:
            headnode  = self.client.getNodeset(request.headnode)
        except Exception, e:
            self.log.warning("Could not find headnode for request '%s'.")
            raise e

        s = ''
        if nodeset.app_type in ['htcondor', 'jupyter+htcondor', 'reana+htcondor']:
            collector = headnode.app_host

            s += ' --require vc3-glidein'
            s += ' -- vc3-glidein --vc3-env VC3_SH_PROFILE_ENV'
            s += ' -c %s -C %s -p %s -t -D %d -m %d --disk %d' % (collector, collector, '%(shared_secret_file)s', nodesize.cores, nodesize.memory_mb * nodesize.cores, nodesize.storage_mb * 1024)

            if nodeset.app_lingertime:
                s += ' --lingertime %d' % (nodeset.app_lingertime, )

        elif nodeset.app_type == 'workqueue':
            s += ' --require cctools'
            s += ' -- work_queue_worker -M %s -dall -t %d --cores %d --memory %d --disk %d --password %s' % (request.name, 60*60*2, nodesize.cores, nodesize.memory_mb * nodesize.cores, nodesize.storage_mb, '%(shared_secret_file)s')
            if nodeset.app_lingertime:
                s += ' --timeout %d' % (nodeset.app_lingertime, )
        elif nodeset.app_type in ['spark', 'jupyter+spark']:
            sparkmaster = 'spark://' + headnode.app_host + ':7077'
            # Workaround to python-dev issue with singularity CMS images.
            if resource.name == 'cms-connect':
                s += '--no-sys python'
            s += ' --require spark-xrootd'
            s += ' --var SPARK_NO_DAEMONIZE=1'
            s += ' -- start-slave.sh %s --properties-file %s -c %d -m %dM' % (sparkmaster, '%(shared_secret_file)s', nodesize.cores, nodesize.memory_mb * nodesize.cores)
        else:
            raise VC3InvalidRequest("Unknown nodeset app_type: '%s'" % nodeset.app_type)

        return s


    def add_environment_to_queuesconf(self, config, request, section_name, nodeset, resource, resource_nodesize):
        #s  = " --revar 'VC3_.*'"
        s  = '" ' # trying to quote the thing
        s += ' --home=.'
        s += ' --install=.'
        s += ' --bosco-workaround'
        # Parse SCRATCH, pretty common in HPC resources.
        s += " --revar '^SCRATCH$'"

        #e.g. FACTORY_JOBID=apf.virtualclusters.org#53406.6
        factory_jobid = "$ENV(HOSTNAME)" + '#$(Cluster).$(Process)'
        if nodeset.app_type in ['htcondor', 'jupyter+htcondor']:
            s += ' --var _CONDOR_FACTORY_JOBID=' + factory_jobid # in the Condor/Jupyterhub case we also make sure its a Condor classad

            # Leaving STARTD_ATTRS out for now, while fixing quoting.
            #s += ' --var _CONDOR_STARTD_ATTRS=""$(STARTD_ATTRS) FACTORY_JOBID""'

            s += ' --var FACTORY_JOBID=' + factory_jobid
        else:
            # otherwise we just put the factory jobid into the environment. other middlewares might be able to use it too
            s += ' --var FACTORY_JOBID=' + factory_jobid
        envs = []

        if request.environments is not None:
            envs.extend(request.environments)

        if nodeset.environment is not None:
            envs.append(nodeset.environment)

        for env_name in envs:
            environment = self.client.getEnvironment(env_name)
            if environment is None:
                raise VC3InvalidRequest("Unknown environment '%s' for '%s'" % (env_name, section_name), request = request)

            vs    = [ "VC3_REQUESTID=%s" % request.name, "VC3_QUEUE=%s" % section_name]
            for k in environment.envmap:
                vs.append("%s=%s" % (k, environment.envmap[k]))

            if environment.required_os is not None:
                os = '--require-os %s' %environment.required_os
                s += ' ' + os

            reqs  = ' '.join(['--require %s' % x for x in environment.packagelist])

            if nodeset.app_type in ['reana+htcondor']:
                reqs += ' --require user-profile-environment'  
            
            vars  = ' '.join(['--var %s' % x for x in vs])

            s += ' ' + vars + ' ' + reqs

            if environment.builder_extra_args:
                s += ' ' + ' '.join(environment.builder_extra_args)

            if environment.command:
                self.log.warning('Ignoring command of environment %s for %s. Adding pilot for %s instead' % (environment.name, section_name, nodeset.name))

        if len(envs) > 0:
            config.set(section_name, 'vc3.environments', ','.join(envs))

        s += ' ' + self.add_pilot_to_queuesconf(config, request, section_name, nodeset, resource, resource_nodesize)

        # add another quote at the end
        s += '"'

        config.set(section_name, 'executable.arguments', s)

    def is_everything_cleaned_up(self, request):
        headnode = None
        if request.headnode:
            try:
                headnode = self.client.getNodeset(request.headnode)
            except InfoConnectionFailure:
                # We don't know if headnode has been cleared or not...
                return False
            except InfoEntityMissingException:
                # Headnode is missing, and that's what we want
                pass

        if headnode:
            return False
        # if somethingElseStillThere:
        #    return False

        # everything has been cleaned up:
        return True

    def job_count_with_state(self, request, state):
        if not request.statusraw:
            return None

        if not request.statusinfo:
            return None

        if not self.is_configuring_state(request.state):
            return None

        at_least_one_nodeset = False
        count = 0

        for nodeset in request.statusinfo.keys():
            count += request.statusinfo[nodeset].get(state, 0)
            at_least_one_nodeset = True

        if at_least_one_nodeset:
            self.log.debug('Counting %d jobs in state %s for request %s', count, state, request.name)
            return count

        self.log.debug('No nodesets with jobs in state %s for request %s', state, request.name)
        return None


    def total_jobs_requested(self, request):
        if self.is_finishing_state(request.state):
            return 0

        cluster = self.client.getCluster(request.cluster)

        if len(cluster.nodesets) < 1:
            raise VC3InvalidRequest("No nodesets have been added to Cluster '%s' " % cluster.name, request = request)

        total_jobs = 0
        for nodeset_name in cluster.nodesets:
            nodeset = self.client.getNodeset(nodeset_name)
            if not nodeset:
                raise VC3InvalidRequest("Nodeset '%s' has not been declared." % nodeset_name, request = request)
            if nodeset.node_number is not None:
                total_jobs += nodeset.node_number
        return total_jobs

    def compute_job_status_summary(self, statusraw, nodesets, next_state):
        if not statusraw:
            return None

        if self.is_initializing_state(next_state):
            return None

        statusinfo = {}
        for nodeset in nodesets:
            statusinfo[nodeset.name]                 = {}
            statusinfo[nodeset.name]['running']      = 0
            statusinfo[nodeset.name]['idle']         = 0
            statusinfo[nodeset.name]['error']       = 0
            statusinfo[nodeset.name]['node_number'] = nodeset.node_number

            if self.is_finishing_state(next_state):
                statusinfo[nodeset.name]['requested'] = 0
            else:
                statusinfo[nodeset.name]['requested'] = statusinfo[nodeset.name]['node_number']

            try:
                for factory in statusraw.keys():
                    for allocation in statusraw[factory][nodeset.name].keys():
                        try:
                            for field in ['running', 'idle', 'error']:
                                statusinfo[nodeset.name][field] += statusraw[factory][nodeset.name][allocation]['aggregated'][field]
                        except KeyError, e:
                            pass
            except KeyError, e:
                pass

        return statusinfo

    def getHeadNode(self, request):
        headnode = None
        try:
            headnode = self.client.getNodeset(request.headnode)
        except InfoConnectionFailure:
            pass
        except InfoEntityMissingException:
            pass
        return headnode

    def getNodesets(self, request):
        cluster = self.client.getCluster(request.cluster)
        if not cluster:
            raise VC3InvalidRequest("Cluster '%s' has not been declared." % cluster.name, request = request)

        if len(cluster.nodesets) < 1:
            raise VC3InvalidRequest("No nodesets have been added to Cluster '%s' " % cluster.name, request = request)

        nodesets = []
        for nodeset_name in cluster.nodesets:
            self.log.debug("retrieving nodeset %s for cluster %s " % (nodeset_name, cluster.name))
            nodeset = self.client.getNodeset(nodeset_name)
            if not nodeset:
                raise VC3InvalidRequest("Nodeset '%s' has not been declared." % nodeset_name, request = request)
            self.log.debug("Retrieved %s for name %s" % ( nodeset, nodeset_name))
            nodesets.append(nodeset)
        return nodesets

    def request_has_expired(self, request):
        if request.expiration is None:
            return False
        limit = datetime.strptime(request.expiration, '%Y-%m-%dT%H:%M:%S')

        if limit < datetime.utcnow().replace(microsecond=0):
            self.log.debug("Request %s has expired.", request.name)
            return True

        return False

class VC3InvalidRequest(Exception):
    def __init__(self, reason, request = None):
        self.reason  = reason
        self.request = request

    def __str__(self):
        return str(self.reason)

