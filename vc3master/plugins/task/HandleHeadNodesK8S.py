#!/usr/bin/env python

from vc3master.task import VC3Task
from vc3infoservice.infoclient import InfoConnectionFailure, InfoEntityMissingException

from base64 import b64encode
import pluginmanager as pm
import traceback

import json
import os
import sys
import re
import subprocess
import time
import pprint
import subprocess, yaml

from kubernetes import client, config, utils
import kubernetes.client 
from kubernetes.client.rest import ApiException
from jinja2 import Environment, FileSystemLoader


class HandleHeadNodes(VC3Task):
    '''
    Plugin to manage the head nodes lifetime.
    '''

    def __init__(self, parent, config, section):
        super(HandleHeadNodes, self).__init__(parent, config, section)
        self.client = parent.client
        self.config = config 
        self.node_prefix           = self.config.get(section, 'node_prefix')
        self.node_image            = self.config.get(section, 'node_image')
        self.node_flavor           = self.config.get(section, 'node_flavor')
        self.node_user             = self.config.get(section, 'node_user')
        self.node_network_id       = self.config.get(section, 'node_network_id')
        self.node_private_key_file = os.path.expanduser(self.config.get(section, 'node_private_key_file'))
        self.node_public_key_name  = self.config.get(section, 'node_public_key_name')

        self.node_max_no_contact_time    = int(self.config.get(section, 'node_max_no_contact_time'))
        self.node_max_initializing_count = int(self.config.get(section, 'node_max_initializing_count'))

        groups = self.config.get(section, 'node_security_groups')
        self.node_security_groups = groups.split(',')

        self.initializers = {}
        # keep las succesful contact to node, to check against node_max_no_contact_time.
        self.last_contact_times = {}

        # number of times we have tries to initialize a node. After node_max_initializing_count, declare failure.
        self.initializing_count = {}
	self.kubernetes_config = self.config.get(section, 'kube_config')
        self.log.debug("HandleHeadNodes VC3Task initialized.")

    global login_info
    def login_info(self, request):
    	"""Finds IP and port for ssh login
	Args: request 
	Returns: IP and port as integers 
	"""
        config.load_kube_config(config_file = self.kubernetes_config)
        v1 = client.CoreV1Api()
        k8s_client = client.ApiClient()
        k8s_api = client.ExtensionsV1beta1Api(k8s_client)
        configuration = kubernetes.client.Configuration()
        api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))
        try:
            service = v1.read_namespaced_service(name = "login-node-service-" + str(request.name), namespace = str(request.name))
            port = service.spec.ports[0].node_port
            list_pods = v1.list_namespaced_pod(str(request.name))
            pod = list_pods.items[0]
            node = v1.read_node(pod.spec.node_name)
            IP = node.status.addresses[0].address
            self.log.info('About to return')
            self.log.info(IP)
            self.log.info(port)
            return [IP, port]
        except Exception:
            self.log.info("Login pod does not exist")
            return None

    def template(self):
	"""Templating for the config files
	Args: None
	Returns: temp_up, temp_up2, temp_up3, temp_up4, temp_up5 as rendered files
	"""
	config_data = yaml.load(open('/usr/lib/python2.7/site-packages/vc3master/plugins/task/vals.yaml'),Loader=yaml.FullLoader)
	env = Environment(loader = FileSystemLoader('./templates'), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template('condor_config.local.j2')
        temp_up = template.render(config_data)
        config_data2 = yaml.load(open('/etc/cvmfs_vals.yaml'),Loader=yaml.FullLoader)
        template2 = env.get_template('cvmfs_default_local.j2')
        temp_up2 = template2.render(config_data2)
        config_data3 = yaml.load(open('/etc/minio'),Loader=yaml.FullLoader)
        template3 = env.get_template('minio.env')
        temp_up3 = template3.render(config_data3)
        config_data4 = yaml.load(open('/etc/minio'),Loader=yaml.FullLoader)
        template4 = env.get_template('core-site.xml.j2')
        temp_up4 = template4.render(config_data4)
        config_data5 = yaml.load(open('/etc/spark'),Loader=yaml.FullLoader)
        template5 = env.get_template('spark.env')
        temp_up5 = template5.render(config_data5)
	return temp_up, temp_up2, temp_up3, temp_up4, temp_up5

    def login_create(self, request):
	""" Creates the deployment, service, and two config maps (one for adding users, the other for adding the config files)
	Args: request
	Returns: None  
	"""
	self.log.info('Starting login_create')
	config.load_kube_config(config_file = self.kubernetes_config)
        v1 = client.CoreV1Api()
        k8s_client = client.ApiClient()
        k8s_api = client.ExtensionsV1beta1Api(k8s_client)
        configuration = kubernetes.client.Configuration()
        api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))
        self.create_namespace(request)
	self.add_keys_to_pod(request)
        try:
            # checks if deployment, service, configmap already created - To do add checks for service + configmaps
            check = k8s_api.read_namespaced_deployment_status(name= "login-node-n-" + str(request.name), namespace =str(request.name))
            self.log.info("pod already exists")
        except Exception:
            # rendering template and creating configmap to mount config files
 	    temp_up, temp_up2, temp_up3, temp_up4, temp_up5 = self.template()
            name = 'temcon-' + str(request.name)
            namespace = str(request.name)
            body = kubernetes.client.V1ConfigMap()
            body.data = {"condor_config.local":str(temp_up),"cvmfs_default_local":str(temp_up2),"minio":str(temp_up3),"core-site.xml":str(temp_up4),"spark":str(temp_up5)}
            body.metadata = kubernetes.client.V1ObjectMeta()
            body.metadata.name = name
            configuration = kubernetes.client.Configuration()
            api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration)) 
            try:
            	api_response = api_instance.create_namespaced_config_map(namespace, body)
            except ApiException as e:
            	print("Exception when calling CoreV1Api->create_namespaced_config_map: %s\n" % e)
	    self.log.info('CREATING DEPLOYMENT')
	    self.create_dep(request)
	    self.create_service(request)
	    self.create_conf_users(request)
	return 
	
    def create_namespace(self, request):
	"""Creates namespace with request name 
	Args: request
	Returns: None 
	"""
	config.load_kube_config(config_file = self.kubernetes_config)
        pp = pprint.PrettyPrinter(indent =4)
        configuration = kubernetes.client.Configuration()
	api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))
	body = kubernetes.client.V1Namespace(api_version = 'v1', kind = 'Namespace', metadata = kubernetes.client.V1ObjectMeta(name = str(request.name))) 
	try: 
    	    api_response = api_instance.create_namespace(body)
	except ApiException as e:
    	    print("Exception when calling CoreV1Api->create_namespace: %s\n" % e)

    def create_dep(self, request):
	"""Creates deployment with request name
	Args: request 
	Returns: None
	"""
	config.load_kube_config(config_file = self.kubernetes_config)
        pp = pprint.PrettyPrinter(indent =4)
        configuration = kubernetes.client.Configuration()
        api_instance = kubernetes.client.AppsV1Api(kubernetes.client.ApiClient(configuration))
        core_v1_api = client.CoreV1Api()
        namespace = 'default'
        body = kubernetes.client.V1Deployment() 
        body.metadata = kubernetes.client.V1ObjectMeta()
        body.metadata.name = 'login-node-n-' + str(request.name)
        body.metadata.labels = {'app':'login-node-n-' + str(request.name)}
        conf_list = []
        conf_list.append(kubernetes.client.V1VolumeMount(name = 'config-vol', mount_path = '/root/tconfig-file.conf',sub_path = 'tconfig-file.conf'))
        conf2_list = []
        volume0 = kubernetes.client.V1Volume(name = 'config-vol', config_map = kubernetes.client.V1ConfigMapVolumeSource(name = 'new-config-' + str(request.name), items = [kubernetes.client.V1KeyToPath(key = "tconfig-file.conf", path = "tconfig-file.conf")]))
        volume1 = kubernetes.client.V1Volume(name = 'temcon-vol', config_map = kubernetes.client.V1ConfigMapVolumeSource(name = 'temcon-' + str(request.name), items = [kubernetes.client.V1KeyToPath(key = "condor_config.local", path = "condor_config.local")]))
        env_list = []
        env_list.append(kubernetes.client.V1EnvVar(name = 'PASSWDFILE', value = "root/tconfig-file.conf"))
        vol_m_list = []
        vol_m_list.append(kubernetes.client.V1VolumeMount(name = 'config-vol', mount_path = '/root/tconfig-file.conf',sub_path = 'tconfig-file.conf'))
        vol_m_list.append(kubernetes.client.V1VolumeMount(name = 'temcon-vol', mount_path = '/etc/condor/config.d/condor_config.local', sub_path = 'condor_config.local'))
        container0 = kubernetes.client.V1Container(name = 'new-container', env = env_list, image = 'nlingareddy/condor-login', volume_mounts = vol_m_list)
        vol_list = []
        vol_list.append(volume0)
        vol_list.append(volume1)
        cont_list = []
        cont_list.append(container0)
        body.spec = kubernetes.client.V1DeploymentSpec(replicas= 1, selector= kubernetes.client.V1LabelSelector(match_labels= {'app':'login-node-n-' + str(request.name)}) , template= kubernetes.client.V1PodTemplateSpec(metadata = kubernetes.client.V1ObjectMeta(labels = {'app':'login-node-n-' + str(request.name)}), spec = kubernetes.client.V1PodSpec(volumes= vol_list, containers = cont_list)))
 	try:
            api_response = api_instance.create_namespaced_deployment(namespace=str(request.name), body=body)
        except ApiException as e:
            print("Exception when calling AppsV1Api->create_namespaced_deployment: %s\n" % e)

    def create_service(self, request):
	"""Creates service with request name
	Args: request 
	Returns: None
	"""
	config.load_kube_config(config_file = self.kubernetes_config)
        core_v1_api = kubernetes.client.CoreV1Api()
        serv_list = []
        serv_list.append(kubernetes.client.V1ServicePort(port=22, protocol='TCP'))
        body = kubernetes.client.V1Service(metadata= kubernetes.client.V1ObjectMeta(name = 'login-node-service-' + str(request.name)), spec = kubernetes.client.V1ServiceSpec(type='NodePort', selector={'app':'login-node-n-' + str(request.name)}, ports=serv_list))
        try:
            api_response = core_v1_api.create_namespaced_service(namespace=str(request.name), body=body)
        except ApiException as e:
            print("Exception when calling AppsV1Api->create_namespaced_service: %s\n" % e)	 

    def create_conf_users(self, request):
	"""Creates config map for users with request name
	Args: request 
	Returns: None
	"""
	config.load_kube_config(config_file = self.kubernetes_config)
        core_v1_api = kubernetes.client.CoreV1Api()
	string_to_append = self.add_keys_to_pod(request)
        bodyc = kubernetes.client.V1ConfigMap(api_version = 'v1', kind = 'ConfigMap', metadata = kubernetes.client.V1ObjectMeta(name = "new-config-" + str(request.name), namespace = str(request.name)), data = {'tconfig-file.conf':'\n' +string_to_append})
        try:
            api_response = core_v1_api.create_namespaced_config_map(namespace=str(request.name), body = bodyc)
        except ApiException as e:
            print("Exception when calling CoreV1Api->create_namespaced_config_map: %s\n" % e)

    def login_pending(self, request): 
	"""Waits until pod finishes building 
	Args: request 
	Returns: None 
	"""
	config.load_kube_config(config_file = self.kubernetes_config)
        k8s_client = client.ApiClient()
        k8s_api = client.ExtensionsV1beta1Api(k8s_client)
        configuration = kubernetes.client.Configuration()
	deps = k8s_api.read_namespaced_deployment_status(name= "login-node-n-" + str(request.name), namespace =str(request.name))
	while(deps.status.available_replicas != 1):
            k8s_api = client.ExtensionsV1beta1Api(k8s_client)
            deps = k8s_api.read_namespaced_deployment_status(name= "login-node-n-" + str(request.name), namespace =str(request.name))
        self.log.info("LOGIN POD CREATED")
        deps.metadata.name = deps.metadata.name + "-" + request.name
        return login_info(self, request)
    
    def add_keys_to_pod(self, request):
	"""Returns string to append to config map that adds users
	Args: request
	Returns: None
	"""
        members    = self.get_members_names(request)
	attributes = {}
	i = 1000
        for member in members:
            try:
                user = self.client.getUser(member)
            except Exception, e:
                self.log.warning("Could not find user: %s", member)
                raise e
	    string_to_append = str(user.name) +':x:' + str(i) + ':' + str(i) + ':'+ '/home/' + str(user.name) + '::' + '/bin/bash:' + str(user.sshpubstring) + '\n' + '\n'
            self.log.info(string_to_append)
	    i = i + 1
	return string_to_append

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
                    except Exception, e:
                        self.log.warning("Request %s had an exception (%s)", r.name, e)
                        self.log.debug(traceback.format_exc(None))
        except InfoConnectionFailure, e:
            self.log.warning("Could not read requests from infoservice. (%s)", e)

    def process_request(self, request):
        self.log.debug("Processing headnode for '%s'", request.name)

        headnode    = None
        next_state  = None
        reason      = None

        if not request.headnode:
            # Request has not yet indicated the name it wants for the headnode,
            # so we simply return.
            return

        try:
            headnode = self.client.getNodeset(request.headnode)
        except InfoEntityMissingException:
            pass
        except InfoConnectionFailure:
            return

        try:
            if headnode is None:
                if request.state == 'initializing':
                    headnode = self.create_headnode_nodeset(request)
                elif request.state == 'cleanup' or request.state == 'terminated':
                    # Nothing to do, the headnode has been cleaned-up
                    return
                else:
                    # Something went wrong, the headnode should still be there.
                    self.log.error("Could not find headnode information for %s", request.name)
                    return

            next_state, reason = headnode.state, headnode.state_reason

            if request.state == 'cleanup' or request.state == 'terminated':
		self.log.info("Calling state_terminating")
                (next_state, reason) = self.state_terminating(request, headnode)

            if next_state == 'new':
                (next_state, reason) = self.state_new(request, headnode)

            if next_state == 'booting': 
                (next_state, reason) = self.state_booting(request, headnode)

            if next_state == 'pending':
                (next_state, reason) = self.state_pending(request, headnode)

            if next_state == 'running':
                (next_state, reason) = self.state_running(request, headnode)

            if (next_state != 'terminated') and (next_state != 'failure'):
                (next_state, reason) = self.check_timeout(request, next_state, reason)

        except Exception, e:
            self.log.debug("Error while processing headnode: %s", e)
            self.log.warning(traceback.format_exc(None))

            if headnode:
                (next_state, reason) = ('failure', 'Internal error: %s' % e)
            else:
                raise

        headnode.state        = next_state
        headnode.state_reason = reason

        try:
            if headnode.state == 'terminated':
                self.delete_headnode_nodeset(request)
            else:
                self.client.storeNodeset(headnode)
        except Exception, e:
            self.log.warning("Storing the new headnode state failed. (%s)", e)
            self.log.warning(traceback.format_exc(None))

    def state_terminating(self, request, headnode):
        try:
            config.load_kube_config(config_file = '/etc/kubernetes/admin.conf')
	    configuration = kubernetes.client.Configuration()
	    api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient(configuration))
    	    api_response = api_instance.delete_namespace(str(request.name))
            self.initializers.pop(request.name, None)
        except Exception, e:
            self.log.warning('Could not delete headnode instance for request %s (%s)', request.name, e)
        finally:
            return ('terminated', 'Headnode succesfully terminated')

    def state_new(self, request, headnode):
        self.log.info('Creating new nodeset %s for request %s', request.headnode, request.name)
        try:
            login = self.boot_pod(request, headnode)
            if not login:
                self.log.warning('Could not boot headnode for request %s', request.name)
                return ('failure', 'Could not boot headnode.', request.name)
            else:
                self.log.debug('Waiting for headnode for request %s to come online', request.name)
		self.log.info("BOOTING")
                return ('booting', 'Headnode is booting up.')
        except Exception, e:
            self.log.warning('Error in request to openstack: %s', e)
            return ('failure', 'Could not boot headnode because of an internal error: %s.' % e)

    def state_booting(self, request, headnode):
        if not headnode.app_host:
            headnode.app_host = self.__get_ip(request)
            if headnode.app_host:
                self.last_contact_times[request.name] = time.time()
	    headnode.port = self.__get_port( request)
            return ('pending', 'Headnode is being configured.')

    def state_pending(self, request, headnode):
	self.login_pending(request)
        (next_state, state_reason) = ('running', 'Headnode is ready to be used.') 

        if next_state == 'running':
            self.log.info('Done initializing pod %s for request %s', request.headnode, request.name)
            #self.report_running_server(request, headnode)
        elif next_state != 'failure':
            self.log.debug('Waiting for headnode for %s to finish initialization.', request.name)
        return (next_state, state_reason)

    def state_running(self, request, headnode):
        return ('running', 'Headnode is ready to be used.')

    def check_if_online(self, request, headnode):
        if headnode.app_host is None:
            self.log.debug('Headnode for %s does not have an address yet.', request.name)
            return False

        try:
	    self.log.info('The port is')
	    self.log.info(headnode.port)
            self.log.debug("Connecting to headnode %s with key %s as user %s", headnode.app_host, self.node_private_key_file, self.node_user )
            subprocess.check_call([
                'ssh',
                '-i',
                self.node_private_key_file,
		'-p',
                str(headnode.port),
                self.node_user +'@'
                + headnode.app_host], shell=True)

            self.log.info('Headnode for %s running at %s', request.name, headnode.app_host)

            return True
        except subprocess.CalledProcessError:
            self.log.debug('Headnode for %s running at %s could not be accessed.', request.name, headnode.app_host)
            return False

    def check_timeout(self, request, next_state, reason):
        now = time.time()

        diff = 0

        if self.last_contact_times.has_key(request.name):
            diff = now - self.last_contact_times[request.name]
        else:
            self.last_contact_times[request.name] = now
            return (next_state, reason)

        self.log.debug('Headnode for %s last contacted %d seconds ago.', request.name, diff)

        if diff > self.node_max_no_contact_time:
            self.log.warning('Headnode for %s could not be contacted after %d seconds. Declaring failure.', request.name, self.node_max_no_contact_time)
            return ('failure', 'Headnode could no be contacted after %d seconds.' % self.node_max_no_contact_time)
        elif diff > self.node_max_no_contact_time/2:
            self.log.warning('Headnode for %s could not be contacted! (waiting for %d seconds before declaring failure)', request.name, self.node_max_no_contact_time - diff)
            reason = reason + " (Headnode could not be contacted. This may be a transient error. Waiting for {:.0f} seconds before declaring failure.)".format(self.node_max_no_contact_time - diff)
            return (next_state, reason)
        else:
            return (next_state, reason)

    def boot_pod(self, request, headnode):
	self.log.info('Starting boot')
        try:
            login = login_info(self,request)
	    if login:
            	self.log.info('Found headnode at %s for request %s', request.headnode, request.name)
            	return login
        except Exception, e:
            pass

        self.log.info('Booting new headnode for request %s...', request.name)
        login = self.login_create(request)
	self.log.info('past login create')
	if login == None:
		self.log.info('login is null')
	self.log.info('returning from boot server')
        return login


    def check_if_done(self, request, headnode):
            return ('running', 'Headnode is ready to be used.')

    def report_running_server(self, request, headnode):
        try:
            headnode.app_sectoken = self.read_encoded(self.secret_auth_filename(request))
            headnode.state = 'running'
        except Exception, e:
            self.log.warning('Cound not read file of shared secret for request %s (%s)', request.name, e)
            self.log.debug(traceback.format_exc(None))
            headnode.state = 'failure'

    def secret_auth_filename(self, request):
        # file created by ansible
        return '/tmp/secret.' + request.name

    def read_encoded(self, filename):
        with open(filename, 'r') as f:
            contents = f.read()
            return self.client.encode(contents)

    def get_members_names(self, request):
        members = None

        if request.project:
            project = self.client.getProject(request.project)
            if project:
                members = project.members

        if not members:
            members = []
            self.log.warning('Could not find user names for request %s.')

        return members

    def _get_members_attributes(self, request, attribute):
        members    = self.get_members_names(request)

        attributes = {}
        for member in members:
            try:
                user = self.client.getUser(member)
            except Exception, e:
                self.log.warning("Could not find user: %s", member)
                raise e

            attr_value = getattr(user, attribute, None)
            if not attr_value:
                self.log.warning('Could not find attribute: %s, for user %s',
                                 attribute, member)
            else:
                attributes[member] = attr_value

        return attributes
        
    def get_members_keys(self, request):
        return self._get_members_attributes(request, 'sshpubstring')

    def get_members_uuids(self, request):
        return self._get_members_attributes(request, 'identity_id')

    def get_globusvc3_mapfile(self, request):
        members_uuids = self.get_members_uuids(request)
        mapfile = {i:j for j, i in members_uuids.iteritems()}

        return mapfile

    def get_builder_options(self, request):
        packages = []
        for env_name in request.environments:
            env = self.client.getEnvironment(env_name)
            if env.packagelist:
                packages.extend(env.packagelist)
        return " ".join([ "--require %s" % p for p in packages ])

    def create_headnode_nodeset(self, request):
        self.log.debug("Creating new headnode spec '%s'", request.headnode)

        headnode = self.client.defineNodeset(
                name = request.headnode,
                owner = request.owner,
                node_number = 1,
                app_type = self.getAppType(request),
                app_role = 'head-node', 
                environment = None,
                description = 'Headnode nodeset automatically created: ' + request.headnode,
                displayname = request.headnode)

        self.last_contact_times[request.name] = time.time()

        return headnode

    def delete_headnode_nodeset(self, request):
        if request.headnode:
            try:
                headnode = self.client.getNodeset(request.headnode)
                self.log.debug("Deleting headnode spec '%s'", request.headnode)
                self.client.deleteNodeset(request.headnode)
            except Exception, e:
                self.log.debug("Could not delete headnode nodeset '%s'." % (request.headnode,))
                self.log.debug(traceback.format_exc(None))

    def getAppType(self, request):
        cluster = self.client.getCluster(request.cluster)
        nodeset = self.client.getNodeset(cluster.nodesets[0])
        app_type = nodeset.app_type

        return app_type

    def __get_ip(self, request):
        login = login_info(self,request)
        if login[0] == None:
                self.log.debug("Headnode for request %s is not active yet.", request.name)
                return None
	else:
                return login[0]
    def __get_port(self, request):
        login = login_info(self, request)
        if login[1] == None:
                self.log.debug("Headnode for request %s is not active yet.", request.name)
                return None
        else:
                return login[1]
    def vm_name(self, request):
        return self.node_prefix + request.name

