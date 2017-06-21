#!/usr/bin/env python
# 
#
import json

from vc3master.task import VC3Task
from vc3infoservice.infoclient import PairingRequest, Pairing

class HandlePairingRequests(VC3Task):
    '''
    Check for requests in /info/pairing to make pairing. 
    Create keypair, store in /pairing/<pairingcode> category for unvalidated retrieval. 
     
    '''    
    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)
        self.log.debug("Getting 'vc3' doc.")
        try:
            doc = self.parent.parent.infoclient.getdocument('pairing')
            #self.log.debug('Doc is %s' % doc)
        except Exception as e:
            self.log.error("Exception: %s" % e)

        if doc is not None:            
            try:
                ds = json.loads(doc)
                try:
                    pdoc = ds['pairing']
                    self.log.debug("pairing section already in doc. Doing nothing.")
                except KeyError:
                    # no vc3 section
                    self.log.debug("No pairing section in doc.")
                self.log.info("Handling pairing request(s)...")    
                    
                for prname in ds.keys():
                    pr = ds[prname]
                    self.log.debug("Detected pairing request %s" % pr)
                    pro = PairingRequest.objectFromDict(pr)
                    self.log.debug("Made pairing request object %s" % pro)
                    (certstr, keystr) = self.parent.parent.ssca.getusercert(pro.cn)
                    self.log.debug("Got cert and key strings for %s" )
                    po = Pairing(pro.name, 
                                 pro.state, 
                                 pro.acl, 
                                 pro.cn, 
                                 pro.pairingcode, 
                                 certstr, 
                                 keystr)        
                    pd = po.makeDictObject()
                    self.log.debug("Made dict %s" % pd)
                    self.parent.parent.pairingclient.mergedocument(po.pairingcode, pd)
                    self.log.debug("Merged doc to /pairing/%s" % po.pairingcode)
                    
                    
            except Exception as e:
                self.log.error("Exception: %s" % e)
                
    def mergedocument(self, key, doc):
                
        u = "https://%s:%s/pairing?key=%s" % (self.infohost, 
                            self.httpsport,
                            key
                            )
        self.log.debug("Trying to merge document %s at %s" % (doc, u))
        try:
            r = requests.put(u, verify=self.chainfile, cert=(self.certfile, self.keyfile), params={'data' : doc})
            self.log.debug(r.status_code)
        
        except requests.exceptions.ConnectionError, ce:
            self.log.error('Connection failure. %s' % ce)
    
    def storedocumentobject(self, dict, key):
        '''
        Directly store Python dictionary as JSON ...
        
        '''
        if key not in dict.keys():
            td = {}
            td[key] = dict
            dict = td
            
        jstr = json.dumps(dict)
        self.log.debug("JSON string: %s" % jstr)
        self.storedocument(key, jstr)