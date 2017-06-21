#!/usr/bin/env python
# 
#
import json

from vc3master.task import VC3Task
from vc3infoservice.infoclient import Pairing

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
                    
                for poname in ds.keys():
                    pd = ds[poname]
                    self.log.debug("Detected pairing request %s" % poname)
                    po = Pairing.objectFromDict(pd)
                    self.log.debug("Made pairing object %s" % po)
                    (certstr, keystr) = self.parent.parent.ssca.getusercert(po.cn)
                    self.log.debug("Got cert and key strings for %s" % poname )
                    ecertstr = self.parent.parent.infoclient.encode(certstr)
                    ekeystr = self.parent.parent.infoclient.encode(keystr)
                    po.cert = ecertstr
                    po.key = ekeystr  
                    po.store(self.parent.parent.infoclient)
                    self.log.debug("Stored object %s" % po)
                    #newpd = po.makeDictObject()
                    #self.log.debug("Made dict %s" % newpd)
                    #self.parent.parent.infoclient.storedocument('pairing', newpd)
                    
            except Exception as e:
                self.log.error("Exception: %s" % e)
                
