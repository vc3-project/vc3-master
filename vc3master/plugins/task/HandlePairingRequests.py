#!/usr/bin/env python
# 
#
import json

from vc3master.task import VC3Task
from vc3infoservice.infoclient import Pairing, InfoConnectionFailure

class HandlePairingRequests(VC3Task):
    '''
    Check for requests in /info/pairing to make pairing. 
    Create keypair, store in /pairing/<pairingcode> category for unvalidated retrieval. 
     
    '''    
    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)
        self.log.debug("Getting 'pairing' doc.")
        self.ic = self.parent.parent.infoclient
        self.ssca = self.parent.parent.ssca
        try:
            doc = self.ic.getdocument('pairing')
            #self.log.debug('Doc is %s' % doc)
            ds = json.loads(doc)
            pdoc = ds['pairing']
            self.log.debug("pairing section in doc. Handling request(s)...")
            for poname in ds['pairing'].keys():
                #pd = ds[poname]
                self.log.debug("Detected pairing request %s" % poname)
                #po = Pairing.objectFromDict(pd)
                #self.log.debug("Made pairing object %s" % po)
                if ds['pairing'][poname]['cert'] is None:                    
                    (certstr, keystr) = self.ssca.getusercert(ds['pairing'][poname]['cn'])
                    self.log.debug("Got cert and key strings for %s" % poname )
                    ecertstr = self.ic.encode(certstr)
                    ekeystr = self.ic.encode(keystr)
                    ds['pairing'][poname]['cert'] = ecertstr
                    ds['pairing'][poname]['key'] = ekeystr  
            jd = json.dumps(ds)
            self.ic.storedocument('pairing',jd)
        except InfoConnectionFailure, e:
            self.log.warning("Could not read pairing requests from infoservice. (%s)", e)
        except KeyError:
        # no pairing section
            self.log.debug("No pairing section in doc.")
        #    ds['pairing'] = {}
        #    jd = json.dumps(ds)
        #    self.ic.storedocument('pairing',jd)

                
