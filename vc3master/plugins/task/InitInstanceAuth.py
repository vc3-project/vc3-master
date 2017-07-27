#!/usr/bin/env python
# 
#
import json

from vc3master.task import VC3Task

class InitInstanceAuth(VC3Task):
    '''
    Plugin to do first-time overall VC3 instance setup. 
    Confirm that CA exists, create host cert for this server. 
     
     
    { "infoservice": {
        "ca-chain" : "<base64encoded ca chain file>"
        
        }
    }
     
    '''    
    def runtask(self):
        '''
        '''
        self.log.info("Running task %s" % self.section)
        self.log.debug("Getting 'infoservice' doc.")
        self.ic = self.parent.parent.infoclient
        try:
            doc = self.ic.getdocument('infoservice')
            self.log.debug('Doc is %s' % doc)

            if doc is not None:            
                try:
                    ds = json.loads(doc)
                    try:
                        vc3 = ds['infoservice']
                        self.log.debug("infoservice section already in doc. Doing nothing.")
                    except KeyError:
                        # no infoservice section
                        self.log.debug("No infoservice section in doc.")
                        ccstr = self.parent.parent.ssca.getcertchain()
                        eccstr = self.parent.parent.infoclient.encode(ccstr)
                        ds['infoservice'] = { "ca-chain" : eccstr, 
                                    "encoding" : "base64" }
                        jd = json.dumps(ds)
                        self.parent.parent.infoclient.storedocument('infoservice',jd)
                except Exception as e:
                    self.log.error("Exception: %s" % e)

        except Exception as e:
            self.log.error("Exception: %s" % e)

