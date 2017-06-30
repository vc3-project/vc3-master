#!/usr/bin/env python

import json

from vc3master.task import VC3Task

class AddFactoryConfiguration(VC3Task):

    def runtask(self):
        self.log.info("Running task %s" % self.section)
        self.infoclient = self.parent.parent.infoclient
    
        # FIXME
        # is there a general purpose method in VC3Task class to download docs ???
        jsondoc = self.infoclient.getdocument('request')
        doc = json.loads(doc)

        """
        format of the request data structure:

        {
            "request" : {
                "jhover-000001" : {
                    "vcid" : "000001",
                    "factories" : {
                        "default" : {
                            "queuesconf" : "queues.conf contents."
                        },
                        "mfa-sdcc-jhover" : {
                             "queuesconf" : "queues.conf contents.",
                             "pin" : "1234"
                        }
                    }
                }
            }
        }
        """

        doc = self._updatedoc(doc)
        self._uploaddoc(doc)


    def _updatedoc(self, doc):

        for request in doc['request']:
            for factory in doc['request'][request]:
                conf = self._createconf(doc, request, factory) 
                doc = self._addconf(doc, request, factory, conf)
        return doc
               

    def _createconf(self, doc, request, factory):
        '''
        builds the content of the attribute 'queuesconf'
        as a string, not as a ConfigParser object, so it is ready
        to be added to the document
        '''
        # FIXME
        # to be done !!!
        conf = ""
        return conf 


    def _addconf(self, doc, request, factory, conf):
        doc['request'][request][factory]['queuesconf'] = conf
        return doc

 
    def _updatedoc(self, doc):
        # FIXME
        # is there a general purpose method in VC3Task class to upload docs ???
        jsondoc = json.dumps(doc)
        self.infoclient.storedocument('requeset',jsondoc)

        

