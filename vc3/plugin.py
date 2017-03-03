class PluginManager(object):
    '''
    Entry point for plugins creation, initialization, starting, and configuration. 
    
    '''
    def __init__(self, parent):
        '''
        Top-level object to provide plugins. 
        '''
        self.parent = parent


    def getplugin(self, kind, config, section, attribute):
        '''
        Provide initialized plugin object using config and section. 
        '''
        try:
            name = config.get(section, attribute)
        except:
            return None
        return self._getplugin(kind, config, section, name)


    def _getplugin(self, kind, config, section, name):
        ko = self._getpluginclass(kind, name)
        po = ko(self.parent, config, section)
        return po
    
        
        
    def _getpluginclass(self, kind, name):
        '''
        returns plugin class. Classes, not objects. The __init__() methods have not been 
        called yet.
        '''
        ppath = 'vc3client.plugins.%s.%s'  %(kind, name)
        try:
            plugin_module = __import__(ppath, globals(), locals(), name)
        except Exception, ex:
            pass
    
        plugin_class = getattr(plugin_module, name)
        return plugin_class
    

