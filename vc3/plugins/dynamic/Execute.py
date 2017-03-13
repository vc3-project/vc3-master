from plugin import PluginInterface
import logging

from multiprocessing import Process

class Execute(PluginInterface):
    def __init__(self, factory, log, **args):
        Super(Execute, self).__init__(factory, log, **args)

        self.process = None

        try:
            self.service_name = args['service']
        except KeyError:
            raise Exception('No service defined')

    def start(self):
        if self.process:
            raise Exception('Plugin already started')

        self.process = Process(target = self.builder_factory, args = (self.service_name,))

        if self.died_with_error():
            self.log.info("Started process for '" + service_name + "' pid: " + self.process.pid)
        else:
            self.log.info("Failed to start process for '" + service_name + "' error: " + self.process.exitcode)

    def is_alive(self):
        return self.process.is_alive()

    def terminate(self):
        return self.process.terminate()

    def wait(self, seconds=60):
        return self.process.join(seconds)

    def died_with_error(self):
        if self.is_alive():
            return False

        if self.exitcode is None or self.exitcode == 0:
            return False

        return True



