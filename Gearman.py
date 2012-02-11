# vim: set tw=120 ts=4 sw=4 et ft=python eol :

import pprint
import socket

class Gearman:
    """ Gearman plugin for sd-agent

        Gets information about a gearman queue.

        By Dominic Scheirlinck <dominic@vendhq.com> """

    default_config = {
        'gearman_server':  None, # None, not 'localhost' so we can disable in config
        'gearman_port':    4730,
        'gearman_timeout': 2, # Let's be strict - no time to waste
    }

    def __init__(self, agentConfig, checksLogger, rawConfig):
        self.agentConfig = agentConfig
        self.checksLogger = checksLogger
        self.rawConfig = rawConfig

        pprint.pprint(self.checksLogger)

    def status(self):
        self.checksLogger.info('Gearman: connecting to %s:%d' %
            (self.config['gearman_server'], self.config['gearman_port']));



        self.checksLogger.info('');
        self.checksLogger.info('');
        self.checksLogger.warning('**********************');

        data = {
        }
        self.checksLogger.info('Gearman: final data is %r' % (data))

        self.checksLogger.warning('**********************');
        self.checksLogger.info('');
        self.checksLogger.info('');
        self.checksLogger.info('');

        return data

    def run(self):
        if 'gearman_server' not in self.rawConfig['Main']:
            self.checksLogger.warning('No gearman_server config found. Skipping Gearman plugin');
            return dict()

        config = dict([(k, self.rawConfig['Main'][k]) for k in
            self.default_config.keys() if k in self.rawConfig['Main']])
        self.config = dict(self.default_config.items() + config.items())

        self.checksLogger.info('Gearman: configured as %r' % (self.config))
        return self.status()
