# vim: set tw=120 ts=4 sw=4 et ft=python eol :

import pprint
import socket
import sys

class Gearman:
    """ Gearman plugin for sd-agent

        Gets information about a gearman queue. Requires python 2.6 because of
        socket.create_connection, bytearray.

        Example Configuration:
         gearman_server:  localhost
         #gearman_port:    4730
         #gearman_timeout: 2

        Produces a bunch of keys:
          - gearman_%FUNCTION_NAME%_workers - The total number of workers
              capable of running that function
          - gearman_%FUNCTION_NAME%_running - The number of this job currently
              being run by workers
          - gearman_%FUNCTION_NAME%_queue - The number of this job on the
              queue
          - gearman_total_workers, gearman_total_running, gearman_total_queue:
              totals of the above, for all functions

        By Dominic Scheirlinck <dominic@vendhq.com> """

    RECV_WINDOW = 4096
    MAX_REPLY_LENGTH = 1024 * 16 # 16 KiB should be enough for anybody

    default_config = {
        'gearman_server':  None, # None, not 'localhost' so we can disable in config
        'gearman_port':    4730,
        'gearman_timeout': 5, # Let's be strict - no time to waste
    }

    status_columns = {
                'queue': 1,
                'running': 2,
                'workers': 3
    }.items()

    def __init__(self, agentConfig, checksLogger, rawConfig):
        self.agentConfig = agentConfig
        self.checksLogger = checksLogger
        self.rawConfig = rawConfig

    def command(self, sock, command):
        self.checksLogger.info('Gearman: sending command: %s', command)
        sock.sendall(command + "\n");

        data = bytearray(self.MAX_REPLY_LENGTH + self.RECV_WINDOW);
        length = 0
        while (1):
            nbytes = sock.recv_into(data, self.RECV_WINDOW)

            if nbytes == 0:
                self.checksLogger.error('Gearman: connection closed prematurely')
                return ''

            length = length + nbytes

            if length > self.MAX_REPLY_LENGTH:
                self.checksLogger.error('Gearman: reply too long')
                return ''

            if ".\n" in data:
                data = data[0:data.index(".\n")]
                self.checksLogger.info('Gearman: found end of reply')
                break

        return data

    def get_data(self, server):
        """ Connects to the Gearman daemon's, and uses the administrative
        protocol (http://gearman.org/?id=protocol) to request status
        information. This is returned as an unformatted bytearray """

        try:
            self.checksLogger.info('Gearman: connecting to %s:%d', server)
            sock = socket.create_connection(server, self.config['gearman_timeout'])

            data = {
                'status': self.command(sock, 'status'),
                'workers': self.command(sock, 'workers')
            }

            self.checksLogger.info('Gearman: closing connection')
        except:
            self.checksLogger.error("Gearman: Could not communicate with server: %s",
                    sys.exc_info()[0])
            raise
            return None
        finally:
            sock.close()

        return data

    def parse_status(self, data):
        data = str(data).strip()

        if data == '':
            return dict()

        status = dict([("gearman_total_" + k, 0) for (k, v) in
            self.status_columns])

        data = data.split("\n")
        for function in data:
            values = function.split("\t")
            if len(values) != 4:
                continue

            function = dict([("gearman_%s_%s" % (values[0], key), values[column])
                for (key, column) in self.status_columns])

            status = dict(status.items() + function.items())

            # total_workers can not be just summed
            for (key, column) in {'queue': 1, 'running': 2}.items():
                key = "gearman_total_" + key
                status[key] = status[key] + int(values[column])

        return status

    def count_workers(self, data):
        data = str(data).strip()

        if data == '':
            return None

        self.checksLogger.debug('Gearman: worker data: %s', data)
        return len(data.split("\n"))

    def status(self):
        server = (self.config['gearman_server'], self.config['gearman_port'])

        data = self.get_data(server)
        if not data:
            return {}

        status = self.parse_status(data['status'])

        workers = self.count_workers(data['workers'])
        if workers:
            status['gearman_total_workers'] = workers

        self.checksLogger.debug('Gearman: final data: %s', status)
        return status

    def run(self):
        config = dict([(k, self.rawConfig['Main'][k]) for k in
            self.default_config.keys() if k in self.rawConfig['Main']])
        self.config = dict(self.default_config.items() + config.items())

        if not self.config['gearman_server']:
            self.checksLogger.warning('No gearman_server config found. Skipping Gearman plugin');
            return dict()

        self.checksLogger.info('Gearman: configured as %r' % (self.config))
        return self.status()
