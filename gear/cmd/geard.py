#!/usr/bin/env python
# Copyright 2013 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import argparse
import daemon
import extras
import gear
import logging
import os
import pbr.version
import signal
import sys

pid_file_module = extras.try_imports(['daemon.pidlockfile', 'daemon.pidfile'])


class Server(object):
    def __init__(self):
        self.args = None
        self.config = None
        self.gear_server_pid = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='Gearman server.')
        parser.add_argument('-d', dest='nodaemon', action='store_true',
                            help='do not run as a daemon')
        parser.add_argument('-p', dest='port', default=4730,
                            help='port on which to listen')
        parser.add_argument('--log-config', dest='log_config',
                            help='logging config file')
        parser.add_argument('--pidfile', dest='pidfile',
                            default='/var/run/geard/geard.pid',
                            help='PID file')
        parser.add_argument('--ssl-ca', dest='ssl_ca', metavar='PATH',
                            help='path to CA certificate')
        parser.add_argument('--ssl-cert', dest='ssl_cert', metavar='PATH',
                            help='path to SSL public certificate')
        parser.add_argument('--ssl-key', dest='ssl_key', metavar='PATH',
                            help='path to SSL private key')
        parser.add_argument('--version', dest='version', action='store_true',
                            help='show version')
        self.args = parser.parse_args()

    def setup_logging(self):
        if self.args.log_config:
            if not os.path.exists(self.args.log_config):
                raise Exception("Unable to read logging config file at %s" %
                                self.args.log_config)
            logging.config.fileConfig(self.args.log_config)
        else:
            if self.args.nodaemon:
                logging.basicConfig(level=logging.DEBUG)
            else:
                logging.basicConfig(level=logging.INFO,
                                    filename="/var/log/geard/geard.log")

    def main(self):
        self.server = gear.Server(self.args.port,
                                  self.args.ssl_key,
                                  self.args.ssl_cert,
                                  self.args.ssl_ca)
        signal.pause()


def main():
    server = Server()
    server.parse_arguments()

    if server.args.version:
        vi = pbr.version.VersionInfo('gear')
        print("Gear version: {}".format(vi.version_string()))
        sys.exit(0)

    server.setup_logging()
    if server.args.nodaemon:
        server.main()
    else:
        pid = pid_file_module.TimeoutPIDLockFile(server.args.pidfile, 10)
        with daemon.DaemonContext(pidfile=pid):
            server.main()


if __name__ == "__main__":
    sys.path.insert(0, '.')
    main()
