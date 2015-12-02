# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2013 Hewlett-Packard Development Company, L.P.
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

"""Common utilities used in testing"""

import errno
import logging
import os
import socket

import fixtures
import testresources
import testtools

TRUE_VALUES = ('true', '1', 'yes')


class BaseTestCase(testtools.TestCase, testresources.ResourcedTestCase):

    def setUp(self):
        super(BaseTestCase, self).setUp()
        test_timeout = os.environ.get('OS_TEST_TIMEOUT', 30)
        try:
            test_timeout = int(test_timeout)
        except ValueError:
            # If timeout value is invalid, fail hard.
            print("OS_TEST_TIMEOUT set to invalid value"
                  " defaulting to no timeout")
            test_timeout = 0
        if test_timeout > 0:
            self.useFixture(fixtures.Timeout(test_timeout, gentle=True))

        if os.environ.get('OS_STDOUT_CAPTURE') in TRUE_VALUES:
            stdout = self.useFixture(fixtures.StringStream('stdout')).stream
            self.useFixture(fixtures.MonkeyPatch('sys.stdout', stdout))
        if os.environ.get('OS_STDERR_CAPTURE') in TRUE_VALUES:
            stderr = self.useFixture(fixtures.StringStream('stderr')).stream
            self.useFixture(fixtures.MonkeyPatch('sys.stderr', stderr))

        self.useFixture(fixtures.FakeLogger(
            level=logging.DEBUG,
            format='%(asctime)s %(name)-32s '
            '%(levelname)-8s %(message)s'))
        self.useFixture(fixtures.NestedTempfile())


def raise_eagain():
    e = socket.error("socket error [Errno 11] "
                     "Resource temporarily unavailable")
    e.errno = errno.EAGAIN
    raise e


class FakeSocketEOF(Exception):
    pass


class FakeSocket(object):
    def __init__(self, af=None, socktype=None, proto=None):
        self.packets = []
        self.packet_num = 0
        self.packet_pos = 0
        self.blocking = 1

    def _set_data(self, data):
        self.packets = data

    def connect(self, sa):
        pass

    def setblocking(self, blocking):
        self.blocking = blocking

    def recv(self, count):
        while True:
            if self.packet_num + 1 > len(self.packets):
                if not self.blocking:
                    raise_eagain()
                raise FakeSocketEOF(
                    "End of data reached in blocking mode")
            packet = self.packets[self.packet_num]
            if self.packet_pos + 1 > len(packet):
                self.packet_num += 1
                self.packet_pos = 0
                if not self.blocking:
                    raise_eagain()
                continue
            break
        start = self.packet_pos
        end = min(start + count, len(packet))
        self.packet_pos = end
        return bytes(packet[start:end])
