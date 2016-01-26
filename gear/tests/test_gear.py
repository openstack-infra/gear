# Copyright (c) 2013 Hewlett-Packard Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket

import testscenarios
import testtools

import gear
from gear import tests


class ConnectionTestCase(tests.BaseTestCase):

    scenarios = [
        ('both_string',
         dict(host="hostname", port='80')),
        ('string_int',
         dict(host="hostname", port=80)),
        ('none_string',
         dict(host=None, port="80")),
    ]

    def setUp(self):
        super(ConnectionTestCase, self).setUp()
        self.conn = gear.Connection(self.host, self.port)

    def test_params(self):
        self.assertTrue(repr(self.conn).endswith(
            'host: %s port: %s>' % (self.host, self.port)))


class TestConnection(tests.BaseTestCase):

    def setUp(self):
        super(TestConnection, self).setUp()
        self.patch(socket, 'socket', tests.FakeSocket)
        self.conn = gear.Connection('127.0.0.1', 4730)
        self.conn.connect()
        self.socket = self.conn.conn

    def assertEndOfData(self):
        # End of data
        with testtools.ExpectedException(tests.FakeSocketEOF):
            self.conn.readPacket()

    def test_readPacket_admin(self):
        response = b'test\t0\t0\t1\n.\n'
        req = gear.StatusAdminRequest()
        self.socket._set_data([response])
        self.conn.admin_requests.append(req)
        r1 = self.conn.readPacket()
        self.assertEqual(r1.response, response)
        self.assertEndOfData()

    def test_readPacket_admin_mix(self):
        p1 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 5000)
        )
        response = b'test\t0\t0\t1\n.\n'
        p2 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 5000)
        )
        req = gear.StatusAdminRequest()
        self.conn.admin_requests.append(req)
        self.socket._set_data([p1.toBinary() + response + p2.toBinary()])
        r1 = self.conn.readPacket()
        self.assertEquals(r1, p1)
        ra = self.conn.readPacket()
        self.assertEqual(ra.response, response)
        r2 = self.conn.readPacket()
        self.assertEquals(r2, p2)
        self.assertEndOfData()

    def test_readPacket_large(self):
        p1 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 10000)
        )
        self.socket._set_data([p1.toBinary()])
        r1 = self.conn.readPacket()
        self.assertEquals(r1, p1)
        self.assertEndOfData()

    def test_readPacket_multi_pdu(self):
        p1 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 2600)
        )
        p2 = gear.Packet(
            gear.constants.REQ,
            gear.constants.GRAB_JOB_UNIQ,
            b''
        )
        self.socket._set_data([p1.toBinary()[:1448],
                               p1.toBinary()[1448:] + p2.toBinary()])
        # First packet
        r1 = self.conn.readPacket()
        self.assertEquals(r1, p1)
        # Second packet
        r2 = self.conn.readPacket()
        self.assertEquals(r2, p2)
        self.assertEndOfData()


class TestServerConnection(tests.BaseTestCase):

    def setUp(self):
        super(TestServerConnection, self).setUp()
        self.socket = tests.FakeSocket()
        self.conn = gear.ServerConnection('127.0.0.1', self.socket,
                                          False, 'test')

    def assertEndOfData(self):
        # End of data
        with testtools.ExpectedException(gear.RetryIOError):
            self.conn.readPacket()
        # Still end of data
        with testtools.ExpectedException(gear.RetryIOError):
            self.conn.readPacket()

    def test_readPacket_admin(self):
        command = b'status\n'
        self.socket._set_data([command])
        r1 = self.conn.readPacket()
        self.assertEqual(r1.command, command.strip())
        self.assertEndOfData()

    def test_readPacket_admin_mix(self):
        p1 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 5000)
        )
        command = b'status\n'
        p2 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 5000)
        )
        self.socket._set_data([p1.toBinary() + command + p2.toBinary()])
        r1 = self.conn.readPacket()
        self.assertEquals(r1, p1)
        ra = self.conn.readPacket()
        self.assertEqual(ra.command, command.strip())
        r2 = self.conn.readPacket()
        self.assertEquals(r2, p2)
        self.assertEndOfData()

    def test_readPacket_large(self):
        p1 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 10000)
        )
        self.socket._set_data([p1.toBinary()])
        r1 = self.conn.readPacket()
        self.assertEquals(r1, p1)
        self.assertEndOfData()

    def test_readPacket_multi_pdu(self):
        p1 = gear.Packet(
            gear.constants.REQ,
            gear.constants.WORK_COMPLETE,
            b'H:127.0.0.1:11\x00' + (b'x' * 2600)
        )
        p2 = gear.Packet(
            gear.constants.REQ,
            gear.constants.GRAB_JOB_UNIQ,
            b''
        )
        self.socket._set_data([p1.toBinary()[:1448],
                               p1.toBinary()[1448:] + p2.toBinary()])
        # First half of first packet
        with testtools.ExpectedException(gear.RetryIOError):
            self.conn.readPacket()
        # Second half of first packet
        r1 = self.conn.readPacket()
        self.assertEquals(r1, p1)
        # Second packet
        r2 = self.conn.readPacket()
        self.assertEquals(r2, p2)
        self.assertEndOfData()


class TestClient(tests.BaseTestCase):

    def test_handleStatusRes_1(self):
        client = gear.Client()

        packet = gear.Packet(
            gear.constants.RES,
            gear.constants.STATUS_RES,
            b'H:127.0.0.1:11\x001\x001\x00\x00'
        )
        packet.getJob = lambda: gear.Job(b"", b"")
        job = client.handleStatusRes(packet)

        self.assertTrue(job.known)
        self.assertTrue(job.running)

    def test_handleStatusRes_2(self):
        client = gear.Client()

        packet = gear.Packet(
            gear.constants.RES,
            gear.constants.STATUS_RES,
            b'H:127.0.0.1:11\x001\x000\x00\x00'
        )
        packet.getJob = lambda: gear.Job(b"", b"")
        job = client.handleStatusRes(packet)

        self.assertTrue(job.known)
        self.assertFalse(job.running)

    def test_ACL(self):
        acl = gear.ACL()
        acl.add(gear.ACLEntry('worker', register='foo.*'))
        acl.add(gear.ACLEntry('client', invoke='foo.*'))
        acl.add(gear.ACLEntry('manager', grant=True))
        self.assertEqual(len(acl.getEntries()), 3)

        self.assertTrue(acl.canRegister('worker', 'foo-bar'))
        self.assertTrue(acl.canRegister('worker', 'foo'))
        self.assertFalse(acl.canRegister('worker', 'bar-foo'))
        self.assertFalse(acl.canRegister('worker', 'bar'))
        self.assertFalse(acl.canInvoke('worker', 'foo'))
        self.assertFalse(acl.canGrant('worker'))

        self.assertTrue(acl.canInvoke('client', 'foo-bar'))
        self.assertTrue(acl.canInvoke('client', 'foo'))
        self.assertFalse(acl.canInvoke('client', 'bar-foo'))
        self.assertFalse(acl.canInvoke('client', 'bar'))
        self.assertFalse(acl.canRegister('client', 'foo'))
        self.assertFalse(acl.canGrant('client'))

        self.assertFalse(acl.canInvoke('manager', 'bar'))
        self.assertFalse(acl.canRegister('manager', 'foo'))
        self.assertTrue(acl.canGrant('manager'))

        acl.remove('worker')
        acl.remove('client')
        acl.remove('manager')

        self.assertFalse(acl.canRegister('worker', 'foo'))
        self.assertFalse(acl.canInvoke('client', 'foo'))
        self.assertFalse(acl.canGrant('manager'))

        self.assertEqual(len(acl.getEntries()), 0)

    def test_ACL_register(self):
        acl = gear.ACL()
        acl.grantRegister('worker', 'bar.*')
        self.assertTrue(acl.canRegister('worker', 'bar'))
        acl.revokeRegister('worker')
        self.assertFalse(acl.canRegister('worker', 'bar'))

    def test_ACL_invoke(self):
        acl = gear.ACL()
        acl.grantInvoke('client', 'bar.*')
        self.assertTrue(acl.canInvoke('client', 'bar'))
        acl.revokeInvoke('client')
        self.assertFalse(acl.canInvoke('client', 'bar'))

    def test_ACL_grant(self):
        acl = gear.ACL()
        acl.grantGrant('manager')
        self.assertTrue(acl.canGrant('manager'))
        acl.revokeGrant('manager')
        self.assertFalse(acl.canGrant('manager'))

    def test_double_shutdown(self):
        client = gear.Client()
        client.shutdown()
        client.shutdown()


def load_tests(loader, in_tests, pattern):
    return testscenarios.load_tests_apply_scenarios(loader, in_tests, pattern)
