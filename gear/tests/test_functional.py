# Copyright (c) 2013-2014 Hewlett-Packard Development Company, L.P.
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

import os
import threading
import time

from OpenSSL import crypto
import fixtures
import testscenarios
import testtools

import gear
from gear import tests


def iterate_timeout(max_seconds, purpose):
    start = time.time()
    count = 0
    while (time.time() < start + max_seconds):
        count += 1
        yield count
        time.sleep(0)
    raise Exception("Timeout waiting for %s" % purpose)


class TestFunctional(tests.BaseTestCase):
    scenarios = [
        ('no_ssl', dict(ssl=False)),
        ('ssl', dict(ssl=True)),
    ]

    def setUp(self):
        super(TestFunctional, self).setUp()
        if self.ssl:
            self.tmp_root = self.useFixture(fixtures.TempDir()).path
            root_subject, root_key = self.create_cert('root')
            self.create_cert('server', root_subject, root_key)
            self.create_cert('client', root_subject, root_key)
            self.create_cert('worker', root_subject, root_key)
            self.server = gear.Server(
                0,
                os.path.join(self.tmp_root, 'server.key'),
                os.path.join(self.tmp_root, 'server.crt'),
                os.path.join(self.tmp_root, 'root.crt'))
            self.client = gear.Client('client')
            self.worker = gear.Worker('worker')
            self.client.addServer('127.0.0.1', self.server.port,
                                  os.path.join(self.tmp_root, 'client.key'),
                                  os.path.join(self.tmp_root, 'client.crt'),
                                  os.path.join(self.tmp_root, 'root.crt'))
            self.worker.addServer('127.0.0.1', self.server.port,
                                  os.path.join(self.tmp_root, 'worker.key'),
                                  os.path.join(self.tmp_root, 'worker.crt'),
                                  os.path.join(self.tmp_root, 'root.crt'))
        else:
            self.server = gear.Server(0)
            self.client = gear.Client('client')
            self.worker = gear.Worker('worker')
            self.client.addServer('127.0.0.1', self.server.port)
            self.worker.addServer('127.0.0.1', self.server.port)

        self.client.waitForServer()
        self.worker.waitForServer()

    def create_cert(self, cn, issuer=None, signing_key=None):
        key = crypto.PKey()
        key.generate_key(crypto.TYPE_RSA, 1024)

        cert = crypto.X509()
        subject = cert.get_subject()
        subject.C = "US"
        subject.ST = "State"
        subject.L = "Locality"
        subject.O = "Org"
        subject.OU = "Org Unit"
        subject.CN = cn
        cert.set_serial_number(1)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(3600)
        cert.set_pubkey(key)
        if issuer:
            cert.set_issuer(issuer)
        else:
            cert.set_issuer(subject)
        if signing_key:
            cert.sign(signing_key, 'sha1')
        else:
            cert.sign(key, 'sha1')

        open(os.path.join(self.tmp_root, '%s.crt' % cn), 'w').write(
            crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode('utf-8'))
        open(os.path.join(self.tmp_root, '%s.key' % cn), 'w').write(
            crypto.dump_privatekey(crypto.FILETYPE_PEM, key).decode('utf-8'))

        return (subject, key)

    def test_job(self):
        self.worker.registerFunction('test')

        for jobcount in range(2):
            job = gear.Job(b'test', b'testdata')
            self.client.submitJob(job)
            self.assertNotEqual(job.handle, None)

            workerjob = self.worker.getJob()
            self.assertEqual(workerjob.handle, job.handle)
            self.assertEqual(workerjob.arguments, b'testdata')
            workerjob.sendWorkData(b'workdata')
            workerjob.sendWorkComplete()

            for count in iterate_timeout(30, "job completion"):
                if job.complete:
                    break
            self.assertTrue(job.complete)
            self.assertEqual(job.data, [b'workdata'])

    def test_worker_termination(self):
        def getJob():
            with testtools.ExpectedException(gear.InterruptedError):
                self.worker.getJob()
        self.worker.registerFunction('test')
        jobthread = threading.Thread(target=getJob)
        jobthread.daemon = True
        jobthread.start()
        self.worker.stopWaitingForJobs()


def load_tests(loader, in_tests, pattern):
    return testscenarios.load_tests_apply_scenarios(loader, in_tests, pattern)
