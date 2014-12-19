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

import time

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
    def setUp(self):
        super(TestFunctional, self).setUp()
        self.server = gear.Server(0)

    def test_job(self):
        client = gear.Client('testclient')
        client.addServer('127.0.0.1', self.server.port)
        client.waitForServer()

        worker = gear.Worker('testworker')
        worker.addServer('127.0.0.1', self.server.port)
        worker.waitForServer()
        worker.registerFunction('test')

        for jobcount in range(2):
            job = gear.Job('test', 'testdata')
            client.submitJob(job)
            self.assertNotEqual(job.handle, None)

            workerjob = worker.getJob()
            self.assertEqual(workerjob.handle, job.handle)
            self.assertEqual(workerjob.arguments, 'testdata')
            workerjob.sendWorkData('workdata')
            workerjob.sendWorkComplete()

            for count in iterate_timeout(30, "job completion"):
                if job.complete:
                    break
            self.assertTrue(job.complete)
            self.assertEqual(job.data, ['workdata'])
