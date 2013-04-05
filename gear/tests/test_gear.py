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

import testscenarios

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


def load_tests(loader, in_tests, pattern):
    return testscenarios.load_tests_apply_scenarios(loader, in_tests, pattern)
