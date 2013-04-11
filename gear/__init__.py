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

"""
This module implements an asynchronous event-driven interface to
Gearman.  Currently, only the client interface is implemented.  To
use, instantiate a :py:class:`Client`, and submit a :py:class:`Job`.
For example::

    import gear
    client = gear.Client()
    client.addServer('gearman.example.com')
    client.waitForServer()  # Wait for at least one server to be connected

    job = gear.Job("reverse", "test string")
    client.submitJob(job)

The waitForServer() call is only necessary when running in a
synchronous context.  When running asynchronously, it is probably more
desirable to omit that call and instead handle the
:py:class:`NoConnectedServersError` exception that submitJob may
raise if no servers are connected at the time.

When Gearman returns data to the client, the :py:class:`Job` object is
updated immediately.  Event handlers are called on the
:py:class:`Client` object so that subclasses have ample facilities for
reacting to events synchronously.

API Reference
=============

Constants
---------
These constants are used by public API classes.

.. py:data:: PRECEDENCE_NORMAL

   Normal job precedence.

.. py:data:: PRECEDENCE_LOW

   Low job precedence.

.. py:data:: PRECEDENCE_HIGH

   High job precedence.

Class Reference
---------------

"""

import logging
import os
import select
import socket
import struct
import threading
import time

from gear import constants

PRECEDENCE_NORMAL = 0
PRECEDENCE_LOW = 1
PRECEDENCE_HIGH = 2


class ConnectionError(Exception):
    pass


class InvalidDataError(Exception):
    pass


class ConfigurationError(Exception):
    pass


class NoConnectedServersError(Exception):
    pass


class UnknownJobError(Exception):
    pass


class Connection(object):
    """A Connection to a Gearman Server."""

    log = logging.getLogger("gear.Connection")

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self._init()

    def _init(self):
        self.conn = None
        self.connected = False
        self.pending_jobs = []
        self.related_jobs = {}

    def __repr__(self):
        return '<gear.Connection 0x%x host: %s port: %s>' % (
            id(self), self.host, self.port)

    def connect(self):
        """Open a connection to the server.

        :raises ConnectionError: If unable to open the socket.
        """

        self.log.debug("Connecting to %s port %s" % (self.host, self.port))
        s = None
        for res in socket.getaddrinfo(self.host, self.port,
                                      socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except socket.error:
                s = None
                continue
            try:
                s.connect(sa)
            except socket.error:
                s.close()
                s = None
                continue
            break
        if s is None:
            self.log.debug("Error connecting to %s port %s" % (
                self.host, self.port))
            raise ConnectionError("Unable to open socket")
        self.log.debug("Connected to %s port %s" % (self.host, self.port))
        self.conn = s
        self.connected = True

    def disconnect(self):
        """Disconnect from the server and remove all associated state
        data.
        """

        self.log.debug("Disconnected from %s port %s" % (self.host, self.port))
        self._init()

    def reconnect(self):
        """Disconnect from and reconnect to the server, removing all
        associated state data.
        """
        self.disconnect()
        self.connect()

    def sendPacket(self, packet):
        """Send a packet to the server.

        :arg Packet packet: The :py:class:`Packet` to send.
        """
        self.conn.send(packet.toBinary())

    def readPacket(self):
        """Read one packet from the server.

        Blocks until the complete packet is read.
        :returns: The :py:class:`Packet` read.
        :rtype: :py:class:`Packet`
        """
        packet = b''
        datalen = 0
        code = None
        ptype = None
        while True:
            c = self.conn.recv(1)
            if not c:
                return None
            packet += c
            if len(packet) == 12:
                code, ptype, datalen = struct.unpack('!4sii', packet)
            elif len(packet) == datalen+12:
                return Packet(code, ptype, packet[12:], connection=self)


class Packet(object):
    """A data packet received from or to be sent over a
    :py:class:`Connection`.

    :arg str code: The Gearman magic code (:py:data:`constants.REQ` or
        :py:data:`constants.RES`)
    :arg str ptype: The packet type (one of the packet types in constasts).
    :arg str data: The data portion of the packet.
    :arg str connection: The connection on which the packet was received
        (optional).
    :raises InvalidDataError: If the magic code is unknown.
    """

    log = logging.getLogger("gear.Packet")

    def __init__(self, code, ptype, data, connection=None):
        if code[0] != '\x00':
            raise InvalidDataError("First byte of packet must be 0")
        self.code = code
        self.ptype = ptype
        self.data = data
        self.connection = connection

    def __repr__(self):
        ptype = constants.types.get(self.ptype, 'UNKNOWN')
        return '<gear.Packet 0x%x type: %s>' % (id(self), ptype)

    def toBinary(self):
        """Return a Gearman wire protocol binary representation of the packet.

        :returns: The packet in binary form.
        :rtype: str
        """
        b = struct.pack('!4sii', self.code, self.ptype, len(self.data))
        b += self.data
        return b

    def getArgument(self, index):
        """Get the nth argument from the packet data.

        :arg int index: The argument index to look up.
        :returns: The argument value.
        :rtype: str
        """

        return self.data.split('\x00')[index]

    def getJob(self):
        """Get the :py:class:`Job` associated with the job handle in
        this packet.

        :returns: The :py:class:`Job` for this packet.
        :rtype: str
        :raises UnknownJobError: If the job is not known.
        """
        handle = self.getArgument(0)
        job = self.connection.related_jobs.get(handle)
        if not job:
            raise UnknownJobError()
        return job


class Client(object):
    """A Gearman client.

    You may wish to subclass this class in order to override the
    default event handlers to react to Gearman events.  Be sure to
    call the superclass event handlers so that they may perform
    job-related housekeeping.
    """

    log = logging.getLogger("gear.Client")

    def __init__(self):
        self.active_connections = []
        self.inactive_connections = []

        self.connection_index = -1
        # A lock and notification mechanism to handle not having any
        # current connections
        self.connections_condition = threading.Condition()

        # A pipe to wake up the poll loop in case it needs to restart
        self.wake_read, self.wake_write = os.pipe()

        self.poll_thread = threading.Thread(name="Gearman client poll",
                                            target=self._doPollLoop)
        self.poll_thread.start()
        self.connect_thread = threading.Thread(name="Gearman client connect",
                                               target=self._doConnectLoop)
        self.connect_thread.start()

    def __repr__(self):
        return '<gear.Client 0x%x>' % id(self)

    def addServer(self, host, port=4730):
        """Add a server to the client's connection pool.

        Any number of Gearman servers may be added to a client.  The
        client will connect to all of them and send jobs to them in a
        round-robin fashion.  When servers are disconnected, the
        client will automatically remove them from the pool,
        continuously try to reconnect to them, and return them to the
        pool when reconnected.  New servers may be added at any time.

        This is a non-blocking call that will return regardless of
        whether the initial connection succeeded.  If you need to
        ensure that a connection is ready before proceeding, see
        :py:meth:`waitForServer`.

        :arg str host: The hostname or IP address of the server.
        :arg int port: The port on which the gearman server is listening.
        :raises ConfigurationError: If the host/port combination has
            already been added to the client.
        """

        self.log.debug("Adding server %s port %s" % (host, port))

        self.connections_condition.acquire()
        try:
            for conn in self.active_connections + self.inactive_connections:
                if conn.host == host and conn.port == port:
                    raise ConfigurationError("Host/port already specified")
            conn = Connection(host, port)
            self.inactive_connections.append(conn)
            self.connections_condition.notifyAll()
        finally:
            self.connections_condition.release()

    def waitForServer(self):
        """Wait for at least one server to be connected.

        Block until at least one gearman server is connected.
        """
        connected = False
        while True:
            self.connections_condition.acquire()
            while not self.active_connections:
                self.log.debug("Waiting for at least one active connection")
                self.connections_condition.wait()
            if self.active_connections:
                self.log.debug("Active connection found")
                connected = True
            self.connections_condition.release()
            if connected:
                return

    def _doConnectLoop(self):
        # Outer run method of the reconnection thread
        while True:
            self.connections_condition.acquire()
            while not self.inactive_connections:
                self.log.debug("Waiting for change in available servers "
                               "to reconnect")
                self.connections_condition.wait()
            self.connections_condition.release()
            self.log.debug("Checking if servers need to be reconnected")
            try:
                if not self._connectLoop():
                    # Nothing happened
                    time.sleep(2)
            except Exception:
                self.log.exception("Exception in connect loop:")

    def _connectLoop(self):
        # Inner method of the reconnection loop, triggered by
        # a connection change
        success = False
        for conn in self.inactive_connections[:]:
            self.log.debug("Trying to reconnect %s" % conn)
            try:
                conn.reconnect()
            except ConnectionError:
                self.log.debug("Unable to connect to %s" % conn)
                continue
            except Exception:
                self.log.error("Exception while connecting to %s" % conn)
                continue
            self.connections_condition.acquire()
            self.inactive_connections.remove(conn)
            self.active_connections.append(conn)
            self.connections_condition.notifyAll()
            os.write(self.wake_write, '1\n')
            self.connections_condition.release()
            success = True
        return success

    def _lostConnection(self, conn):
        # Called as soon as a connection is detected as faulty.  Remove
        # it and return ASAP and let the connection thread deal with it.
        self.log.debug("Marking %s as disconnected" % conn)
        self.connections_condition.acquire()
        self.active_connections.remove(conn)
        self.inactive_connections.append(conn)
        self.connections_condition.notifyAll()
        self.connections_condition.release()

    def getConnection(self):
        """Return a connected server.

        Finds the next scheduled connected server in the round-robin
        rotation and returns it.  It is not usually necessary to use
        this method external to the library, as more consumer-oriented
        methods such as submitJob already use it internally, but is
        available nonetheless if necessary.

        :returns: The next scheduled :py:class:`Connection` object.
        :rtype: :py:class:`Connection`
        :raises NoConnectedServersError: If there are not currently
            connected servers.
        """

        conn = None
        try:
            self.connections_condition.acquire()
            if not self.active_connections:
                raise NoConnectedServersError("No connected Gearman servers")

            self.connection_index += 1
            if self.connection_index >= len(self.active_connections):
                self.connection_index = 0
            conn = self.active_connections[self.connection_index]
        finally:
            self.connections_condition.release()
        return conn

    def _doPollLoop(self):
        # Outer run method of poll thread.
        while True:
            self.connections_condition.acquire()
            while not self.active_connections:
                self.log.debug("Waiting for change in available servers "
                               "to poll")
                self.connections_condition.wait()
            self.connections_condition.release()
            try:
                self._pollLoop()
            except Exception:
                self.log.exception("Exception in poll loop:")

    def _pollLoop(self):
        # Inner method of poll loop
        self.log.debug("Preparing to poll")
        poll = select.poll()
        bitmask = (select.POLLIN | select.POLLERR |
                   select.POLLHUP | select.POLLNVAL)
        # Reverse mapping of fd -> connection
        conn_dict = {}
        for conn in self.active_connections:
            poll.register(conn.conn.fileno(), bitmask)
            conn_dict[conn.conn.fileno()] = conn
        # Register the wake pipe so that we can break if we need to
        # reconfigure connections
        poll.register(self.wake_read, bitmask)
        while True:
            self.log.debug("Polling %s connections" %
                           len(self.active_connections))
            ret = poll.poll()
            for fd, event in ret:
                if fd == self.wake_read:
                    self.log.debug("Woken by pipe")
                    while True:
                        if os.read(self.wake_read, 1) == '\n':
                            break
                    return
                if event & select.POLLIN:
                    self.log.debug("Processing input on %s" % conn)
                    p = conn_dict[fd].readPacket()
                    if p:
                        self.handlePacket(p)
                    else:
                        self.log.debug("Received no data on %s" % conn)
                        self._lostConnection(conn_dict[fd])
                        return
                else:
                    self.log.debug("Received error event on %s" % conn)
                    self._lostConnection(conn_dict[fd])
                    return

    def submitJob(self, job, background=False, precedence=PRECEDENCE_NORMAL):
        """Submit a job to a Gearman server.

        Submits the provided job to the next server in this client's
        round-robin connection pool.

        If the job is a foreground job, updates will be made to the
        supplied :py:class:`Job` object as they are received.

        :arg Job job: The :py:class:`Job` to submit.
        :arg bool background: Whether the job should be backgrounded.
        :arg int precedence: Whether the job should have normal, low, or
            high precedence.  One of :py:data:`PRECEDENCE_NORMAL`,
            :py:data:`PRECEDENCE_LOW`, or :py:data:`PRECEDENCE_HIGH`
        :raises ConfigurationError: If an invalid precendence value
            is supplied.
        """
        if job.unique is None:
            unique = ''
        else:
            unique = job.unique
        data = '%s\x00%s\x00%s' % (job.name, unique, job.arguments)
        if background:
            if precedence == PRECEDENCE_NORMAL:
                cmd = constants.SUBMIT_JOB_BG
            elif precedence == PRECEDENCE_LOW:
                cmd = constants.SUBMIT_JOB_LOW_BG
            elif precedence == PRECEDENCE_HIGH:
                cmd = constants.SUBMIT_JOB_HIGH_BG
            else:
                raise ConfigurationError("Invalid precedence value")
        else:
            if precedence == PRECEDENCE_NORMAL:
                cmd = constants.SUBMIT_JOB
            elif precedence == PRECEDENCE_LOW:
                cmd = constants.SUBMIT_JOB_LOW
            elif precedence == PRECEDENCE_HIGH:
                cmd = constants.SUBMIT_JOB_HIGH
            else:
                raise ConfigurationError("Invalid precedence value")
        p = Packet(constants.REQ, cmd, data)
        while True:
            conn = self.getConnection()
            conn.pending_jobs.append(job)
            try:
                conn.sendPacket(p)
                return
            except Exception:
                self.log.exception("Exception while submitting job %s to %s" %
                                   (job, conn))
                conn.pending_jobs.remove(job)
                # If we can't send the packet, discard the connection and
                # try again
                self._lostConnection(conn)

    def handlePacket(self, packet):
        """Handle a packet received from a Gearman server.

        This method is called whenever a packet is received from any
        of this client's connections.  It normally calls the handle
        method appropriate for the specific packet.

        :arg Packet packet: The :py:class:`Packet` that was received.
        """

        self.log.debug("Received packet %s" % packet)
        if packet.ptype == constants.JOB_CREATED:
            self.handleJobCreated(packet)
        elif packet.ptype == constants.WORK_COMPLETE:
            self.handleWorkComplete(packet)
        elif packet.ptype == constants.WORK_FAIL:
            self.handleWorkFail(packet)
        elif packet.ptype == constants.WORK_EXCEPTION:
            self.handleWorkException(packet)
        elif packet.ptype == constants.WORK_DATA:
            self.handleWorkData(packet)
        elif packet.ptype == constants.WORK_WARNING:
            self.handleWorkWarning(packet)
        elif packet.ptype == constants.WORK_STATUS:
            self.handleWorkStatus(packet)

    def handleJobCreated(self, packet):
        """Handle a JOB_CREATED packet.

        Updates the appropriate :py:class:`Job` with the newly
        returned job handle.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.connection.pending_jobs.pop(0)
        job.handle = packet.data
        packet.connection.related_jobs[job.handle] = job
        self.log.debug("Job created; handle: %s" % job.handle)
        return job

    def handleWorkComplete(self, packet):
        """Handle a WORK_COMPLETE packet.

        Updates the referenced :py:class:`Job` with the returned data
        and removes it from the list of jobs associated with the
        connection.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.getJob()
        job.data += packet.getArgument(1)
        job.complete = True
        job.failure = False
        del packet.connection.related_jobs[job.handle]
        self.log.debug("Job complete; handle: %s data: %s" %
                       (job.handle, job.data))
        return job

    def handleWorkFail(self, packet):
        """Handle a WORK_FAIL packet.

        Updates the referenced :py:class:`Job` with the returned data
        and removes it from the list of jobs associated with the
        connection.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.getJob()
        job.complete = True
        job.failure = True
        del packet.connection.related_jobs[job.handle]
        self.log.debug("Job failed; handle: %s" % job.handle)
        return job

    def handleWorkException(self, packet):
        """Handle a WORK_Exception packet.

        Updates the referenced :py:class:`Job` with the returned data
        and removes it from the list of jobs associated with the
        connection.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.getJob()
        job.exception = packet.getArgument(1)
        job.complete = True
        job.failure = True
        del packet.connection.related_jobs[job.handle]
        self.log.debug("Job exception; handle: %s data: %s" %
                       (job.handle, job.exception))
        return job

    def handleWorkData(self, packet):
        """Handle a WORK_DATA packet.

        Updates the referenced :py:class:`Job` with the returned data.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.getJob()
        job.data += packet.getArgument(1)
        self.log.debug("Job data; handle: %s data: %s" %
                       (job.handle, job.data))
        return job

    def handleWorkWarning(self, packet):
        """Handle a WORK_WARNING packet.

        Updates the referenced :py:class:`Job` with the returned data.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.getJob()
        job.data += packet.getArgument(1)
        job.warning = True
        self.log.debug("Job warning; handle: %s data: %s" %
                       (job.handle, job.data))
        return job

    def handleWorkStatus(self, packet):
        """Handle a WORK_STATUS packet.

        Updates the referenced :py:class:`Job` with the returned data.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.getJob()
        job.numerator = packet.getArgument(1)
        job.denominator = packet.getArgument(2)
        try:
            job.fraction_complete = float(job.numerator)/float(job.denominator)
        except Exception:
            job.fraction_complete = None
        self.log.debug("Job status; handle: %s complete: %s/%s" %
                       (job.handle, job.numerator, job.denominator))
        return job

    def handleStatusRes(self, packet):
        """Handle a STATUS_RES packet.

        Updates the referenced :py:class:`Job` with the returned data.

        :arg Packet packet: The :py:class:`Packet` that was received.
        :returns: The :py:class:`Job` object associated with the job request.
        :rtype: :py:class:`Job`
        """

        job = packet.getJob()
        job.known = (packet.getArgument(1) == 1)
        job.running = (packet.getArgument(2) == 1)
        job.numerator = packet.getArgument(3)
        job.denominator = packet.getArgument(4)
        try:
            job.fraction_complete = float(job.numerator)/float(job.denominator)
        except Exception:
            job.fraction_complete = None
        return job


class Job(object):
    """A job to run or being run by Gearman.

    :arg str name: The name of the job.
    :arg str arguments: The opaque data blob to be passed to the worker
        as arguments.
    :arg str unique: A string to uniquely identify the job to Gearman
        (optional).

    The following instance attributes are available:

    **name** (str)
        The name of the job.
    **arguments** (str)
        The opaque data blob passed to the worker as arguments.
    **unique** (str or None)
        The unique ID of the job (if supplied).
    **handle** (str or None)
        The Gearman job handle.  None if no job handle has been received yet.
    **data** (str)
        The result data returned from Gearman.  The empty string until
        data have been received.
    **exception** (str or None)
        Exception information returned from Gearman.  None if no exception
        has been received.
    **warning** (bool)
        Whether the worker has reported a warning.
    **complete** (bool)
        Whether the job is complete.
    **failure** (bool)
        Whether the job has failed.  Only set when complete is True.
    **numerator** (str or None)
        The numerator of the completion ratio reported by the worker.
        Only set when a status update is sent by the worker.
    **denominator** (str or None)
        The denominator of the completion ratio reported by the
        worker.  Only set when a status update is sent by the worker.
    **fraction_complete** (float or None)
        The fractional complete ratio reported by the worker.  Only set when
        a status update is sent by the worker.
    **known** (bool or None)
        Whether the job is known to Gearman.  Only set by handleStatusRes() in
        response to a getStatus() query.
    **running** (bool or None)
        Whether the job is running.  Only set by handleStatusRes() in
        response to a getStatus() query.
    """

    log = logging.getLogger("gear.Job")

    def __init__(self, name, arguments, unique=None):
        self.name = name
        self.arguments = arguments
        self.unique = unique
        self.handle = None
        self.data = b''
        self.exception = None
        self.warning = False
        self.complete = False
        self.failure = False
        self.numerator = None
        self.denominator = None
        self.fraction_complete = None
        self.known = None
        self.running = None

    def __repr__(self):
        return '<gear.Job 0x%x handle: %s name: %s unique: %s>' % (
            id(self), self.handle, self.name, self.unique)
