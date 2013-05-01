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

import logging
import os
import Queue
import re
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


class InterruptedError(Exception):
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
        self.admin_requests = []
        self.changeState("INIT")

    def changeState(self, state):
        # The state variables are provided as a convenience (and used by
        # the Worker implementation).  They aren't used or modified within
        # the connection object itself except to reset to "INIT" immediately
        # after reconnection.
        self.log.debug("Setting state to: %s" % state)
        self.state = state
        self.state_time = time.time()

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
        """Read one packet or administrative response from the server.

        Blocks until the complete packet or response is read.

        :returns: The :py:class:`Packet` or :py:class:`AdminRequest` read.
        :rtype: :py:class:`Packet` or :py:class:`AdminRequest`
        """
        packet = b''
        datalen = 0
        code = None
        ptype = None
        admin = None
        admin_request = None
        while True:
            c = self.conn.recv(1)
            if not c:
                return None
            if admin is None:
                if c == '\x00':
                    admin = False
                else:
                    admin = True
                    admin_request = self.admin_requests.pop(0)
            packet += c
            if admin:
                if admin_request.isComplete(packet):
                    return admin_request
            else:
                if len(packet) == 12:
                    code, ptype, datalen = struct.unpack('!4sii', packet)
                if len(packet) == datalen+12:
                    return Packet(code, ptype, packet[12:], connection=self)

    def sendAdminRequest(self, request):
        """Send an administrative request to the server.

        :arg AdminRequest request: The :py:class:`AdminRequest` to send.
        """
        self.admin_requests.append(request)
        self.conn.send(request.getCommand())


class AdminRequest(object):
    """Encapsulates a request (and response) sent over the
    administrative protocol.  This is a base class that may not be
    instantiated dircectly; a subclass implementing a specific command
    must be used instead.

    :arg list arguments: A list of string arguments for the command.

    The following instance attributes are available:

    **response** (str)
        The response from the server.
    **arguments** (str)
        The argument supplied with the constructor.
    **command** (str)
        The administrative command.
    **finished_re** (compiled regular expression)
        The regular expression used to determine when the response is
        complete.
    """

    log = logging.getLogger("gear.AdminRequest")

    command = None
    arguments = []
    finished_re = None
    response = None

    def __init__(self, *arguments):
        self.wait_event = threading.Event()
        self.arguments = arguments
        if type(self) == AdminRequest:
            raise NotImplementedError("AdminRequest must be subclassed")

    def __repr__(self):
        return '<gear.AdminRequest 0x%x command: %s>' % (
            id(self), self.command)

    def getCommand(self):
        cmd = self.command
        if self.arguments:
            cmd += ' ' + ' '.join(self.arguments)
        cmd += '\n'
        return cmd

    def isComplete(self, data):
        if self.finished_re.search(data):
            self.response = data
            return True
        return False

    def setComplete(self):
        self.wait_event.set()

    def waitForResponse(self):
        """Blocks until the complete response has been received from
        Gearman.
        """
        self.wait_event.wait()


class StatusAdminRequest(AdminRequest):
    """A "status" administrative request.

    The response from gearman may be found in the **response** attribute.
    """
    command = 'status'
    finished_re = re.compile('^\.\r?\n', re.M)

    def __init__(self):
        super(StatusAdminRequest, self).__init__()


class ShowJobsAdminRequest(AdminRequest):
    """A "show jobs" administrative request.

    The response from gearman may be found in the **response** attribute.
    """
    command = 'show jobs'
    finished_re = re.compile('^\.\r?\n', re.M)

    def __init__(self):
        super(ShowJobsAdminRequest, self).__init__()


class ShowUniqueJobsAdminRequest(AdminRequest):
    """A "show unique jobs" administrative request.

    The response from gearman may be found in the **response** attribute.
    """

    command = 'show unique jobs'
    finished_re = re.compile('^\.\r?\n', re.M)

    def __init__(self):
        super(ShowUniqueJobsAdminRequest, self).__init__()


class CancelJobAdminRequest(AdminRequest):
    """A "cancel job" administrative request.

    :arg str handle: The job handle to be canceled.

    The response from gearman may be found in the **response** attribute.
    """

    command = 'cancel job'
    finished_re = re.compile('^(OK|ERR .*)\r?\n', re.M)

    def __init__(self, handle):
        super(CancelJobAdminRequest, self).__init__(handle)


class VersionAdminRequest(AdminRequest):
    """A "version" administrative request.

    The response from gearman may be found in the **response** attribute.
    """

    command = 'version'
    finished_re = re.compile('^(OK .*)\r?\n', re.M)

    def __init__(self):
        super(VersionAdminRequest, self).__init__()


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


class BaseClient(object):
    log = logging.getLogger("gear.BaseClient")

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
                self.log.exception("Exception while connecting to %s" % conn)
                continue

            try:
                self._onConnect(conn)
            except Exception:
                self.log.exception("Exception while performing on-connect "
                                   "tasks for %s" % conn)
                continue
            self.connections_condition.acquire()
            self.inactive_connections.remove(conn)
            self.active_connections.append(conn)
            self.connections_condition.notifyAll()
            os.write(self.wake_write, '1\n')
            self.connections_condition.release()
            success = True
        return success

    def _onConnect(self, conn):
        # Called immediately after a successful (re-)connection
        pass

    def _lostConnection(self, conn):
        # Called as soon as a connection is detected as faulty.  Remove
        # it and return ASAP and let the connection thread deal with it.
        self.log.debug("Marking %s as disconnected" % conn)
        self.connections_condition.acquire()
        jobs = conn.pending_jobs + conn.related_jobs.values()
        self.active_connections.remove(conn)
        self.inactive_connections.append(conn)
        self.connections_condition.notifyAll()
        self.connections_condition.release()
        for job in jobs:
            self.handleDisconnect(job)

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
                        if isinstance(p, Packet):
                            self.handlePacket(p)
                        else:
                            self.handleAdminResponse(p)
                    else:
                        self.log.debug("Received no data on %s" % conn)
                        self._lostConnection(conn_dict[fd])
                        return
                else:
                    self.log.debug("Received error event on %s" % conn)
                    self._lostConnection(conn_dict[fd])
                    return

    def broadcast(self, packet):
        """Send a packet to all currently connected servers.

        :arg Packet packet: The :py:class:`Packet` to send.
        """
        connections = self.active_connections[:]
        for connection in connections:
            try:
                self.sendPacket(packet, connection)
            except Exception:
                # Error handling is all done by sendPacket
                pass

    def sendPacket(self, packet, connection):
        """Send a packet to a single connection, removing it from the
        list of active connections if that fails.

        :arg Packet packet: The :py:class:`Packet` to send.
        :arg Connection connection: The :py:class:`Connection` on
            which to send the packet.
        """
        try:
            connection.sendPacket(packet)
            return
        except Exception:
            self.log.exception("Exception while sending packet %s to %s" %
                               (packet, connection))
                # If we can't send the packet, discard the connection
            self._lostConnection(connection)
            raise

    def handleAdminResponse(self, request):
        """Handle an administrative command response from Gearman.

        This method is called whenever a response to a previously
        issued administrative command is received from one of this
        client's connections.  It normally releases the wait lock on
        the initiating AdminRequest object.

        :arg AdminRequest request: The :py:class:`AdminRequest` that
            initiated the received response.
        """

        self.log.debug("Received admin response %s" % request)
        request.setComplete()


class Client(BaseClient):
    """A Gearman client.

    You may wish to subclass this class in order to override the
    default event handlers to react to Gearman events.  Be sure to
    call the superclass event handlers so that they may perform
    job-related housekeeping.
    """

    log = logging.getLogger("gear.Client")

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
                job.connection = conn
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
        elif packet.ptype == constants.STATUS_RES:
            self.handleStatusRes(packet)

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
        job._setHandleReceived()
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
        data = packet.getArgument(1)
        if data:
            job.data.append(data)
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
        data = packet.getArgument(1)
        if data:
            job.data.append(data)
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
        data = packet.getArgument(1)
        if data:
            job.data.append(data)
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

    def handleDisconnect(self, job):
        """Handle a Gearman server disconnection.

        If the Gearman server is disconnected, this will be called for any
        jobs currently associated with the server.

        :arg Job packet: The :py:class:`Job` that was running when the server
            disconnected.
        """
        pass


class FunctionRecord(object):
    """Represents a function that should be registered with Gearman.

    This class only directly needs to be instatiated for use with
    :py:meth:`Worker.setFunctions`.  If a timeout value is supplied,
    the function will be registered with CAN_DO_TIMEOUT.

    :arg str name: The name of the function to register.
    :arg numeric timeout: The timeout value (optional).
    """
    def __init__(self, name, timeout=None):
        self.name = name
        self.timeout = timeout

    def __repr__(self):
        return '<gear.FunctionRecord 0x%x name: %s timeout: %s>' % (
            id(self), self.name, self.timeout)


class Worker(BaseClient):
    """A Gearman worker.

    :arg str worker_id: The worker ID to provide to Gearman (will
        appear in administrative command output).
    """

    log = logging.getLogger("gear.Worker")

    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.functions = {}
        self.job_lock = threading.Lock()
        self.waiting_for_jobs = 0
        self.job_queue = Queue.Queue()
        super(Worker, self).__init__()

    def registerFunction(self, name, timeout=None):
        """Register a function with Gearman.

        If a timeout value is supplied, the function will be
        registered with CAN_DO_TIMEOUT.

        :arg str name: The name of the function to register.
        :arg numeric timeout: The timeout value (optional).
        """
        self.functions[name] = FunctionRecord(name, timeout)
        if timeout:
            self._sendCanDoTimeout(name, timeout)
        else:
            self._sendCanDo(name)

    def unRegisterFunction(self, name):
        """Remove a function from Gearman's registry.

        :arg str name: The name of the function to remove.
        """
        del self.functions[name]
        self._sendCantDo(name)

    def setFunctions(self, functions):
        """Replace the set of functions registered with Gearman.

        Accepts a list of :py:class:`FunctionRecord` objects which
        represents the complete set of functions that should be
        registered with Gearman.  Any existing functions will be
        unregistered and these registered in their place.  If the
        empty list is supplied, then the Gearman registered function
        set will be cleared.

        :arg list functions: A list of :py:class:`FunctionRecord` objects.
        """

        self._sendResetAbilities()
        self.functions = {}
        for f in functions:
            if not isinstance(f, FunctionRecord):
                raise InvalidDataError(
                    "An iterable of FunctionRecords is required.")
            self.functions[f.name] = f
        for f in self.functions.values():
            if f.timeout:
                self._sendCanDoTimeout(f.name, f.timeout)
            else:
                self._sendCanDo(f.name)

    def _sendCanDo(self, name):
        p = Packet(constants.REQ, constants.CAN_DO, name)
        self.broadcast(p)

    def _sendCanDoTimeout(self, name, timeout):
        data = name + '\x00' + timeout
        p = Packet(constants.REQ, constants.CAN_DO_TIMEOUT, data)
        self.broadcast(p)

    def _sendCantDo(self, name):
        p = Packet(constants.REQ, constants.CANT_DO, name)
        self.broadcast(p)

    def _sendResetAbilities(self, name):
        p = Packet(constants.REQ, constants.RESET_ABILITIES, '')
        self.broadcast(p)

    def _sendPreSleep(self, connection):
        p = Packet(constants.REQ, constants.PRE_SLEEP, '')
        self.sendPacket(p, connection)

    def _sendGrabJobUniq(self, connection=None):
        p = Packet(constants.REQ, constants.GRAB_JOB_UNIQ, '')
        if connection:
            self.sendPacket(p, connection)
        else:
            self.broadcast(p)

    def _onConnect(self, conn):
        # Called immediately after a successful (re-)connection
        p = Packet(constants.REQ, constants.SET_CLIENT_ID, self.worker_id)
        conn.sendPacket(p)
        for f in self.functions.values():
            if f.timeout:
                data = f.name + '\x00' + f.timeout
                p = Packet(constants.REQ, constants.CAN_DO_TIMEOUT, data)
            else:
                p = Packet(constants.REQ, constants.CAN_DO, f.name)
        conn.sendPacket(p)
        conn.changeState("IDLE")
        # Any exceptions will be handled by the calling function, and the
        # connection will not be put into the pool.

    def _updateStateMachines(self):
        connections = self.active_connections[:]

        for connection in connections:
            if (connection.state == "IDLE" and self.waiting_for_jobs > 0):
                self._sendGrabJobUniq(connection)
                connection.changeState("GRAB_WAIT")
            if (connection.state != "IDLE" and self.waiting_for_jobs < 1):
                connection.changeState("IDLE")

    def getJob(self):
        """Get a job from Gearman.

        Blocks until a job is received.  This method is re-entrant, so
        it is safe to call this method on a single worker from
        multiple threads.  In that case, one of them at random will
        receive the job assignment.

        :returns: The :py:class:`WorkerJob` assigned.
        :rtype: :py:class:`WorkerJob`.
        :raises InterruptedError: If interrupted (by
            :py:meth:`stopWaitingForJobs`) before a job is received.
        """
        self.job_lock.acquire()
        try:
            self.waiting_for_jobs += 1
            self.log.debug("Get job; number of threads waiting for jobs: %s" %
                           self.waiting_for_jobs)

            try:
                job = self.job_queue.get(False)
            except Queue.Empty:
                job = None

            if not job:
                self._updateStateMachines()
        finally:
            self.job_lock.release()

        if not job:
            job = self.job_queue.get()

        self.log.debug("Received job: %s" % job)
        if job is None:
            raise InterruptedError()
        return job

    def stopWaitingForJobs(self):
        """Interrupts all running :py:meth:`getJob` calls, which will raise
        an exception.
        """

        self.job_lock.acquire()
        while True:
            connections = self.active_connections[:]
            now = time.time()
            ok = True
            for connection in connections:
                if connection.state == "GRAB_WAIT":
                    if now - connection.state_time > 5:
                        self._lostConnection(connection)
                    else:
                        ok = False
            if ok:
                break
            else:
                self.job_lock.release()
                time.sleep(0.1)
                self.job_lock.acquire()

        while self.waiting_for_jobs > 0:
            self.waiting_for_jobs -= 1
            self.job_queue.put(None)

        self._updateStateMachines()
        self.job_lock.release()

    def handleNoop(self, packet):
        """Handle a NOOP packet.

        Sends a GRAB_JOB_UNIQ packet on the same connection.
        GRAB_JOB_UNIQ will return jobs regardless of whether they have
        been specified with a unique identifier when submitted.  If
        they were not, then :py:attr:`WorkerJob.unique` attribute
        will be None.

        :arg Packet packet: The :py:class:`Packet` that was received.
        """

        self.job_lock.acquire()
        try:
            if packet.connection.state == "SLEEP":
                self.log.debug("Sending GRAB_JOB_UNIQ")
                self._sendGrabJobUniq(packet.connection)
                packet.connection.changeState("GRAB_WAIT")
            else:
                self.log.debug("Received unexpecetd NOOP packet on %s" %
                               packet.connection)
        finally:
            self.job_lock.release()

    def handleNoJob(self, packet):
        """Handle a NO_JOB packet.

        Sends a PRE_SLEEP packet on the same connection.

        :arg Packet packet: The :py:class:`Packet` that was received.
        """
        self.job_lock.acquire()
        try:
            if packet.connection.state == "GRAB_WAIT":
                self.log.debug("Sending PRE_SLEEP")
                self._sendPreSleep(packet.connection)
                packet.connection.changeState("SLEEP")
            else:
                self.log.debug("Received unexpecetd NO_JOB packet on %s" %
                               packet.connection)
        finally:
            self.job_lock.release()

    def handleJobAssignUnique(self, packet):
        """Handle a JOB_ASSIGN_UNIQ packet.

        Adds a WorkerJob to the internal queue to be picked up by any
        threads waiting in :py:meth:`getJob`.

        :arg Packet packet: The :py:class:`Packet` that was received.
        """

        handle = packet.getArgument(0)
        name = packet.getArgument(1)
        unique = packet.getArgument(2)
        if unique == '':
            unique = None
        arguments = packet.getArgument(3)
        job = WorkerJob(handle, name, arguments, unique)
        job.connection = packet.connection

        self.job_lock.acquire()
        try:
            packet.connection.changeState("IDLE")
            self.waiting_for_jobs -= 1
            self.log.debug("Job assigned; number of threads waiting for "
                           "jobs: %s" % self.waiting_for_jobs)
            self.job_queue.put(job)

            self._updateStateMachines()
        finally:
            self.job_lock.release()

    def handlePacket(self, packet):
        """Handle a packet received from a Gearman server.

        This method is called whenever a packet is received from any
        of this worker's connections.  It normally calls the handle
        method appropriate for the specific packet.

        :arg Packet packet: The :py:class:`Packet` that was received.
        """

        self.log.debug("Received packet %s" % packet)

        if packet.ptype == constants.JOB_ASSIGN_UNIQ:
            self.handleJobAssignUnique(packet)
        elif packet.ptype == constants.NO_JOB:
            self.handleNoJob(packet)
        elif packet.ptype == constants.NOOP:
            self.handleNoop(packet)


class BaseJob(object):
    log = logging.getLogger("gear.Job")

    def __init__(self, name, arguments, unique=None, handle=None):
        self.name = name
        self.arguments = arguments
        self.unique = unique
        self.handle = handle
        self.connection = None

    def __repr__(self):
        return '<gear.Job 0x%x handle: %s name: %s unique: %s>' % (
            id(self), self.handle, self.name, self.unique)


class Job(BaseJob):
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
    **data** (list of byte-arrays)
        The result data returned from Gearman.  Each packet appends an
        element to the list.  Depending on the nature of the data, the
        elements may need to be concatenated before use.
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
    **connection** (:py:class:`Connection` or None)
        The connection associated with the job.  Only set after the job
        has been submitted to a Gearman server.
    """

    log = logging.getLogger("gear.Job")

    def __init__(self, name, arguments, unique=None):
        super(Job, self).__init__(name, arguments, unique)
        self._wait_event = threading.Event()
        self.data = []
        self.exception = None
        self.warning = False
        self.complete = False
        self.failure = False
        self.numerator = None
        self.denominator = None
        self.fraction_complete = None
        self.known = None
        self.running = None

    def _setHandleReceived(self):
        self._wait_event.set()

    def waitForHandle(self, timeout=None):
        """Wait until the Job handle has been recieved from Gearman.

        :arg int timeout: If not None, return after this many seconds if no
            handle has been received (default: None).
        """

        self._wait_event.wait(timeout)


class WorkerJob(BaseJob):
    """A job that Gearman has assigned to a Worker.  Not intended to
    be instantiated directly, but rather returned by
    :py:meth:`Worker.getJob`.

    :arg str handle: The job handle assigned by gearman.
    :arg str name: The name of the job.
    :arg str arguments: The opaque data blob passed to the worker
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
    **handle** (str)
        The Gearman job handle.
    **connection** (:py:class:`Connection` or None)
        The connection associated with the job.  Only set after the job
        has been submitted to a Gearman server.
    """

    log = logging.getLogger("gear.WorkerJob")

    def __init__(self, handle, name, arguments, unique=None):
        super(WorkerJob, self).__init__(name, arguments, unique, handle)

    def sendWorkData(self, data=''):
        """Send a WORK_DATA packet to the client.

        :arg str data: The data to be sent to the client (optional).
        """

        data = self.handle + '\x00' + data
        p = Packet(constants.REQ, constants.WORK_DATA, data)
        self.connection.sendPacket(p)

    def sendWorkWarning(self, data=''):
        """Send a WORK_WARNING packet to the client.

        :arg str data: The data to be sent to the client (optional).
        """

        data = self.handle + '\x00' + data
        p = Packet(constants.REQ, constants.WORK_WARNING, data)
        self.connection.sendPacket(p)

    def sendWorkStatus(self, numerator, denominator):
        """Send a WORK_STATUS packet to the client.

        Sends a numerator and denominator that together represent the
        fraction complete of the job.

        :arg numeric numerator: The numerator of the fraction complete.
        :arg numeric denominator: The denominator of the fraction complete.
        """

        data = (self.handle + '\x00' +
                str(numerator) + '\x00' + str(denominator))
        p = Packet(constants.REQ, constants.WORK_STATUS, data)
        self.connection.sendPacket(p)

    def sendWorkComplete(self, data=''):
        """Send a WORK_COMPLETE packet to the client.

        :arg str data: The data to be sent to the client (optional).
        """

        data = self.handle + '\x00' + data
        p = Packet(constants.REQ, constants.WORK_COMPLETE, data)
        self.connection.sendPacket(p)

    def sendWorkFail(self):
        "Send a WORK_FAIL packet to the client."

        p = Packet(constants.REQ, constants.WORK_FAIL, self.handle)
        self.connection.sendPacket(p)

    def sendWorkException(self, data=''):
        """Send a WORK_EXCEPTION packet to the client.

        :arg str data: The exception data to be sent to the client (optional).
        """

        data = self.handle + '\x00' + data
        p = Packet(constants.REQ, constants.WORK_EXCEPTION, data)
        self.connection.sendPacket(p)
