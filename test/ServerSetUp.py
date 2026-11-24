#!/usr/bin/env python
#   This file is part of nexdatas - Tango Server for FIO blissdata writer
#
#    Copyright (C) 2026 DESY, Jan Kotanski <jkotan@mail.desy.de>
#
#    nexdatas is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    nexdatas is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with nexdatas.  If not, see <http://www.gnu.org/licenses/>.
# \package test nexdatas
# \file ServerSetUp.py
# class with server settings
#
import time

try:
    import tango
except Exception:
    import PyTango as tango

from tango.test_context import DeviceTestContext

from fioblisswriter.FIOBlissWriter import FIOBlissWriter


# test fixture
class ServerSetUp(object):

    # constructor
    # \brief defines server parameters
    # \param device device name
    # \param instance server instance name
    # \param properties device properties
    def __init__(self, device="test/fioblisswriter/01", instance="FIOTEST",
                 properties=None):
        # information about tango writer
        #: (:obj:`str`) device name
        self.device = device
        #: (:obj:`str`) server instance name
        self.instance = instance
        #: (:obj:`str`) server class name
        self.server_class = "FIOBlissWriter"
        #: (:obj:`dict`) device properties.
        #  A dead redis url is used by default so that the embedded writer
        #  service records a connection error instead of writing real data.
        self.properties = properties if properties is not None else {
            "RedisUrl": "redis://localhost:6380",
            "Session": "testsession",
            "NextScanTimeout": 1,
            "PointSleepTime": 0.01,
        }
        #: (:class:`tango.test_context.DeviceTestContext`) device context
        self._context = None
        #: (:class:`tango.DeviceProxy`) device proxy
        self.dp = None

    # test starter
    # \brief Common set up of Tango Server
    def setUp(self):
        self._context = DeviceTestContext(
            FIOBlissWriter,
            device_name=self.device,
            properties=self.properties,
            process=True)
        self._context.start()
        self.dp = self._context.device

        # wait for the device to be exported and ON
        found = False
        cnt = 0
        while not found and cnt < 1000:
            try:
                if self.dp.state() == tango.DevState.ON:
                    found = True
            except Exception:
                found = False
            if not found:
                time.sleep(0.01)
            cnt += 1

    # test closer
    # \brief Common tear down of Tango Server
    def tearDown(self):
        if self._context is not None:
            self._context.stop()
            self._context = None
            self.dp = None
