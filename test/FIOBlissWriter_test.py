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
# \file FIOBlissWriter_test.py
# unittests for the FIOBlissWriter Tango device running in a test context
#
import unittest
import sys
import time

try:
    import tango
except Exception:
    import PyTango as tango

try:
    from . import ServerSetUp
except Exception:
    import ServerSetUp


if sys.version_info > (3,):
    long = int


# test fixture
class FIOBlissWriterTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        #: (:class:`ServerSetUp.ServerSetUp`) tango server fixture
        self._sv = ServerSetUp.ServerSetUp()

        self.seed = long(time.time() * 256)  # use fractional seconds

    # test starter
    # \brief Common set up of Tango Server
    def setUp(self):
        print("\nsetting up...")
        print("SEED = %s" % self.seed)
        self._sv.setUp()

    # test closer
    # \brief Common tear down of Tango Server
    def tearDown(self):
        print("tearing down ...")
        self._sv.tearDown()

    # Exception tester
    # \param exception expected exception
    # \param method called method
    # \param args list with method arguments
    # \param kwargs dictionary with method arguments
    def myAssertRaise(self, exception, method, *args, **kwargs):
        try:
            error = False
            method(*args, **kwargs)
        except Exception:
            error = True
        self.assertEqual(error, True)

    # waits until the Errors attribute gets populated
    # \param timeout maximal waiting time in seconds
    # \returns list of error messages
    def __wait_for_errors(self, timeout=5.0):
        dp = self._sv.dp
        end = time.time() + timeout
        errs = list(dp.Errors or [])
        while not errs and time.time() < end:
            time.sleep(0.02)
            errs = list(dp.Errors or [])
        return errs

    # state test
    # \brief It tests that the device comes up in the ON state
    def test_state(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        self.assertEqual(self._sv.dp.state(), tango.DevState.ON)

    # status test
    # \brief It tests the device status string
    def test_status(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        dp = self._sv.dp
        self.assertTrue(dp.status().startswith("FIOBlissWriter"))
        # without a reachable redis the writer service fails to start
        self.__wait_for_errors()
        self.assertEqual(dp.status(), "FIOBlissWriter is FAILED")

    # Errors attribute test
    # \brief It tests reading the Errors spectrum attribute
    def test_Errors(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        errs = self.__wait_for_errors()
        # the redis connection failure is reported through the attribute
        self.assertTrue(len(errs) >= 1)
        self.assertTrue(
            any("connect" in e.lower() for e in errs))

    # commands test
    # \brief It tests the Start and Stop commands
    def test_Start_Stop(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        dp = self._sv.dp
        # the device is started from init_device
        self.assertEqual(dp.state(), tango.DevState.ON)

        dp.Stop()
        self.assertEqual(dp.state(), tango.DevState.OFF)

        dp.Start()
        self.assertEqual(dp.state(), tango.DevState.ON)


if __name__ == '__main__':
    unittest.main()
