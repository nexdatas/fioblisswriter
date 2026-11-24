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
# \file fiowriterservice_test.py
# unittests for the FIOWriterService API with a mocked blissdata datastore
#
import unittest
import sys
import time

from fioblisswriter.FIOWriterService import FIOWriterService


if sys.version_info > (3,):
    unicode = str
    long = int


# a mocked blissdata scan exposing only the attributes used by the service
class MockScan(object):

    # constructor
    # \param session scan session name
    def __init__(self, session="mock_session"):
        #: (:obj:`str`) scan session
        self.session = session


# a mocked blissdata datastore replacing
# :class:`blissdata.redis_engine.store.DataStore`.
# It never connects to redis so it can be injected in tests.
class MockDataStore(object):

    # constructor
    def __init__(self):
        #: (:obj:`int`) number of get_next_scan calls
        self.get_next_scan_calls = 0
        #: (:obj:`list` <:obj:`str`>) loaded scan keys
        self.loaded_keys = []
        #: (:class:`FIOWriterService`) service to stop after the first scan
        self.service = None
        #: (:class:`MockScan`) scan returned by load_scan
        self.scan = MockScan()

    # mocked get_next_scan
    # \param since last timestamp
    # \param timeout scan timeout
    def get_next_scan(self, since=None, timeout=None):
        self.get_next_scan_calls += 1
        # stop the service loop after the first iteration
        if self.service is not None:
            self.service._FIOWriterService__running = False
        return (123, "scankey")

    # mocked load_scan
    # \param key scan key
    def load_scan(self, key):
        self.loaded_keys.append(key)
        return self.scan


# test fixture
class FIOWriterServiceTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.seed = long(time.time() * 256)  # use fractional seconds

        #: (:obj:`str`) dummy redis url - never used with the mock datastore
        self.__url = "redis://localhost:6380"

    # test starter
    # \brief Common set up
    def setUp(self):
        print("\nsetting up...")
        print("SEED = %s" % self.seed)

    # test closer
    # \brief Common tear down
    def tearDown(self):
        print("tearing down ...")

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

    # helper creating a service with an injected mocked datastore
    # \param session blissdata session name
    # \returns (service, mocked datastore) tuple
    def __service(self, session="__all__"):
        ds = MockDataStore()
        svc = FIOWriterService(
            self.__url, session, 0, datastore=ds)
        ds.service = svc
        return svc, ds

    # constructor test
    # \brief It tests that the mocked datastore is injected
    def test_constructor(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        svc, ds = self.__service()
        # the injected datastore is used instead of a real one
        self.assertTrue(svc._FIOWriterService__datastore is ds)
        # a freshly built service is stopped and error free
        self.assertEqual(svc.get_status(), "FIOBlissWriter is STOPPED")
        self.assertEqual(svc.errors(), [])

    # get_status test
    # \brief It tests the three service status strings
    def test_get_status(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        svc, _ = self.__service()
        self.assertEqual(svc.get_status(), "FIOBlissWriter is STOPPED")

        svc._FIOWriterService__running = True
        self.assertEqual(svc.get_status(), "FIOBlissWriter is RUNNING")

        svc._FIOWriterService__error = True
        self.assertEqual(svc.get_status(), "FIOBlissWriter is FAILED")

    # errors test
    # \brief It tests that errors() returns a copy of the error list
    def test_errors(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        svc, _ = self.__service()
        self.assertEqual(svc.errors(), [])

        svc._FIOWriterService__errors.append("boom")
        errs = svc.errors()
        self.assertEqual(errs, ["boom"])
        # a copy is returned - mutating it does not affect the service
        self.assertTrue(errs is not svc._FIOWriterService__errors)
        errs.append("other")
        self.assertEqual(svc.errors(), ["boom"])

    # join_scans test
    # \brief It tests join_scans with no running scan writers
    def test_join_scans_empty(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        svc, _ = self.__service()
        # with no scan writers join_scans is a no-op and does not raise
        svc.join_scans()
        svc.join_scans(stop=True)
        self.assertEqual(svc.errors(), [])

    # start test
    # \brief It tests one start() iteration with a mocked datastore
    def test_start_skips_other_session(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        # the service session differs from the scan session so no
        # ScanWriter is spawned and start() returns after one iteration
        svc, ds = self.__service(session="my_session")
        ds.scan = MockScan(session="other_session")

        svc.start()

        self.assertEqual(ds.get_next_scan_calls, 1)
        self.assertEqual(ds.loaded_keys, ["scankey"])
        self.assertEqual(svc.get_status(), "FIOBlissWriter is STOPPED")
        self.assertEqual(svc.errors(), [])


if __name__ == '__main__':
    unittest.main()
