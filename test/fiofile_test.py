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
# \file fiofile_test.py
# unittests for the FIOFile API
#
import unittest
import os
import sys
import time
import pathlib
import tempfile

from fioblisswriter.FIOFile import (
    create_fio_file,
    FIOFile,
    ALLOWED_FIO_SURFIXES,
)


if sys.version_info > (3,):
    unicode = str
    long = int


# minimal blissdata-scan replacement exposing only the attributes used here
class FakeScan(object):

    # constructor
    # \param info scan info dictionary
    # \param name scan name
    def __init__(self, info, name="fakescan"):
        #: (:obj:`dict`) scan info
        self.info = info
        #: (:obj:`str`) scan name
        self.name = name
        #: (:obj:`dict`) scan streams
        self.streams = {}


# minimal tango-like stream set swallowing log messages
class FakeStreams(object):

    def error(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass


# test fixture
class FIOFileTest(unittest.TestCase):

    # constructor
    # \param methodName name of the test method
    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)

        self.seed = long(time.time() * 256)  # use fractional seconds

        self.__streams = FakeStreams()

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

    # module constants test
    # \brief It tests the module level suffix table
    def test_constants(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        self.assertTrue(".fio" in ALLOWED_FIO_SURFIXES)

    # channels/labels test
    # \brief It tests the channels and labels cached properties
    def test_channels_labels(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        info = {"datadesc": {
            "c1": {"label": "det one"},
            "c2": {"label": "mot two"},
        }}
        fiofl = FIOFile(FakeScan(info), "dummy.fio", self.__streams)

        self.assertEqual(len(fiofl.channels), 2)
        # spaces in labels are replaced by underscores
        self.assertEqual(fiofl.labels, ("det_one", "mot_two"))
        # the properties are cached - the same object is returned
        self.assertTrue(fiofl.channels is fiofl.channels)
        self.assertTrue(fiofl.labels is fiofl.labels)

    # snapshot_keys test
    # \brief It tests snapshot_keys with a blacklist pattern
    def test_snapshot_keys(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        info = {"snapshot": {"aaa": {}, "bbb": {}, "abc": {}}}
        # no blacklist - all keys are returned
        fiofl = FIOFile(FakeScan(info), "dummy.fio", self.__streams)
        self.assertEqual(set(fiofl.snapshot_keys()), {"aaa", "bbb", "abc"})

        # the "a*" pattern removes the matching keys
        fiofl2 = FIOFile(FakeScan(info), "dummy.fio", self.__streams,
                         snapshot_blacklist=["a*"])
        self.assertEqual(set(fiofl2.snapshot_keys()), {"bbb"})

    # file structure test
    # \brief It tests create_file_structure, snapshots and close
    def test_create_file_structure(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        tdir = tempfile.mkdtemp()
        fpath = pathlib.Path(tdir) / "scan.fio"

        info = {
            "title": "MyTitle",
            "user_name": "tester",
            "start_time": "STARTNOW",
            "end_time": "ENDNOW",
            "snapshot": {
                "p1": {"value": 5, "strategy": "INIT", "unit": "mm"},
                "p2": {"value": 9, "strategy": "FINAL"},
            },
        }
        fiofl = FIOFile(FakeScan(info, name="scan"), fpath, self.__streams)
        try:
            fiofl.create_file_structure()
            fiofl.write_init_snapshot()
            fiofl.write_final_snapshot()
            fiofl.close()

            self.assertTrue(fpath.exists())
            content = open(str(fpath)).read()
            # the comment header keeps the title and the user line
            self.assertTrue("MyTitle" in content)
            self.assertTrue("user tester" in content)
            self.assertTrue("STARTNOW" in content)
            # the INIT parameter and its unit are written
            self.assertTrue("p1 = 5" in content)
            self.assertTrue("p1@unit = mm" in content)
            # the FINAL parameter and the end time are written
            self.assertTrue("p2 = 9" in content)
            self.assertTrue("ENDNOW" in content)
        finally:
            if fpath.exists():
                os.remove(fpath)
            os.rmdir(tdir)

    # create_fio_file test
    # \brief It tests the create_fio_file factory function
    def test_create_fio_file(self):
        fun = sys._getframe().f_code.co_name
        print("Run: %s.%s() " % (self.__class__.__name__, fun))

        tdir = tempfile.mkdtemp()
        # a non .fio filename is coerced to a .fio suffix
        target = os.path.join(tdir, "myscan.h5")
        info = {"filename": target, "title": "T",
                "user_name": "u", "start_time": "t"}
        fiofl = create_fio_file(FakeScan(info, name="myscan"), self.__streams)
        try:
            self.assertTrue(isinstance(fiofl, FIOFile))
            expected = os.path.join(tdir, "myscan.fio")
            self.assertTrue(os.path.exists(expected))
        finally:
            fiofl.close()
            for nm in os.listdir(tdir):
                os.remove(os.path.join(tdir, nm))
            os.rmdir(tdir)


if __name__ == '__main__':
    unittest.main()
