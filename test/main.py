#!/usr/bin/env python
#   This file is part of nexdatas - Tango Server for NeXus data writer
#
#    Copyright (C) 2012-2017 DESY, Jan Kotanski <jkotan@mail.desy.de>
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
# \file runtest.py
# the unittest runner
#

import sys
import unittest

import fioblisswriter_test

try:
    try:
        __import__("tango")
    except Exception:
        __import__("PyTango")

    # if module tango avalable
    PYTANGO_AVAILABLE = True
except ImportError as e:
    PYTANGO_AVAILABLE = False
    print("tango is not available: %s" % e)


try:
    __import__("pninexus.h5cpp")
    # if module pni avalable
    H5CPP_AVAILABLE = True
except Exception as e:
    H5CPP_AVAILABLE = False
    print("h5cpp is not available: %s" % e)


# if not PNI_AVAILABLE and not H5PY_AVAILABLE and not H5CPP_AVAILABLE:
#     raise Exception("Please install h5py, h5cpp or pni")

# if PNI_AVAILABLE:
# if H5PY_AVAILABLE:
# if PNI_AVAILABLE and H5PY_AVAILABLE:


# list of available databases


# main function
def main():

    # test server
    # ts = None

    # test suit
    suite = unittest.TestSuite()

    suite.addTests(
          unittest.defaultTestLoader.loadTestsFromModule(
              fioblisswriter_test)
    )

    # test runner
    runner = unittest.TextTestRunner()
    # test result
    result = runner.run(suite).wasSuccessful()
    sys.exit(not result)

    #   if ts:
    #       ts.tearDown()


if __name__ == "__main__":
    main()
