# -*- coding: utf-8 -*-
#
# This file is part of the FIOBlissWriter project
#
#
#
# Distributed under the terms of the GPL license.
# See LICENSE.txt for more info.

"""
FIO Bliss Writer

FIO Bliss Writer stores (meta)data from blissdata provided by FIODataWriter
"""

import threading

from tango import DebugIt, DevState
from tango.server import Device
# from tango.server import DevStatus
from tango.server import command
from tango.server import device_property

from .FIOWriterService import FIOWriterService as NWS

__all__ = ["FIOBlissWriter"]


class FIOBlissWriter(Device):
    """
    FIO Bliss Writer stores (meta)data from blissdata provided
    by FIODataWriter

    **Properties:**

    - Device Property
        RedisUrl
            - Dlissdata redis url
            - Type:'str'
        Session
            - session to be recorder
            - Type:'str'
        NextScanTimeout
            - timeout for next scan writing
            - Type:'int'
        SkipFinalParameters
            - do not store parameters with the FINAL  strategy
            - Type:'bool'
        MaxStringParameterSize
            - maximal  value size of string parameters
            - Type:'int'
        SnapshotBlacklist
            - a list of snapshot keys for skip
            - Type:'DevVarStringArray'
    """

    # -----------------
    # Device Properties
    # -----------------

    RedisUrl = device_property(
        dtype='str',
        default_value="redis://localhost:6380",
        doc="Blissdata redis url"
    )

    Session = device_property(
        dtype='str',
        doc="session to be recorder"
    )

    NextScanTimeout = device_property(
        dtype='int',
        default_value=2,
        doc="timeout for next scan writing"
    )
    SkipFinalParameters = device_property(
        dtype='bool',
        default_value=False,
        doc="do not store parameters with the FINAL strategy"
    )

    MaxStringParameterSize = device_property(
        dtype='int',
        default_value=300,
        doc="maximal value size of string parameters"
    )

    SnapshotBlacklist = device_property(
        dtype='DevVarStringArray',
        doc="a list of snapshot keys for skip"
    )

    # ---------------
    # General methods
    # ---------------

    def init_device(self):
        """Initializes the attributes and properties of the FIOBlissWriter."""
        Device.init_device(self)
        self.info_stream("Initializing device...")
        self.fio_writer_service = NWS(
            self.RedisUrl, self.Session, self.NextScanTimeout,
            self.SkipFinalParameters, self.MaxStringParameterSize,
            self.SnapshotBlacklist,
            self
        )
        self.Start()

    def dev_status(self):
        return self.fio_writer_service.get_status()

    # --------
    # Commands
    # --------

    @command(
    )
    @DebugIt()
    def Start(self):
        self.thread = threading.Thread(target=self.fio_writer_service.start)
        self.thread.start()
        self.set_state(DevState.ON)

    @command(
    )
    @DebugIt()
    def Stop(self):
        self.fio_writer_service.stop()
        self.thread.join()
        self.set_state(DevState.OFF)

    def delete_device(self):
        """Destructs the attributes and properties of the FIOBlissWriter."""
        self.Stop()
