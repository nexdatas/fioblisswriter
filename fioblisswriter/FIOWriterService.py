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
#

""" Provides the access to a database with NDTS configuration files """

# import time

from blissdata.redis_engine.store import DataStore
from blissdata.redis_engine.scan import ScanState
# from blissdata.redis_engine.exceptions import EndOfStream
from blissdata.redis_engine.exceptions import NoScanAvailable

from .FIOFile import create_fio_file


class FIOWriterService:

    def __init__(self, redis_url, session, next_scan_timeout):
        """ constructor

        :param redis_url: blissdata redis url
        :type redis_url: :obj:`str`
        :param session: blissdata session name
        :type session: :obj:`str`
        :param next_scan_timeout: timeout  between the scans in seconds
        :type next_scan_timeout: :obj:`int`
        """
        #: (:obj:`bool`) service running flag
        self.__running = False
        #: (:obj:`int`) scan timeout in seconds
        self.__next_scan_timeout = next_scan_timeout
        #: (:obj:`str`) session name
        self.__session = session
        #: (:class:`blissdata.redis_engine.store.DataStore`) datastore
        self.__datastore = DataStore(redis_url)

    def start(self):
        """ start writer service
        """
        self.__running = True
        timestamp = None

        while self.__running:
            try:
                timestamp, key = self.__datastore.get_next_scan(
                    since=timestamp, timeout=self.__next_scan_timeout
                )
            except NoScanAvailable:
                continue
            scan = self.__datastore.load_scan(key)
            if not self.__session or scan.session == self.__session:
                self.write_scan(scan)

    def get_status(self):
        """ get writer service status
        """
        status = "is RUNNING" if self.__running else "is STOPPED"
        return "FIOWriter %s" % status

    def stop(self):
        """ stop writer service
        """
        self.__running = False

    def write_scan(self, scan):
        """ write scan data

        :param scan: blissdata scan
        :type scan:
        """
        while scan.state < ScanState.PREPARED:
            scan.update()
        print("SCAN", scan.number)

        fiofl = create_fio_file(scan)
        if fiofl is None:
            return

        print("SCAN INIT", scan.number)
        fiofl.write_init_snapshot()

        fiofl.prepareChannels()

        while scan.state < ScanState.STOPPED:
            scan.update(block=False)
            print("SCAN POINT", scan.number)
            fiofl.write_scan_points()

        while scan.state < ScanState.CLOSED:
            scan.update()

        print("SCAN FINAL", scan.number)
        fiofl.write_final_snapshot()
        fiofl.close()


def main():
    """ main function
    """

    import argparse

    parser = argparse.ArgumentParser(
        description="Start a Tango server for saving data from blisdata"
        "in FIO file"
    )
    parser.add_argument(
        "--redis-url", "-u",
        type=str,
        dest="redis_url",
        default="redis://localhost:6380",
        help="Blissdata redis url ('redis://localhost:6380' by default)",
    )
    parser.add_argument(
        "--session", "-s",
        type=str,
        dest="session",
        default="",
        help="Blissdata session name ('' by default)",
    )
    parser.add_argument(
        "--scan-timeout", "-t",
        type=int,
        dest="scan_timeout",
        default=0,
        help="Scan timeout (0 by default)",
    )

    args = parser.parse_args()
    FIOWriterService(args.redis_url, args.session, args.scan_timeout).start()


if __name__ == "__main__":
    main()
