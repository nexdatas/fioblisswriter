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

import functools
import time
import pathlib
import os
import fnmatch
import numpy as np

# from blissdata.redis_engine.store import DataStore
# from blissdata.redis_engine.scan import ScanState
from blissdata.redis_engine.exceptions import EndOfStream
# from blissdata.redis_engine.exceptions import NoScanAvailable


ALLOWED_FIO_SURFIXES = {".fio"}


def create_fio_file(
        scan,
        streams,
        skip_final_parameters=False, max_string_parameter_size=300,
        snapshot_blacklist=None):
    """ open fio file

    :param scan: blissdata scan
    :type scan: :obj:`blissdata.redis_engine.scan.Scan`
    :param streams: tango-like steamset class
    :type streams: :class:`StreamSet` or :class:`tango.LatestDeviceImpl`
    :returns: nexus file object
    :rtype: :obj:`FIOFile`
    """
    fpath = pathlib.Path(scan.info["filename"])
    if fpath.suffix not in ALLOWED_FIO_SURFIXES:
        fpath = fpath.with_suffix(".fio")

    fdir = fpath.parent
    if not fdir.is_dir():
        fdir.mkdir(parents=True)

    fiofl = FIOFile(scan, fpath,
                    streams,
                    skip_final_parameters,
                    max_string_parameter_size,
                    snapshot_blacklist)
    # ?? append mode
    if not fpath.exists():
        fiofl.create_file_structure()
    return fiofl


class FIOFile:

    def __init__(
            self, scan, fpath, streams,
            skip_final_parameters=False, max_string_parameter_size=300,
            snapshot_blacklist=None, max_write_interval=1):
        """ constructor

        :param scan: blissdata scan
        :type scan: :obj:`blissdata.redis_engine.scan.Scan`
        :param fpath: nexus file path
        :type fpath: :obj:`pathlib.Path`
        :param streams: tango-like steamset class
        :type streams: :class:`StreamSet` or :class:`tango.LatestDeviceImpl`
        :param max_write_interval: max write interval
        :type max_write_interval: :obj:`int`
        """
        self.__scan = scan
        self.__fpath = fpath
        #: (:class:`StreamSet` or :class:`tango.LatestDeviceImpl`) stream set
        self._streams = streams
        self.__skip_final_parameters = skip_final_parameters
        self.__max_string_parameter_size = max_string_parameter_size
        self.__snapshot_blacklist = snapshot_blacklist or []
        self.__max_write_interval = max_write_interval

        self.__last_write_time = 0

        self.__mfile = None
        self.__cursors = {}

        self.__mca_dir_name = ""
        self.__mca_aliases = []
        self.__mca_names = []
        self.__ct_names = []
        self.__twod_names = []
        self.__scan_name = ""

        self.__tot_point_nb = 0

    @functools.cached_property
    def channels(self):
        return tuple(ch for ch in self.__scan.info["datadesc"].values())

    @functools.cached_property
    def labels(self):
        labels = (ch["label"] for ch in self.channels)
        return tuple(label.replace(" ", "_") for label in labels)

    def snapshot_keys(self):
        si = self.__scan.info
        snapshot = si.get("snapshot", {})
        names = [nm for nm in snapshot.keys()]
        bllist = []
        for flt in self.__snapshot_blacklist:
            bllist.extend(fnmatch.filter(names, flt))
        return tuple(set(names) - set(bllist))

    def create_file_structure(self):
        """ create nexus structure
        """
        si = self.__scan.info
        self.__scan_name = self.__scan.name
        afpath = self.__fpath.absolute()
        filename = str(afpath)
        self.__mca_dir_name = filename[:-len(afpath.suffix)]
        self.__mfile = open(filename, "w")
        try:
            starttime = si['start_time']
        except Exception:
            starttime = time.ctime()
        try:
            title = si['title']
        except Exception:
            title = self.__scan.name
        try:
            user = si['user_name']
        except Exception:
            import getpass
            user = getpass.getuser()

        self.__mfile.write(
            "!\n! Comments\n!\n%%c\n%s\nuser %s Acquisition started at %s\n" %
            (title, user, starttime))

    def write_init_snapshot(self):
        """ write inits data
        """
        self.__mfile.write("!\n! Parameter\n!\n%p\n")
        self.__mfile.flush()

        si = self.__scan.info
        snapshot = si.get("snapshot", {})
        self._write_snapshot(snapshot, [None, "INIT"])

    def _write_snapshot(self, snapshot, strategies):

        for ds in self.snapshot_keys():
            item = snapshot[ds]
            strategy = item.get("strategy", None)
            unit = item.get("unit", None)

            if strategy in strategies:
                try:
                    value = item["value"]
                    try:
                        dtype = item["dtype"]
                    except Exception:
                        dtype = ""
                    if dtype in ["string", "str"] and "\n" in str(value):
                        value = value.replace("\n", "\\n")
                        value = '"%s"' % value
                        if len(value) > self.__max_string_parameter_size:
                            continue
                    self.__mfile.write("%s = %s\n" % (str(ds), str(value)))
                    if unit:
                        self.__mfile.write("%s@unit = %s\n"
                                           % (str(ds), str(unit)))

                except Exception as e:
                    self._streams.error(
                        "FIOFile::_write_snapshot() %s %s %s %s"
                        % (ds, strategy, item, str(e)))
                    break

    def prepareChannels(self):
        """ prepare cursors
        """

        self.__tot_point_nb = 0

        self.__mca_aliases = []
        self.__mca_names = []
        self.__ct_names = []
        self.__twod_names = []

        self.__mfile.write("!\n! Data\n!\n%d\n")
        self.__mfile.flush()
        self.__cursors = {}
        #         for key, stream in self.__scan.streams.items():
        # -           self.__cursors[key] = stream.cursor()
        i = 1
        for ch in self.channels:
            key = ch["label"]
            self._streams.debug(
                "FIOFile::prepareChannels() - "
                "CH %s %s" % (key, list(self.__scan.streams.keys())))
            if key in list(self.__scan.streams.keys()):
                stream = self.__scan.streams[key]
                self.__cursors[key] = stream.cursor()

                shape = list(stream.shape)

                dtype = str(stream.dtype)
                if hasattr(dtype, "__name__"):
                    dtype = str(dtype.__name__)
                if dtype == "string":
                    dtype = "str"
                if dtype == 'float64':
                    dtype = 'DOUBLE'

                if len(shape) == 2:
                    self.__twod_names.append(key)
                elif len(shape) == 1 and shape[0] != 1:
                    self.__mca_names.append(key)
                    self.__mca_aliases.append(key)
                elif len(shape) == 0 or (len(shape) == 1 and shape[0] == 1):
                    self.__ct_names.append(key)

                if key == 'point_nb':
                    continue
                if key == 'timestamp':
                    continue
                if len(shape) != 0 and (len(shape) != 1 or shape[0] != 1):
                    continue
                outLine = " Col %d %s %s\n" % (i, key, dtype)
                self.__mfile.write(outLine)
                i += 1

        outLine = " Col %d %s %s\n" % (i, 'timestamp', 'DOUBLE')
        self.__mfile.write(outLine)

        self.__mfile.flush()
        os.fsync(self.__mfile.fileno())

    def write_scan_points(self):
        """ write step data
        """
        now = time.monotonic()
        if (now - self.__last_write_time) < self.__max_write_interval:
            return
        nan = float('nan')
        ctnames = self.__ct_names
        mcanames = self.__mca_names
        fd = self.__mfile

        ct_values = {}
        mca_values = {}
        ct_lengths = []

        for ch in ctnames:
            try:
                val = self.__cursors[ch].read()
            except EndOfStream:
                self._streams.error(
                    "FIOFile::write_scan_point() - "
                    "End of stream for ct column {}".format(ch))
                return
            val = val.get_data()
            if isinstance(val, np.ndarray):
                ct_values[ch] = val
            else:
                ct_values[ch] = [val]
            ct_lengths.append(len(ct_values[ch]))

        try:
            npoints = ct_lengths[0]
        except Exception:
            npoints = 0

        for ch in mcanames:
            try:
                val = self.__cursors[ch].read()
            except EndOfStream:
                self._streams.error(
                    "FIOFile::write_scan_point() - "
                    "End of stream for ct column {}".format(ch))
                return
            val = val.get_data()
            if isinstance(val, np.ndarray):
                mca_values[ch] = val
            else:
                mca_values[ch] = [val]

        lines = []

        for i in range(npoints):
            try:
                timestamp = ct_values["timestamp"][i]
            except Exception:
                timestamp = nan

            outstr = ''
            for ch in ctnames:
                if ch == "timestamp" or ch == "point_nb":
                    continue

                try:
                    data = ct_values[ch][i]
                except Exception as e:
                    self._streams.error(
                        "FIOFile::write_scan_point() - "
                        "MISSING DATA for %s at %s (%s)" % (ch, i, str(e)))
                    data = nan
                data_len = None
                # We are sure we get the 1d even with different types
                try:
                    data_len = len(data)
                    if data_len > 0:
                        outstr += ' ' + str(data[0])
                    else:
                        outstr += ' ' + str(data)
                except Exception:
                    outstr += ' ' + str(data)
                outstr += ' ' + str(timestamp)

            lines.append(outstr)

        fd.write("\n".join(lines))
        fd.write("\n")
        fd.flush()
        os.fsync(self.__mfile.fileno())

        if len(mcanames) > 0:
            self.write_mca_file(mca_values, ct_values, npoints)

        self.__tot_point_nb += npoints
        self.__last_write_time = now

    def write_mca_file(self, mca_values, ct_values, npoints):
        curr_dir = os.getcwd()
        mcanames = self.__mca_names
        nan = float('nan')
        if not os.path.isdir(self.__mca_dir_name):
            try:
                os.makedirs(self.__mca_dir_name)
            except Exception:
                self.__mca_dir_name = None
                return
        os.chdir(self.__mca_dir_name)

        mca_file_templ = self.__scan_name + "_mca_s%d.fio"

        for i in range(npoints):
            try:
                timestamp = ct_values["timestamp"][i]
            except Exception:
                timestamp = nan
            try:
                point_nb = float(ct_values["point_nb"][i])
            except Exception:
                point_nb = i + self.__tot_point_nb

            for ch in mcanames:

                mca_file_name = mca_file_templ % (point_nb + 1)
                fd = open(mca_file_name, 'w')

                fd.write("!\n! Comments\n!\n%%c\n Index %d \n" % point_nb)
                fd.write("!\n! Parameter \n%%p\n timestamp = %g \n"
                         % timestamp)
                fd.flush()

                col = 1
                fd.write("!\n! Data \n%d \n")
                for mca in self.__mca_aliases:
                    fd.write(" Col %d %s FLOAT \n" % (col, mca))
                    col = col + 1

                data = mca_values[ch][i]

                if data is not None:
                    try:
                        lmax = len(data)
                    except Exception as e:
                        self._streams.error(
                            "FIOFile::write_mca_file() - "
                            "storage.py: %s, wrong data (%s)"
                            % (mcanames[0], repr(e)))
                        fd.close()
                        os.chdir(curr_dir)
                        return

                    for mca in self.__mca_names:
                        try:
                            if len(data) > lmax:
                                lmax = len(data)
                        except Exception as e:
                            self._streams.error(
                                "FIOFile::write_mca_file() - "
                                "storage.py: %s, wrong data (%s)"
                                % (mcanames[0], repr(e)))
                            fd.close()
                            os.chdir(curr_dir)
                            return

                    for i in range(0, lmax):
                        line = ""
                        for mca in self.__mca_names:
                            if i > (len(data) - 1):
                                line = line + " 0"
                            else:
                                line = line + " " + str(data[i])
                        line = line + "\n"
                        fd.write(line)

                    fd.close()
                else:
                    pass

        os.chdir(curr_dir)

    def write_final_snapshot(self):
        """ write final data
        """
        si = self.__scan.info
        snapshot = si.get("snapshot", {})

        if not self.__skip_final_parameters:
            self.__mfile.write("!\n! Parameter\n!\n%p\n")
            self.__mfile.flush()

            self._write_snapshot(snapshot, ["FINAL"])

        try:
            end_time = si["end_time"]
        except Exception:
            end_time = time.ctime()
        self.__mfile.write("! Acquisition ended at %s\n" % end_time)
        self.__mfile.flush()

    def close(self):
        """ close file
        """
        self.__mfile.close()
