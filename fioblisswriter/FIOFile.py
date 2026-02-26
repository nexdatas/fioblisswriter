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
import xml.etree.ElementTree as et
from lxml.etree import XMLParser
from lxml import etree
import numpy as np
import json

# from blissdata.redis_engine.store import DataStore
# from blissdata.redis_engine.scan import ScanState
from blissdata.redis_engine.exceptions import EndOfStream
# from blissdata.redis_engine.exceptions import NoScanAvailable


ALLOWED_FIO_SURFIXES = {".fio"}

try:
    _npver = np.version.version.split(".")
    NPMAJOR = int(_npver[0])
except Exception:
    NPMAJOR = 1


PTH = {
    "long": h5cpp.datatype.Integer,
    "str": h5cpp.datatype.kVariableString,
    "unicode": h5cpp.datatype.kVariableString,
    "bool": h5cpp.datatype.kEBool,
    "int": h5cpp.datatype.kInt64,
    "int64": h5cpp.datatype.kInt64,
    "int32": h5cpp.datatype.kInt32,
    "int16": h5cpp.datatype.kInt16,
    "int8": h5cpp.datatype.kInt8,
    "uint": h5cpp.datatype.kInt64,
    "uint64": h5cpp.datatype.kUInt64,
    "uint32": h5cpp.datatype.kUInt32,
    "uint16": h5cpp.datatype.kUInt16,
    "uint8": h5cpp.datatype.kUInt8,
    "float": h5cpp.datatype.kFloat32,
    "float64": h5cpp.datatype.kFloat64,
    "float32": h5cpp.datatype.kFloat32,
    "string": h5cpp.datatype.kVariableString,
}

ATTRDESC = {
    "nexus_type": "type",
    "unit": "units",
    "depends_on": "depends_on",
    "trans_type": "transformation_type",
    "trans_vector": "vector",
    "trans_offset": "offset",
    "source": "nexdatas_source",
    "strategy": "nexdatas_strategy",
}


NOATTRS = {"name", "label", "dtype", "value", "nexus_path", "shape", "stream"}


def create_field(grp, name, dtype, value=None, shape=None, chunk=None):
    """ create field

    :param n: group name
    :type n: :obj:`str`
    :param type_code: nexus field type
    :type type_code: :obj:`str`
    :param shape: shape
    :type shape: :obj:`list` < :obj:`int` >
    :param chunk: chunk
    :type chunk: :obj:`list` < :obj:`int` >
    :param dfilter: filter deflater
    :type dfilter: :class:`H5CppDataFilter`
    :returns: file tree field
    :rtype: :class:`H5CppField`
    """
    # print("CREATE", name, dtype, value, shape, chunk)
    dcpl = h5cpp.property.DatasetCreationList()
    if shape is None and hasattr(value, "shape"):
        shape = value.shape
    elif dtype in ["str", "unicode", "string"]:
        dataspace = h5cpp.dataspace.Scalar()
        field = h5cpp.node.Dataset(
            grp, h5cpp.Path(name), PTH[dtype], dataspace,
            dcpl=dcpl)
        if value is not None:
            field.write(value)
        return field
    shape = shape or [1]
    dataspace = h5cpp.dataspace.Simple(
        tuple(shape), tuple([h5cpp.dataspace.UNLIMITED] * len(shape)))
    if chunk is None:
        chunk = [(dm if dm != 0 else 1) for dm in shape]
    dcpl.layout = h5cpp.property.DatasetLayout.CHUNKED
    dcpl.chunk = tuple(chunk)
    field = h5cpp.node.Dataset(
        grp, h5cpp.Path(name), PTH[dtype], dataspace, dcpl=dcpl)
    if value is not None:
        field.write(value)
    return field


def create_groupfield(root, lnxpath, dtype,
                      value=None, shape=None, chunk=None):
    """ create field

    :param root: root object
    :type root: :class:`nxgroup`
    :param lnxpath: nexus path list
    :type lnxpath: :obj:`list` <:obj:`str`>
    :param dtype: nexus field type
    :type dtype: :obj:`str`
    :param value: field value
    :type value: :obj:`any`
    :param shape: shape
    :type shape: :obj:`list` < :obj:`int` >
    :param chunk: chunk
    :type chunk: :obj:`list` < :obj:`int` >
    :returns: nexus field
    :rtype: :class:`nxfield`
    """
    grp = root
    for gr in lnxpath[:-1]:
        gn = gr
        gt = None
        if ":" in gr:
            gn, gt = gr.split(":")

        if grp.has_group(gn):
            grp = grp.get_group(gn)
        else:
            grp = h5cpp.node.Group(grp, gn)
            if gt is not None:
                grp.attributes.create(
                    "NX_class",
                    h5cpp.datatype.kVariableString).write(gt)
        # print(gn)
    name = lnxpath[-1]
    if isinstance(value, list):
        value = np.array(value, dtype=dtype)
    # print("CREATE %s (%s)" % (nxpath, dtype))
    dataset = create_field(grp, name, dtype, value, shape, chunk)
    return dataset


def first(array):
    """  get first element if the only

    :param array: numpy array
    :type array: :class:`numpy.ndarray`
    :returns: first element of the array
    :type array: :obj:`any`
    """
    try:
        if isinstance(array, np.ndarray) and len(array) == 1:
            return array[0]
    except Exception:
        try:
            if hasattr(array, "all"):
                if NPMAJOR < 2:
                    array = array.all()
                if hasattr(array, "decode"):
                    return array.decode()
        except Exception:
            pass
    return array


def write_attr(am, name, dtype, value, item=None):
    """ write attribute
    """

    try:
        at = am[name]
        # print("TYPE",name, value, at.dataspace.type)
    except Exception:
        at = None
    if at is None:
        try:
            vshape = None
            if isinstance(value, list):
                vshape = np.array(value).shape
            elif hasattr(value, "shape"):
                vshape = value.shape
            if not vshape:
                at = am.create(name, PTH[str(dtype)])
            else:
                at = am.create(name, PTH[str(dtype)], vshape)
        except Exception as e:
            print("CREATE ATT", name, dtype, PTH[dtype])
            print("WWAA", name, name, dtype, value, type(value), item)
            print(str(e))
    try:
        if at is not None:
            try:
                try:
                    rvalue = at.read()
                except Exception:
                    rvalue = None
                ashape = None
                vshape = None
                if isinstance(value, list):
                    vshape = np.array(value).shape
                elif hasattr(value, "shape"):
                    vshape = value.shape
                if at.dataspace.type == h5cpp.dataspace.Type.SCALAR:
                    rvalue = first(rvalue)
                else:
                    ashape = at.dataspace.current_dimensions
                if str(rvalue) != str(value):
                    if ashape != vshape:
                        am.remove(name)
                        at = am.create(name, PTH[str(dtype)], vshape)
                    at.write(value)
                    # print("WRITE", am, name, dtype, value,
                    #       type(value), rvalue)
                    # print("DIFF", name, str(value), str(rvalue))
                else:
                    pass
                    # print("THE SAME", name, value)
            except Exception as e:
                print("ERROR", str(e), am, name, dtype, value, item)
                # print("at", at.read(), dir(at))
                shape = None
                if hasattr(at.dataspace, "current_dimensions"):
                    shape = at.dataspace.current_dimensions
                if shape:
                    if not isinstance(value, list) and \
                            not hasattr(value, "shape"):
                        value = json.loads(value)
                at.write(value)
    except Exception as e:
        # print("READ", at.read())
        print("ERROR2", str(e), am, name, dtype, value, item)
        # print("WW", am, name, dtype, value, item)
        # print(at, dir(at))


def write_snapshot_item(root, item):
    """ write snapshot item
    """
    nxpath = item.get('nexus_path', None)
    value = item.get('value', None)
    dtype = item.get('dtype', None)
    if dtype == "string":
        dtype = "str"
    dataset = None
    if nxpath and value is not None:
        attr = None
        if "@" in nxpath:
            nxpath, attr = nxpath.split("@", 1)
        lnxpath = nxpath.split("/")
        h5path = "/".join([nd.split(":")[0] for nd in lnxpath])
        try:
            if not attr:
                dataset = root.get_dataset(h5path)
                if dataset.dataspace.type != \
                        h5cpp.dataspace.Type.SCALAR:
                    if not dataset.dataspace.size:
                        try:
                            dataset.extent(0, 1)
                        except Exception:
                            pass
                dataset.write(value)
            else:
                if ":" not in lnxpath:
                    try:
                        adataset = root.get_dataset(h5path)
                        am = adataset.attributes
                    except Exception:
                        group = root.get_group(h5path)
                        am = group.attributes
                else:
                    group = root.get_group(h5path)
                    am = group.attributes
                write_attr(am, attr, dtype, value)
        except Exception as e:
            # print(nxpath, str(e))
            if str(e).startswith("No node ["):
                dataset = create_groupfield(
                    root, lnxpath, dtype, value)
            else:
                print("ERROR", str(e), type(e))
                raise
    if dataset is not None:
        attrs = set(item.keys()) - NOATTRS
        am = dataset.attributes
        for anm in attrs:
            avl = item[anm]
            if isinstance(avl, list):
                av = avl[0]
                while isinstance(av, list) and len(av):
                    av = av[0]
                dtp = str(type(av).__name__)
            elif hasattr(avl, "dtype"):
                dtp = str(dtype.__name__)
            else:
                dtp = str(type(avl).__name__)
            nanm = ATTRDESC.get(anm, anm)
            try:
                write_attr(am, nanm, dtp, avl, item)
            except Exception as e:
                print("WRII", am, nanm, dtp, avl, item, str(e))


def create_nexus_file(scan):
    """ open nexus file

    :param scan: blissdata scan
    :type scan: :obj:`blissdata.redis_engine.scan.Scan`
    :returns: nexus file object
    :rtype: :obj:`FIOFile`
    """
    fpath = pathlib.Path(scan.info["filename"])
    if fpath.suffix not in ALLOWED_FIO_SURFIXES:
        fpath = fpath.with_suffix(".fio")

    fdir = fpath.parent
    if not fdir.is_dir():
        fdir.mkdir(parents=True)

    number = scan.number
    serialno = ""
    entryname = "entry"
    snapshot = {}
    si = scan.info
    if "snapshot" in si:
        snapshot = si["snapshot"]
        if serialno in snapshot.keys() and "value" in snapshot["serialno"]:
            serialno = snapshot["serialno"]["value"]
        if entryname in snapshot.keys() and "value" in snapshot["entryname"]:
            entryname = snapshot["entryname"]["value"]

    fiofl = FIOFile(scan, fpath)
    # ?? append mode
    if not fpath.exists():
        fiofl.create_file_structure()
    return fiofl


class FIOFile:

    def __init__(self, scan, fpath, max_write_interval=1):
        """ constructor

        :param scan: blissdata scan
        :type scan: :obj:`blissdata.redis_engine.scan.Scan`
        :param fpath: nexus file path
        :type fpath: :obj:`pathlib.Path`
        :param max_write_interval: max write interval
        :type max_write_interval: :obj:`int`
        """
        self.__scan = scan
        self.__fpath = fpath
        self.__mfile = None
        self.__cursors = {}
        self.__nxfields = {}
        self.__last_write_time = 0
        self.__max_write_interval = max_write_interval

    @functools.cached_property
    def channels(self):
        return tuple(ch for ch in self.__scan.info["datadesc"].values())

    @functools.cached_property
    def labels(self):
        labels = (ch["label"] for ch in self.channels)
        return tuple(label.replace(" ", "_") for label in labels)

    def create_file_structure(self):
        """ create nexus structure
        """
        si = self.__scan.info
        filename = str(self.__fpath.absolute())
        snapshot = {}
        if "snapshot" in si:
            snapshot = si["snapshot"]
        xmls = None
        self.__mfile = nexus.create_file(filename,
                                         h5cpp.file.AccessFlags.TRUNCATE)
        root = self.__mfile.root()
        if xmls:
            nexus.create_from_string(root, xmls)

    def write_init_snapshot(self):
        """ write init data
        """
        si = self.__scan.info
        root = self.__mfile.root()
        snapshot = {}
        if "snapshot" in si:
            snapshot = si["snapshot"]
        for ds, items in snapshot.items():
            if not isinstance(items, list):
                items = [items]
            for item in items:
                strategy = None
                if "strategy" in item:
                    strategy = item["strategy"]
                if not strategy or strategy in ["INIT"]:
                    try:
                        # print("WRITE", ds, strategy)
                        write_snapshot_item(
                            root, item,
                            "%s/%s" % (self.__default_nexus_path, ds))
                    except Exception as e:
                        print("Error: ", ds, strategy, item, str(e))
                        break
            else:
                continue
            break

    def prepareChannels(self):
        """ prepare cursors
        """
        self.__cursors = {}
        self.__nxfields = {}
        #         for key, stream in self.__scan.streams.items():
        # -           self.__cursors[key] = stream.cursor()
        for ch in self.channels:
            key = ch["label"]
            # print("CH", key, list(self.__scan.streams.keys()))
            if key in list(self.__scan.streams.keys()):
                stream = self.__scan.streams[key]
                self.__cursors[key] = stream.cursor()
                shape = [0] + list(stream.shape)
                chunk = [1] + list(stream.shape)
                nxpath = ch.get(
                    'nexus_path',
                    "%s/%s" % (self.__default_nexus_path, key))
                dtype = str(stream.dtype)
                if hasattr(dtype, "__name__"):
                    dtype = str(dtype.__name__)
                if dtype == "string":
                    dtype = "str"
                lnxpath = nxpath.split("/")
                h5path = "/".join([nd.split(":")[0] for nd in lnxpath])
                root = self.__mfile.root()
                try:
                    self.__nxfields[key] = root.get_dataset(h5path)
                except Exception as e:
                    if str(e).startswith("No node ["):
                        # print("S", key, shape, chunk, stream.dtype, ch)
                        self.__nxfields[key] = create_groupfield(
                            root, lnxpath, dtype, value=None,
                            shape=shape, chunk=chunk)
                    else:
                        print("ERROR", str(e), type(e))
                        raise

    def write_scan_points(self):
        """ write step data
        """
        now = time.monotonic()
        if (now - self.__last_write_time) < self.__max_write_interval:
            return

        for ch in self.channels:
            if "stream" in ch and ch["stream"] not in ["stream"]:
                print("SKIP", ch["label"])
                continue
            try:
                val = self.__cursors[ch["label"]].read()
            except EndOfStream:
                print("END of STREAM")
                return
            try:
                key = ch["label"]
                values = val.get_data()
                # print("CHANNEL", ch["label"], ch["shape"], values)
                npoints = len(values)
                if npoints:
                    oldshape = \
                        self.__nxfields[key].dataspace.current_dimensions
                    rank = len(oldshape)
                    if rank:
                        offset = [0] * rank
                        block = list(oldshape)
                        offset[0] = oldshape[0]
                        block[0] = npoints
                        selection = h5cpp.dataspace.Hyperslab(
                            offset=offset, block=block)
                        self.__nxfields[key].extent(0, npoints)
                        # print(self.__nxfields[key].dataspace.current_dimensions)
                        # print(offset, block)
                        self.__nxfields[key].write(values, selection)
            except Exception as e:
                print(ch)
                print(key, values)
                print(str(e))
        # npoints = len(values[0])

        # for i in range(npoints):
        self.__last_write_time = now

    def write_final_snapshot(self):
        """ write final data
        """
        root = self.__mfile.root()
        si = self.__scan.info
        snapshot = {}
        if "snapshot" in si:
            snapshot = si["snapshot"]
        for ds, items in snapshot.items():
            if not isinstance(items, list):
                items = [items]
            for item in items:
                strategy = None
                if "strategy" in item:
                    strategy = item["strategy"]
                if strategy in ["FINAL"]:
                    try:
                        # print("WRITE", ds, strategy)
                        write_snapshot_item(
                            root, item,
                            "%s/%s" % (self.__default_nexus_path, ds))
                    except Exception as e:
                        print("Error: ", ds, strategy, item, str(e))
                        break
            else:
                continue
            break

    def close(self):
        """ close file
        """
        root = self.__mfile.root()
        root.close()
        self.__mfile.close()
