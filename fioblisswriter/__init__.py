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

""" Implementation of FIO Blissdata writer Server """

# package version
from tango.server import run

from .Release import __version__

__all__ = ["__version__", "run"]


def main(args=None, **kwargs):
    """ runs the Configuration TANGO server

    :param args: command-line arguments
    :type args: :obj:`list` <:obj:`str`>
    """
    from .FIOBlissWriter import FIOBlissWriter as FIOBlWriter
    # from .FIOBlissWriter import FIOBlissWriterClass as FIOBLSrvClass

    return run((FIOBlWriter,), args=args, **kwargs)


if __name__ == '__main__':
    main()
