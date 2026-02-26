#!/usr/bin/env python
#   This file is part of nexdatas - Tango Server for NeXus data writer
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
#

""" setup.py for FIO configuration server """

import os
import sys
from setuptools import setup
from distutils.core import Command

try:
    from sphinx.setup_command import BuildDoc
except Exception:
    BuildDoc = None


#: package name
FIO = "fioblisswriter"
#: fio imported package
IFIO = __import__(FIO)

# __requires__ = 'nextdata ==%s' % IFIO.__version__
needs_pytest = set(['test']).intersection(sys.argv)
pytest_runner = ['pytest-runner'] if needs_pytest else []


def read(fname):
    """ read the file

    :param fname: readme file name
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


class TestCommand(Command):

    """ test command class
    """

    #: user options
    user_options = []

    #: initializes options
    def initialize_options(self):
        pass

    #: finalizes options
    def finalize_options(self):
        pass

    #: runs command
    def run(self):
        import sys
        import subprocess
        errno = subprocess.call([sys.executable, 'test/runtest.py'])
        raise SystemExit(errno)


release = IFIO.__version__
version = ".".join(release.split(".")[:2])
name = "FIOBlissWriter"


#: required files
REQUIRED = [
    'numpy (>=1.5.0)',
    'tango (>=7.2.2)',
]

#: required python packages
install_requires = [
    'lxml',
]

#: metadata for distutils
SETUPDATA = dict(
    name="fioblisswriter",
    version=IFIO.__version__,
    author="Jan Kotanski",
    author_email="jankotan@gmail.com",
    description=("FIO BlissData Writer"),
    license="GNU GENERAL PUBLIC LICENSE v3",
    keywords="FIO blissdata writer Tango server",
    url="http://github.com/jkotan/nexdatas/fioblisswriter",
    packages=[FIO],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Physics',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ],
    install_requires=install_requires,
    scripts=['FIOBlissWriter'],
    cmdclass={
        # 'test': TestCommand,
        'build_sphinx': BuildDoc
    },
    zip_safe=False,
    setup_requires=pytest_runner,
    tests_require=['pytest'],
    command_options={
        'build_sphinx': {
            'project': ('setup.py', name),
            'version': ('setup.py', version),
            'release': ('setup.py', release)}},
    long_description=read('README.rst')
    # long_description_content_type='text/x-rst'
)


def main():
    """ the main function """
    setup(**SETUPDATA)


if __name__ == '__main__':
    main()
