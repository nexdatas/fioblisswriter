================================================
Welcome to FIO BlissData Writer's documentation!
================================================

|github workflow|
|docs|
|Pypi Version|
|Python Versions|

.. |github workflow| image:: https://github.com/nexdatas/fioblisswriter/actions/workflows/tests.yml/badge.svg
   :target: https://github.com/nexdatas/fioblisswriter/actions
   :alt:

.. |docs| image:: https://img.shields.io/badge/Documentation-webpages-ADD8E6.svg
   :target: https://nexdatas.github.io/fioblisswriter/index.html
   :alt:

.. |Pypi Version| image:: https://img.shields.io/pypi/v/fioblisswriter.svg
                  :target: https://pypi.python.org/pypi/fioblisswriter
                  :alt:

.. |Python Versions| image:: https://img.shields.io/pypi/pyversions/fioblisswriter.svg
                     :target: https://pypi.python.org/pypi/fioblisswriter/
                     :alt:



Authors: Jan Kotanski

FIO BlissData Writer Server is a Tango Server  which
allows to write FIO file from (meta)data stored in BlissData
by FIODataWriter tango server

Tango Server API: https://nexdatas.github.io/fioblisswriter/doc_html

| Source code: https://github.com/nexdatas/fioblisswriter/
| Web page: https://nexdatas.github.io/fioblisswriter/
| NexDaTaS Web page: https://nexdatas.github.io

------------
Installation
------------

Install the dependencies:

|    tango, sphinx

From sources
^^^^^^^^^^^^

Download the latest version of FIO Bliss Writer Server from

|    https://github.com/nexdatas/fioblisswriter/

Extract the sources and run

.. code-block:: console

	  $ python setup.py install

Debian packages
^^^^^^^^^^^^^^^

Debian Trixie, Bookworm, Bullseye and as well as Ubuntu Questing, Noble, Jammy  packages can be found in the HDRI repository.

To install the debian packages, add the PGP repository key

.. code-block:: console

	  $ sudo su
	  $ curl -s http://repos.pni-hdri.de/debian_repo.pub.gpg | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/debian-hdri-repo.gpg --import
	  $ chmod 644 /etc/apt/trusted.gpg.d/debian-hdri-repo.gpg

and then download the corresponding source list, e.g. for trixie

.. code-block:: console

	  $ cd /etc/apt/sources.list.d
	  $ wget http://repos.pni-hdri.de/trixie-pni-hdri.sources

Finally, 

.. code-block:: console

	  $ apt-get update
	  $ apt-get install python3-fioblisswriter

and the FIOBlissWriter tango server (from 2.10.0)

	  $ apt-get install fioblisswriter


From pip
""""""""

To install it from pip you need pymysqldb e.g.

.. code-block:: console

   $ python3 -m venv myvenv
   $ . myvenv/bin/activate

   $ pip install fioblisswriter

Moreover it is also good to install

.. code-block:: console

   $ pip install pytango
   $ pip install nxstools

Setting FIO BlissData Writer Server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To set up FIO Blissdata WriterServer with the default configuration run

.. code-block:: console

          $ nxsetup -x FIOBlissWriter

The *nxsetup* command comes from the **python-nxstools** package.

