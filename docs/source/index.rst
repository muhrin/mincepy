.. mincepy documentation master file, created by
   sphinx-quickstart on Fri Mar 31 17:03:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _mincePy: https://github.com/muhrin/mincepy


Welcome to mincePy's documentation!
===================================

.. image:: https://codecov.io/gh/muhrin/mincepy/branch/develop/graph/badge.svg
    :target: https://codecov.io/gh/muhrin/mincepy
    :alt: Coveralls

.. image:: https://travis-ci.org/muhrin/mincepy.svg
    :target: https://travis-ci.org/muhrin/mincepy
    :alt: Travis CI

.. image:: https://img.shields.io/pypi/v/mincepy.svg
    :target: https://pypi.python.org/pypi/mincepy/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/mincepy.svg
    :target: https://pypi.python.org/pypi/mincepy/

.. image:: https://img.shields.io/pypi/pyversions/mincepy.svg
    :target: https://pypi.python.org/pypi/mincepy/

.. image:: https://img.shields.io/pypi/l/mincepy.svg
    :target: https://pypi.python.org/pypi/mincepy/


`mincePy`_: move the database to one side and let your objects take centre stage.


Features
++++++++

* Automatic tracking of in-memory objects.
* Easy storage of python objects.
* Version control.
* Live references between objects.
* Optimistic locking.
* Python 3.5+ compatible.


Installation
++++++++++++

Installation with pip:

.. code-block:: shell

    pip install mincepy


Installation from git:

.. code-block:: shell

    # via pip
    pip install https://github.com/muhrin/mincepy/archive/master.zip

    # manually
    git clone https://github.com/muhrin/mincepy.git
    cd mincepy
    python setup.py install


Development
+++++++++++

Clone the project:

.. code-block:: shell

    git clone https://github.com/muhrin/mincepy.git
    cd mincepy


Create a new virtualenv for `mincePy`_:

.. code-block:: shell

    virtualenv -p python3 mincepy

Install all requirements for `mincePy`_:

.. code-block:: shell

    env/bin/pip install -e '.[dev]'

Table Of Contents
+++++++++++++++++

.. toctree::
   :glob:
   :maxdepth: 3

   apidoc


Versioning
++++++++++

This software follows `Semantic Versioning`_


.. _Semantic Versioning: http://semver.org/
