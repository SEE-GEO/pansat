==========
User Guide
==========

Introduction
============

Pansat is a python package that provides an easy interface to download data relevant to atmospheric research.
So far, several satellite and reanalysis products are implemented as well as radiosonde measurements. See 
:ref:`products` for all available products. The aim is to add more functionality to the package in the future,
for example collocation between different products.

The package requires at least python version 3.8 and is being tested on Unix-based operating systems. For Windows,
the test-coverage is not equally large, so pansat may only work in parts.

Installation
============

PyPI
----

Install the latest release from PyPI by

.. code-block::

   pip install pansat

From source
-----------

Pansat is available on `GitHub <https://github.com/SEE-MOF/pansat>`_ where you can clone or download the latest
version of the repository. Once you have done that, pansat can be installed by

.. code-block::

   pip install .

or also

.. code-block::

   pip install -e .

if you want to modify anything within the package.

Getting started
===============

Once pansat is installed, you can use it by importing it into a python executable:

.. code-block::

   import pansat

For inspiration on how to use pansat, see the :ref:`example <examples>`  jupyter notebooks.

Accounts
--------

All of the data providers require a user account. Pansat manages these accounts, see
:ref:`accounts`. The user needs to create the respective account on the providers website
before downloading data for the first time. See the specific :ref:`providers` classes for
links to the websites.

Command line interface
----------------------

There is also a command line interface included in the package, see :ref:`command-line-tool`.

Catalogue
---------

It is possible to list and search through the files downloaded with Pansat, see :ref:`catalogue`.