"""
pansat.download
===============

The :py:mod:`pansat.download` module contains all code responsible for
downloading data. It is itself structured into three parts with
different responsibilities:

   1. The :py:mod:`pansat.download.providers` module, which contains all data providers.
      Data provider class provide interfaces for different source from which data
      can be downloaded.
   2. The :py:mod:`pansat.download.accounts` module, which manages the login information
      for different data providers.
   3. The :py:mod:`pansat.download.commandline` module, which provides a command line
      interface for the download functionality.
"""
