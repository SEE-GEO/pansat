===============
Developer guide
===============

This guide provides an overview of the ``pansat`` code and the ideas
behind its design.

Principal components
====================


Products and data providers
---------------------------

Two principal concepts in ``pansat`` are data providers and data products. A
data product is any dataset that contains geospatial variables. A data provider
is any instance that provides access to files from these products. ``pansat``
separates the representation of data products and providers because a single
data product may be provided by different providers.

.. image:: diagrams/pansat_download.png

In the code, data products and providers are represented by the
:py:class:`~pansat.products.Product` and
:py:class:`~pansat.download.providers.data_provider.DataProvider`
classes, respectively. Both of these classes are abstract base classes. This
means that they do not contain much code themselves but rather that they
define a functional interface for data products and providers.
The essential attributes and functions  are listed in the diagram above.

An example of a ``DataProvider`` class is
:py:class:`~pansat.download.providers.ges_disc.GesDiscProviderDay`, which
provides access to a range of GPM files available from the `NASA GES DISC
<https://gpm1.gesdisc.eosdis.nasa.gov/data/>`_ servers. The data
providers are instantiated at the bottom of their parent module, which makes
them available within ``pansat``.


Adding products
````````````````

Adding new products to pansat therefore typically involves the following steps:

  1. Add a new product class to the :py:mod:`pansat.products` module. Instances
     of this class represent specific variants of the product. For satellite
     data there variants may be different processing levels (1, 2, 3), for
     reanalysis data they may be specific selections of variables.

  2. Add a data provider class to the :py:mod:`pansat.download.providers` module.
     This data provider class must inherit from the
     :py:class:`pansat.download.providers.data_provider.DataProvider` class, whose
     constructor will take care of registering the instances of the new provider
     class and making them available througout ``pansat``.

File indexing
-------------

In addition to functionality to download and open datasets, ``pansat`` provides
functionality to index the temporal and spatial coverage of data files, which
aims to simplify retrieving relevant files and matching files from different
products in space and time. This functionality is principally implemented by
the :py:class:`~pansat.catalog.index.Index` and :py:class:`~pansat.catalog.Catalog`
classes. An index holds information about the temporal or spatial coverage for
a *single product*, while a catalog is just a collection of indices for multiple
products.

In order to allow indexing the temporal and spatial coverage of data files,
``pansat`` makes use of granules. A granule represents the geographical and
temporal coverage of a segment of data. They are represented by the
:py:class:`~pansat.granule.Granule` class.


Auxiliary classes
-----------------

Two, additional important classes in ``pansat`` are the
:py:class:`~pansat.file_record.FileRecord` and the
:py:class:`~pansat.time.TimeRange` classes.

The :py:class:`~pansat.file_record.FileRecord` class represents references
to specific product files. These files may be located on a remote server
or on the local machine, or both, and therefore the
:py:class:`~pansat.file_record.FileRecord` class is used to uniquely identify
these file instances.

As its name suggests, :py:class:`~pansat.time.TimeRange` class represents time
ranges. Since most product files have finite temporal coverage determining
whether a file contains data for a specific time requires dealing with time
ranges. The :py:class:`~pansat.time.TimeRange` aims to simplify these type of
calculations. The class also tries to hide away much of the complexityu of
dealing with times and dates in Python.

Contributing
============

Everyone is welcome to expand the functionality and capabilities of ``pansat``.
To do so, either start by opening an issue on the ``pansat``'s `GitHub repository
<https://github.com/see-mof/pansat>`_ or by opening a `pull request
<https://docs.github.com/en/free-pro-team@latest/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`_
with your proposed changes or addition.

