"""
pansat.download.providers
=========================

The ``providers`` module provides classes to access online services that allow
downloading data. Objects of these data provider classes provide download access
for specific data products. For this,  an object of the data provider class
must be instantiated with a given product object. If the product is available
from the data provider class, the resulting data provider instance can be used
to download files of the data product.

"""
from pansat.download.providers.data_provider import DataProvider
from pansat.download.providers.copernicus import CopernicusProvider
from pansat.download.providers.icare import IcareProvider
from pansat.download.providers.noaa import NOAAProvider
from pansat.download.providers.meteo_france import GeoservicesProvider
from pansat.download.providers.laads_daac import LAADSDAACProvider
from pansat.download.providers.goes_aws import GOESAWSProvider
from pansat.download.providers.himawari_aws import HimawariAWSProvider
from pansat.download.providers.iowa_state import IowaStateProvider
from pansat.download.providers.cloudnet import CloudnetProvider
from pansat.download.providers.cloudsat_dpc import CloudSatDPCProvider
import pansat.download.providers.iowa_state


ALL_PROVIDERS = [
    CopernicusProvider,
    IcareProvider,
    NOAAProvider,
    GeoservicesProvider,
    LAADSDAACProvider,
    GOESAWSProvider,
    HimawariAWSProvider,
    IowaStateProvider,
    CloudnetProvider,
    CloudSatDPCProvider,
]
