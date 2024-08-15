"""
Tests for the pansat.products.sations.wegener_net module.
"""
from datetime import datetime

from pansat.geometry import LonLatRect
from pansat.products.stations.wegener_net import (
    get_station_data,
    station_data
)



def test_get_station_data():
    """
    Test loading of station data and ensure that the expected number of stations is loaded.
    """
    station_data = get_station_data()
    assert station_data.station.size == 367


def test_filename_regexp():
    """
    Ensure 'station_data' product matches station data filename.

    """
    assert station_data.matches("WN_L2_V8_HD_St2_2023-01-01d00h00m_2024-01-01d00h00m_UTC.csv")


def test_temporal_coverage():
    """
    Ensure 'station_data' extracts the correct temporal coverage.
    """
    time_range = station_data.get_temporal_coverage("WN_L2_V8_HD_St2_2023-01-01d00h00m_2024-01-01d00h00m_UTC.csv")
    time_range.start = datetime(2023, 1, 1)
    time_range.end = datetime(2024, 1, 1)


def test_spatial_coverage():
    """
    Ensure 'station_data' return the correct spatial coverage.
    """
    europe = LonLatRect(-25, 34, 45, 72)
    conus = LonLatRect(-125, 24.5, -66.5, 49.5)
    spatial_coverage = station_data.get_spatial_coverage("WN_L2_V8_HD_St2_2023-01-01d00h00m_2024-01-01d00h00m_UTC.csv")

    assert europe.covers(spatial_coverage)
    assert not conus.covers(spatial_coverage)
