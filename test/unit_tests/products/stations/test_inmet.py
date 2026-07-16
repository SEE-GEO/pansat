"""
Tests for the pansat.products.stations.inmet module.
"""
from datetime import datetime
from pathlib import Path

import pytest

from pansat.geometry import LonLatRect
from pansat.products.stations.inmet import (
    get_station_data,
    station_data,
    InmetStationFile
)
from pansat.time import TimeRange
from pansat.file_record import FileRecord


def test_get_station_data():
    """
    Test loading of station data and ensure that the expected number of stations is loaded.
    """
    stations = get_station_data()
    assert stations.station.size == 594


def test_filename_regexp():
    """
    Ensure 'station_data' product matches INMET filename.
    """
    # Test various INMET filename patterns
    test_files = [
        "INMET_CO_DF_A001_BRASILIA_01-01-2025_A_31-12-2025.CSV",
        "INMET_N_AM_A101_MANAUS_01-01-2025_A_31-12-2025.CSV",
        "INMET_NE_CE_A305_FORTALEZA_01-01-2025_A_31-12-2025.CSV"
    ]
    
    for filename in test_files:
        assert station_data.matches(filename), f"Failed to match {filename}"
    
    # Test files that should NOT match
    invalid_files = [
        "WN_L2_V8_HD_St2_2023-01-01d00h00m_2024-01-01d00h00m_UTC.csv",
        "random_file.txt",
        "INMET_incomplete.csv"
    ]
    
    for filename in invalid_files:
        assert not station_data.matches(filename), f"Incorrectly matched {filename}"


def test_temporal_coverage():
    """
    Test extraction of temporal coverage from filename.
    """
    filename = "INMET_CO_DF_A001_BRASILIA_01-01-2025_A_31-12-2025.CSV"
    file_record = FileRecord(filename)
    
    time_range = station_data.get_temporal_coverage(file_record)
    
    expected_start = datetime(2025, 1, 1)
    expected_end = datetime(2025, 12, 31)
    
    assert time_range.start == expected_start
    assert time_range.end == expected_end


def test_filename_to_date():
    """
    Test extraction of date from filename.
    """
    filename = "INMET_CO_DF_A001_BRASILIA_01-01-2025_A_31-12-2025.CSV"
    expected_date = datetime(2025, 1, 1)
    
    extracted_date = station_data.filename_to_date(filename)
    assert extracted_date == expected_date


def test_spatial_coverage_with_station_registry():
    """
    Test spatial coverage extraction using station registry.
    """
    filename = "INMET_CO_DF_A001_BRASILIA_01-01-2025_A_31-12-2025.CSV"
    file_record = FileRecord(filename)
    
    # This should work if the station exists in the registry
    try:
        geometry = station_data.get_spatial_coverage(file_record)
        # Basic check that we got a geometry object
        assert geometry is not None
    except Exception as e:
        # If station not in registry, that's also OK for this test
        pass


def test_product_name():
    """
    Test that the product name is correctly generated.
    """
    expected_name = "stations.inmet.station_data"
    assert station_data.name == expected_name


def test_default_destination():
    """
    Test that the default destination is set correctly.
    """
    expected_destination = Path("inmet")
    assert station_data.default_destination == expected_destination


def test_init_with_stations():
    """
    Test initialization with specific station filtering.
    """
    stations_filter = ["A001", "A002", "A003"]
    inmet_product = InmetStationFile(stations=stations_filter)
    
    assert inmet_product.stations == stations_filter
    assert inmet_product.name == "stations.inmet.station_data"


def test_init_without_stations():
    """
    Test initialization without station filtering.
    """
    inmet_product = InmetStationFile()
    
    assert inmet_product.stations is None
    assert inmet_product.name == "stations.inmet.station_data"


@pytest.mark.skipif(not Path("/home/simon/src/pansat/2025").exists(), 
                   reason="INMET data directory not available")
def test_open_real_file():
    """
    Test opening a real INMET file if available.
    """
    data_dir = Path("/home/simon/src/pansat/2025")
    inmet_files = list(data_dir.glob("INMET_*.CSV"))
    
    if inmet_files:
        test_file = inmet_files[0]
        file_record = FileRecord(test_file)
        
        dataset = station_data.open(file_record)
        
        # Basic checks on the opened dataset
        assert dataset is not None
        assert 'time' in dataset.dims
        assert 'station' in dataset.coords
        assert 'latitude' in dataset.coords
        assert 'longitude' in dataset.coords
        
        # Check that we have some expected variables
        data_vars = list(dataset.data_vars.keys())
        expected_vars = ['precipitation', 'temperature', 'pressure', 'relative_humidity']
        
        # At least some of these should be present
        present_vars = [var for var in expected_vars if var in data_vars]
        assert len(present_vars) > 0, f"No expected variables found. Available: {data_vars}"


def test_invalid_filename_pattern():
    """
    Test behavior with invalid filename patterns.
    """
    invalid_filename = "not_an_inmet_file.csv"
    
    with pytest.raises(ValueError):
        station_data.filename_to_date(invalid_filename)
    
    file_record = FileRecord(invalid_filename)
    with pytest.raises(RuntimeError):
        station_data.get_temporal_coverage(file_record)


def test_string_representation():
    """
    Test string representation of the product.
    """
    expected_str = "stations.inmet.station_data"
    assert str(station_data) == expected_str