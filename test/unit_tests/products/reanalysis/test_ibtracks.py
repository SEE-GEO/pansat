"""
Tests for the pansat.products.reanalysis.ibtracks module.
"""

import pytest
from pathlib import Path

from pansat import FileRecord
from pansat.products.reanalysis.ibtracks import ibtracks


def test_product_properties():
    """
    Test basic product properties.
    """
    assert ibtracks.name == "reanalysis.ibtracks"
    assert ibtracks.default_destination == Path("ibtracks")
    assert len(ibtracks.variables) > 0
    assert "LAT" in ibtracks.variables
    assert "LON" in ibtracks.variables


def test_matches_valid_csv_filenames():
    """
    Test that valid CSV IBTracks filenames are matched correctly.
    """
    valid_csv_filenames = [
        "ibtracs.ALL.list.v04r00.csv",
        "ibtracs.NA.list.v04r00.csv",
        "ibtracs.EP.list.v04r00.csv", 
        "ibtracs.WP.list.v04r00.csv",
        "ibtracs.NI.list.v04r00.csv",
        "ibtracs.SI.list.v04r00.csv",
        "ibtracs.SP.list.v04r00.csv",
        "ibtracs.SA.list.v04r00.csv",
        "ibtracs.ALL.list.v04r01.csv",
        "ibtracs.ALL.list.v03r03.csv",
    ]
    
    for filename in valid_csv_filenames:
        assert ibtracks.matches(filename), f"Failed to match valid filename: {filename}"


def test_matches_valid_netcdf_filenames():
    """
    Test that valid NetCDF IBTracks filenames are matched correctly.
    """
    valid_nc_filenames = [
        "ibtracs.ALL.list.v04r00.nc",
        "ibtracs.NA.list.v04r00.nc",
        "ibtracs.EP.list.v04r00.nc",
        "ibtracs.WP.list.v04r00.nc", 
        "ibtracs.NI.list.v04r00.nc",
        "ibtracs.SI.list.v04r00.nc",
        "ibtracs.SP.list.v04r00.nc",
        "ibtracs.SA.list.v04r00.nc",
        "ibtracs.ALL.list.v04r01.nc",
        "ibtracs.ALL.list.v03r03.nc",
    ]
    
    for filename in valid_nc_filenames:
        assert ibtracks.matches(filename), f"Failed to match valid filename: {filename}"


def test_matches_invalid_filenames():
    """
    Test that invalid filenames are not matched.
    """
    invalid_filenames = [
        "some_other_file.csv",
        "ibtracs.ALL.v04r00.csv",  # Missing .list
        "ibtracs.list.v04r00.csv",  # Missing basin
        "ibtracs.ALL.list.csv",    # Missing version
        "ibtracs.ALL.list.v04r00.txt",  # Wrong extension
        "ibtracs.ALL.list.v04r00.dat",  # Wrong extension  
        "ibtracs.ALL.list.v04r00",      # No extension
        "ibtracks.ALL.list.v04r00.csv", # Wrong prefix (ibtracks vs ibtracs)
        "ibtracs.all.list.v04r00.csv",  # Lowercase basin
        "ibtracs.ALL.LIST.v04r00.csv",  # Uppercase LIST
        "ibtracs.ALL.list.V04R00.csv",  # Uppercase version
        "ibtracs.ABCD.list.v04r00.csv", # Too long basin code
        "ibtracs.A.list.v04r00.csv",    # Too short basin code
        "ibtracs.ALL.list.v4r0.csv",    # Invalid version format
        "ibtracs.ALL.list.v04r0.csv",   # Invalid version format
        "ibtracs.123.list.v04r00.csv",  # Numeric basin
        "ibtracs.A1.list.v04r00.csv",   # Mixed alphanumeric basin
        "",                              # Empty filename
        ".csv",                         # Just extension
        "ibtracs.ALL.list.v04r00.csv.bak",  # Additional extension
    ]
    
    for filename in invalid_filenames:
        assert not ibtracks.matches(filename), f"Incorrectly matched invalid filename: {filename}"


def test_matches_edge_case_filenames():
    """
    Test edge cases for filename matching.
    """
    # Test with Path objects
    valid_path = Path("ibtracs.ALL.list.v04r00.csv")
    assert ibtracks.matches(valid_path)
    
    invalid_path = Path("some_other_file.csv")
    assert not ibtracks.matches(invalid_path)
    
    # Test with FileRecord objects
    valid_record = FileRecord("ibtracs.ALL.list.v04r00.nc")
    assert ibtracks.matches(valid_record)
    
    invalid_record = FileRecord("invalid_file.nc")
    assert not ibtracks.matches(invalid_record)


def test_matches_basin_codes():
    """
    Test that all valid basin codes are matched.
    """
    valid_basins = ["ALL", "NA", "EP", "WP", "NI", "SI", "SP", "SA"]
    
    for basin in valid_basins:
        csv_filename = f"ibtracs.{basin}.list.v04r00.csv"
        nc_filename = f"ibtracs.{basin}.list.v04r00.nc"
        
        assert ibtracks.matches(csv_filename), f"Failed to match basin: {basin} (CSV)"
        assert ibtracks.matches(nc_filename), f"Failed to match basin: {basin} (NetCDF)"


def test_matches_version_codes():
    """
    Test that various version codes are matched correctly.
    """
    valid_versions = ["v04r00", "v04r01", "v03r03", "v05r00", "v10r99"]
    
    for version in valid_versions:
        csv_filename = f"ibtracs.ALL.list.{version}.csv"
        nc_filename = f"ibtracs.ALL.list.{version}.nc"
        
        assert ibtracks.matches(csv_filename), f"Failed to match version: {version} (CSV)"
        assert ibtracks.matches(nc_filename), f"Failed to match version: {version} (NetCDF)"


def test_filename_regexp_groups():
    """
    Test that the filename regexp extracts the correct groups.
    """
    filename = "ibtracs.EP.list.v04r01.nc"
    match = ibtracks.filename_regexp.match(filename)
    
    assert match is not None, "Regexp should match valid filename"
    assert match.group(1) == "EP", "First group should be basin code"
    assert match.group(2) == "04r01", "Second group should be version"
    assert match.group(3) == "nc", "Third group should be file extension"


def test_matches_case_sensitivity():
    """
    Test that filename matching is case sensitive where appropriate.
    """
    # These should match (correct case)
    assert ibtracks.matches("ibtracs.ALL.list.v04r00.csv")
    assert ibtracks.matches("ibtracs.EP.list.v04r00.nc")
    
    # These should not match (incorrect case)
    assert not ibtracks.matches("IBTRACS.ALL.list.v04r00.csv")
    assert not ibtracks.matches("ibtracs.all.list.v04r00.csv")
    assert not ibtracks.matches("ibtracs.ALL.LIST.v04r00.csv")
    assert not ibtracks.matches("ibtracs.ALL.list.V04R00.csv")
    assert not ibtracks.matches("ibtracs.ALL.list.v04r00.CSV")
    assert not ibtracks.matches("ibtracs.ALL.list.v04r00.NC")


def test_matches_with_directory_paths():
    """
    Test that matching works correctly with full file paths.
    """
    # For string paths, the matches method uses the full string, not just filename
    # So we need to test with just filenames or use Path objects
    valid_filenames = [
        "ibtracs.ALL.list.v04r00.csv",
        "ibtracs.NA.list.v04r00.nc",
        "ibtracs.WP.list.v04r01.csv",
        "ibtracs.EP.list.v04r00.nc",
    ]
    
    # Test with Path objects (these extract just the filename)
    valid_paths = [
        Path("/data/ibtracks/ibtracs.ALL.list.v04r00.csv"),
        Path("/home/user/data/ibtracs.NA.list.v04r00.nc"),
        Path("data/ibtracs.WP.list.v04r01.csv"),
        Path("ibtracs.EP.list.v04r00.nc"),
    ]
    
    for filename in valid_filenames:
        assert ibtracks.matches(filename), f"Failed to match filename: {filename}"
        
    for path in valid_paths:
        assert ibtracks.matches(path), f"Failed to match path: {path}"
    
    # Test invalid cases
    invalid_paths = [
        Path("/data/ibtracks/invalid_file.csv"),
        Path("/home/user/data/ibtracs.ALL.v04r00.nc"),  # Missing .list
        Path("data/other_file.csv"),
    ]
    
    for path in invalid_paths:
        assert not ibtracks.matches(path), f"Incorrectly matched invalid path: {path}"