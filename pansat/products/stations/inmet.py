"""
pansat.products.stations.inmet
==============================

This module provides a product class for data from the Brazilian INMET stations.
"""

from datetime import datetime
from pathlib import Path
import re
from typing import List, Optional

import shapely
import numpy as np
import pandas as pd
import xarray as xr

import pansat
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.products import Product, FilenameRegexpMixin
from pansat import geometry


_STATION_DATA = None


def get_station_data() -> xr.Dataset:
    """
    Get xarray.Dataset containing the station data.
    """
    global _STATION_DATA
    file_path = Path(__file__).parent / "files" / "inmet_stations.csv"
    if _STATION_DATA is None:
        data_frame = pd.read_csv(file_path, index_col="station_code")
        dataset = xr.Dataset.from_dataframe(data_frame)
        dataset = dataset.rename({
            "station_code": "station",
            "latitude": "latitude",
            "longitude": "longitude", 
            "altitude": "altitude",
            "region": "region",
            "state": "state",
            "name": "station_name",
        })

        lons = dataset.longitude.data
        lats = dataset.latitude.data
        valid = (
            (-180 <= lons) * (lons <= 180) *
            (-90 <= lats) * (lats <= 90)
        )
        dataset = dataset[{"station": valid}]
        _STATION_DATA = dataset

    return _STATION_DATA


class InmetStationFile(FilenameRegexpMixin, Product):
    """
    Class representing data from Brazilian INMET stations.
    """

    def __init__(self, stations: Optional[List[str]] = None):
        self._name = "station_data"
        self.stations = stations
        super().__init__()

        self.filename_regexp = re.compile(
            rf"INMET_([^_]+)_([^_]+)_([^_]+)_([^_]+)_([\d\-]+)_A_([\d\-]+)\.CSV"
        )

    @property
    def name(self) -> str:
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, "inmet", self._name])

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(str): Filename of an INMET CSV file.

        Returns:
            datetime object representing the timestamp of the filename.
        """
        match = self.filename_regexp.match(filename)
        if match is None:
            raise ValueError(f"Filename {filename} doesn't match INMET pattern")
        
        start_date_str = match.group(5)  # e.g., "01-01-2025"
        start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
        return start_date

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Implements interface to extract temporal coverage of file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                f"Provided file record with filename {rec.filename} does not "
                "match the product's filename regexp "
                f"{self.filename_regexp.pattern}."
            )

        start_date_str = match.group(5)  # e.g., "01-01-2025"
        end_date_str = match.group(6)    # e.g., "31-12-2025"
        
        start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
        end_date = datetime.strptime(end_date_str, "%d-%m-%Y")

        return TimeRange(start_date, end_date)

    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
            
        # Extract station code from filename
        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                f"Provided file record with filename {rec.filename} does not "
                "match the product's filename regexp."
            )
        
        station_code = match.group(3)  # e.g., "A001"
        
        station_data = get_station_data()
        if self.stations is not None:
            station_data = station_data.sel(station=self.stations)

        # Try to find the specific station
        try:
            station_info = station_data.sel(station=station_code)
            lon = float(station_info.longitude.data)
            lat = float(station_info.latitude.data)
            point = shapely.Point(lon, lat)
            return geometry.ShapelyGeometry(point)
        except (KeyError, ValueError):
            # If station not found in registry, try to extract from file
            if rec.local_path and rec.local_path.exists():
                with open(rec.local_path, 'r', encoding='latin-1') as f:
                    lines = f.readlines()
                    if len(lines) >= 6:
                        lat_line = lines[4].split(';')
                        lon_line = lines[5].split(';')
                        if len(lat_line) > 1 and len(lon_line) > 1:
                            lat = float(lat_line[1].replace(',', '.'))
                            lon = float(lon_line[1].replace(',', '.'))
                            point = shapely.Point(lon, lat)
                            return geometry.ShapelyGeometry(point)
            
            # Fallback to a default point if we can't extract coordinates
            point = shapely.Point(0, 0)
            return geometry.ShapelyGeometry(point)

    @property
    def default_destination(self):
        """
        Default destination directory for INMET files.
        """
        return Path("inmet")

    def __str__(self):
        return self.name

    def open(self, rec: FileRecord, slcs: Optional[dict[str, slice]] = None) -> xr.Dataset:
        """
        Open file as xarray dataset.

        Args:
            rec: A FileRecord whose local_path attribute points to a local INMET CSV file to open.
            slcs: An optional dictionary of slices to use to subset the data to load.

        Return:
            An xarray.Dataset containing the loaded data.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        file_path = rec.local_path

        # Read metadata from header lines
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
        
        # Extract station metadata
        region = lines[0].split(';')[1].strip()
        state = lines[1].split(';')[1].strip()
        station_name = lines[2].split(';')[1].strip()
        station_code = lines[3].split(';')[1].strip()
        latitude = float(lines[4].split(';')[1].replace(',', '.'))
        longitude = float(lines[5].split(';')[1].replace(',', '.'))
        altitude = float(lines[6].split(';')[1].replace(',', '.'))

        # Read data starting from line 9 (index 8)
        data_frame = pd.read_csv(
            file_path, 
            skiprows=8, 
            sep=';',
            encoding='latin-1',
            parse_dates=['Data'],
            date_format='%Y/%m/%d'
        )

        # Clean and process the data
        # Replace commas with dots in numeric columns and convert to float
        numeric_columns = data_frame.columns[2:]  # Skip 'Data' and 'Hora UTC' columns
        for col in numeric_columns:
            if col in data_frame.columns:
                data_frame[col] = data_frame[col].astype(str).str.replace(',', '.')
                data_frame[col] = pd.to_numeric(data_frame[col], errors='coerce')

        # Create datetime column by combining date and hour
        data_frame['Hora UTC'] = data_frame['Hora UTC'].astype(str).str.replace(' UTC', '')
        data_frame['Hora UTC'] = pd.to_numeric(data_frame['Hora UTC'], errors='coerce')
        data_frame['datetime'] = data_frame['Data'] + pd.to_timedelta(data_frame['Hora UTC'] * 60 / 100, unit='m')
        
        # Set datetime as index
        data_frame = data_frame.set_index('datetime')
        
        # Drop original date and hour columns
        data_frame = data_frame.drop(['Data', 'Hora UTC'], axis=1)

        # Rename columns to more standard English names
        # Handle multiple possible encodings and column name variations
        column_mapping = {}
        
        # Map based on partial matches to handle encoding issues
        for col in data_frame.columns:
            col_lower = col.lower()
            if 'precipita' in col_lower and 'total' in col_lower:
                column_mapping[col] = 'precipitation'
            elif 'pressao atmosferica ao nivel' in col_lower or 'pressao atmosferica ao nível' in col_lower:
                column_mapping[col] = 'pressure'
            elif 'temperatura do ar' in col_lower and 'bulbo seco' in col_lower:
                column_mapping[col] = 'temperature'
            elif 'temperatura do ponto de orvalho' in col_lower:
                column_mapping[col] = 'dew_point_temperature'
            elif 'umidade relativa do ar' in col_lower and 'horaria' in col_lower:
                column_mapping[col] = 'relative_humidity'
            elif 'vento' in col_lower and 'velocidade' in col_lower:
                column_mapping[col] = 'wind_speed'
            elif 'vento' in col_lower and 'dire' in col_lower:
                column_mapping[col] = 'wind_direction'
            elif 'radiacao global' in col_lower or 'radiação global' in col_lower:
                column_mapping[col] = 'solar_radiation'
        
        # Apply column renaming
        data_frame = data_frame.rename(columns=column_mapping)

        # Create xarray dataset
        dataset = xr.Dataset.from_dataframe(data_frame)
        dataset = dataset.rename({'datetime': 'time'})

        # Add station metadata as coordinates
        dataset['station'] = station_code
        dataset['station_name'] = station_name
        dataset['latitude'] = latitude
        dataset['longitude'] = longitude
        dataset['altitude'] = altitude
        dataset['region'] = region
        dataset['state'] = state

        # Set coordinates
        dataset = dataset.set_coords(['station', 'station_name', 'latitude', 'longitude', 'altitude', 'region', 'state'])

        return dataset


station_data = InmetStationFile()
