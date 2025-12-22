"""
pansat.products.reanalysis.ibtracks
===================================

Provides a pansat product representing IBTracks files.
"""
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import numpy as np
import pandas as pd
import xarray as xr

import pansat
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.products import Product, GranuleProduct, FilenameRegexpMixin
from pansat.products import Granule
from pansat import geometry


class IBTracksProduct(FilenameRegexpMixin, GranuleProduct):
    """
    Product class for IBTracks (International Best Track Archive for Climate Stewardship) data files.
    
    IBTracks provides global tropical cyclone best track data including storm location, 
    intensity, and structure parameters at typically 3-hour intervals.
    """

    def __init__(self):
        """
        Initialize the IBTracks product.
        
        IBTracks data files can be either CSV or NetCDF format with filenames like:
        - ibtracs.ALL.list.v04r00.csv
        - ibtracs.ALL.list.v04r00.nc
        - ibtracs.NA.list.v04r00.csv (North Atlantic basin)
        - ibtracs.EP.list.v04r00.nc (Eastern Pacific basin)
        - etc.
        """
        # Match IBTracks file pattern: ibtracs.[basin].list.v[version].[csv|nc]
        # Use ^ and $ to match complete filename
        self.filename_regexp = re.compile(
            r"^ibtracs\.([A-Z]{2,3})\.list\.v(\d{2}r\d{2})\.(csv|nc)$"
        )
        GranuleProduct.__init__(self)

    @property
    def name(self) -> str:
        """
        The product name that uniquely identifies the product within pansat.
        """
        return "reanalysis.ibtracks"

    @property
    def default_destination(self) -> Path:
        """
        The default destination for IBTracks data.
        """
        return Path("ibtracks")

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Extract temporal coverage from IBTracks file.
        
        For IBTracks files, we need to read the file to determine the actual
        temporal coverage since the filename doesn't contain date information.
        
        Args:
            rec: FileRecord pointing to the IBTracks file.
            
        Returns:
            TimeRange representing the temporal coverage of the file.
        """
        if rec.local_path is None:
            raise ValueError(
                "IBTracks product requires a local file to determine temporal coverage."
            )
        
        # Read just the first few rows to get date columns and determine the actual range
        try:
            df = pd.read_csv(rec.local_path, nrows=0)  # Just get column names
            df_sample = pd.read_csv(rec.local_path, nrows=1000)  # Sample to check date format
            
            # IBTracks typically has ISO_TIME column with timestamps
            if 'ISO_TIME' in df.columns:
                # Read all ISO_TIME values to get full time range
                df_full = pd.read_csv(rec.local_path, usecols=['ISO_TIME'])
                times = pd.to_datetime(df_full['ISO_TIME'], errors='coerce').dropna()
                return TimeRange(times.min(), times.max())
            else:
                # Fallback: look for other time columns
                time_cols = [col for col in df.columns if 'TIME' in col.upper() or 'DATE' in col.upper()]
                if time_cols:
                    df_full = pd.read_csv(rec.local_path, usecols=[time_cols[0]])
                    times = pd.to_datetime(df_full[time_cols[0]], errors='coerce').dropna()
                    return TimeRange(times.min(), times.max())
                else:
                    raise ValueError("No time column found in IBTracks file")
                    
        except Exception as e:
            raise RuntimeError(f"Failed to determine temporal coverage of IBTracks file: {e}")

    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Extract spatial coverage from IBTracks file.
        
        Args:
            rec: FileRecord pointing to the IBTracks file.
            
        Returns:
            Geometry representing the spatial coverage.
        """
        if rec.local_path is None:
            raise ValueError(
                "IBTracks product requires a local file to determine spatial coverage."
            )
        
        try:
            # Read latitude/longitude columns to determine spatial extent
            df = pd.read_csv(rec.local_path, usecols=['LAT', 'LON'], dtype={'LAT': float, 'LON': float})
            
            # Remove invalid coordinates
            df = df.dropna()
            df = df[(df['LAT'] >= -90) & (df['LAT'] <= 90)]
            df = df[(df['LON'] >= -180) & (df['LON'] <= 360)]
            
            if df.empty:
                # Fallback to global coverage if no valid coordinates found
                return geometry.LonLatRect(-180, -90, 180, 90)
            
            # Convert longitude to -180 to 180 range if needed
            df['LON'] = df['LON'].where(df['LON'] <= 180, df['LON'] - 360)
            
            # Get bounding box with some padding
            min_lat = max(df['LAT'].min() - 5, -90)
            max_lat = min(df['LAT'].max() + 5, 90)
            min_lon = max(df['LON'].min() - 5, -180)
            max_lon = min(df['LON'].max() + 5, 180)
            
            return geometry.LonLatRect(min_lon, min_lat, max_lon, max_lat)
            
        except Exception as e:
            # Fallback to global coverage if parsing fails
            return geometry.LonLatRect(-180, -90, 180, 90)

    def open(self, rec: FileRecord, slcs: Optional[dict] = None) -> xr.Dataset:
        """
        Open IBTracks file (CSV or NetCDF) as xarray Dataset.
        
        Args:
            rec: FileRecord pointing to the IBTracks file.
            slcs: Optional dictionary of slices to subset the data.
            
        Returns:
            xarray.Dataset containing the IBTracks data.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        
        if rec.local_path is None:
            raise ValueError("IBTracks product requires a local file path.")
        
        file_path = Path(rec.local_path)
        file_ext = file_path.suffix.lower()
        
        try:
            if file_ext == '.nc':
                # Open NetCDF file directly
                ds = xr.open_dataset(rec.local_path)
                
                # Apply slices if provided
                if slcs:
                    ds = ds.isel(slcs)
                    
            elif file_ext == '.csv':
                # Read CSV file and convert to xarray
                df = pd.read_csv(rec.local_path, low_memory=False)
                
                # Convert time columns to datetime
                time_cols = ['ISO_TIME']
                for col in time_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Convert to xarray Dataset
                ds = df.to_xarray()
                
                # Apply slices if provided
                if slcs:
                    ds = ds.isel(slcs)
                    
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            
            # Add standard metadata if not present
            if 'title' not in ds.attrs:
                ds.attrs['title'] = 'IBTracks - International Best Track Archive for Climate Stewardship'
            if 'source' not in ds.attrs:
                ds.attrs['source'] = 'https://www.ncei.noaa.gov/products/international-best-track-archive'
            if 'description' not in ds.attrs:
                ds.attrs['description'] = 'Global tropical cyclone best track data'
            
            # Set coordinate attributes if available and not already set
            for var_name, attrs in [
                ('LAT', {'long_name': 'Latitude', 'units': 'degrees_north', 'standard_name': 'latitude'}),
                ('LON', {'long_name': 'Longitude', 'units': 'degrees_east', 'standard_name': 'longitude'}),
                ('ISO_TIME', {'long_name': 'ISO timestamp', 'standard_name': 'time'}),
                ('lat', {'long_name': 'Latitude', 'units': 'degrees_north', 'standard_name': 'latitude'}),
                ('lon', {'long_name': 'Longitude', 'units': 'degrees_east', 'standard_name': 'longitude'}),
                ('time', {'long_name': 'Time', 'standard_name': 'time'})
            ]:
                if var_name in ds.variables:
                    for attr, value in attrs.items():
                        if attr not in ds[var_name].attrs:
                            ds[var_name].attrs[attr] = value
            
            return ds
            
        except Exception as e:
            raise RuntimeError(f"Failed to open IBTracks file: {e}")

    def get_granules(self, rec: FileRecord) -> List[Granule]:
        """
        Extract granules from IBTracks file.
        
        For IBTracks data, each individual storm/cyclone can be treated as a granule.
        Each storm has a unique storm ID and represents a complete cyclone track.
        
        Args:
            rec: FileRecord pointing to the IBTracks file.
            
        Returns:
            List of Granule objects, one per storm.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
            
        if rec.local_path is None:
            raise ValueError("IBTracks product requires a local file to get granules.")
        
        try:
            ds = self.open(rec)
            
            # Find storm ID column (different names possible)
            storm_id_col = None
            for col in ['SID', 'STORM_ID', 'storm_id', 'sid']:
                if col in ds.variables:
                    storm_id_col = col
                    break
                    
            if storm_id_col is None:
                # Fallback: treat entire file as one granule
                return [Granule(
                    rec,
                    self.get_temporal_coverage(rec),
                    self.get_spatial_coverage(rec)
                )]
            
            # Get unique storm IDs
            storm_ids = np.unique(ds[storm_id_col].values)
            storm_ids = storm_ids[~pd.isna(storm_ids)]  # Remove NaN values
            
            granules = []
            for storm_id in storm_ids:
                # Get indices for this storm
                storm_mask = ds[storm_id_col] == storm_id
                storm_indices = np.where(storm_mask)[0]
                
                if len(storm_indices) == 0:
                    continue
                    
                # Extract storm data to determine temporal/spatial coverage
                storm_data = ds.isel(index=storm_indices)
                
                # Get time range for this storm
                time_col = None
                for col in ['ISO_TIME', 'time', 'TIME']:
                    if col in storm_data.variables:
                        time_col = col
                        break
                
                if time_col is not None:
                    times = storm_data[time_col].values
                    times = times[~pd.isna(times)]
                    if len(times) > 0:
                        time_range = TimeRange(
                            pd.to_datetime(times.min()),
                            pd.to_datetime(times.max())
                        )
                    else:
                        time_range = self.get_temporal_coverage(rec)
                else:
                    time_range = self.get_temporal_coverage(rec)
                
                # Get spatial coverage for this storm
                lat_col, lon_col = None, None
                for lat_name in ['LAT', 'lat', 'latitude']:
                    if lat_name in storm_data.variables:
                        lat_col = lat_name
                        break
                for lon_name in ['LON', 'lon', 'longitude']:
                    if lon_name in storm_data.variables:
                        lon_col = lon_name
                        break
                        
                if lat_col is not None and lon_col is not None:
                    lats = storm_data[lat_col].values
                    lons = storm_data[lon_col].values
                    
                    # Remove invalid coordinates
                    valid_mask = ~(np.isnan(lats) | np.isnan(lons))
                    lats = lats[valid_mask]
                    lons = lons[valid_mask]
                    
                    if len(lats) > 0:
                        # Convert longitude to -180 to 180 if needed
                        lons = np.where(lons > 180, lons - 360, lons)
                        
                        # Create bounding box with padding
                        min_lat = max(np.min(lats) - 2, -90)
                        max_lat = min(np.max(lats) + 2, 90)
                        min_lon = max(np.min(lons) - 2, -180)
                        max_lon = min(np.max(lons) + 2, 180)
                        
                        spatial_coverage = geometry.LonLatRect(min_lon, min_lat, max_lon, max_lat)
                    else:
                        spatial_coverage = self.get_spatial_coverage(rec)
                else:
                    spatial_coverage = self.get_spatial_coverage(rec)
                
                # Create granule with storm-specific slice information
                granule = Granule(
                    rec,
                    time_range,
                    spatial_coverage,
                    storm_indices[0],  # Start index
                    storm_indices[-1] + 1,  # End index (exclusive)
                    additional_info={'storm_id': storm_id, 'storm_indices': storm_indices}
                )
                
                granules.append(granule)
            
            return granules
            
        except Exception as e:
            # Fallback: treat entire file as one granule
            return [Granule(
                rec,
                self.get_temporal_coverage(rec), 
                self.get_spatial_coverage(rec)
            )]

    def open_granule(self, granule: Granule) -> xr.Dataset:
        """
        Open a specific granule (storm) from IBTracks data.
        
        Args:
            granule: Granule object representing a specific storm.
            
        Returns:
            xarray.Dataset containing data for the specific storm.
        """
        # Get the full dataset
        ds = self.open(granule.file_record)
        
        # Apply granule-specific slicing if available
        if hasattr(granule, 'additional_info') and granule.additional_info:
            if 'storm_indices' in granule.additional_info:
                storm_indices = granule.additional_info['storm_indices']
                ds = ds.isel(index=storm_indices)
            elif hasattr(granule, 'start_index') and hasattr(granule, 'end_index'):
                # Use start/end indices if available
                ds = ds.isel(index=slice(granule.start_index, granule.end_index))
        
        return ds

    @property
    def variables(self):
        """
        Common variables found in IBTracks data.
        """
        return [
            'LAT', 'LON', 'WMO_WIND', 'WMO_PRES', 'USA_WIND', 'USA_PRES',
            'TOKYO_WIND', 'TOKYO_PRES', 'CMA_WIND', 'CMA_PRES', 'HKO_WIND', 
            'HKO_PRES', 'NEWDELHI_WIND', 'NEWDELHI_PRES', 'REUNION_WIND', 
            'REUNION_PRES', 'BOM_WIND', 'BOM_PRES', 'NADI_WIND', 'NADI_PRES',
            'WELLINGTON_WIND', 'WELLINGTON_PRES', 'DIST2LAND', 'LANDFALL'
        ]


# Create product instance
ibtracks = IBTracksProduct()
