"""
Contains fixtures that are automatically available in all test files.
"""
from datetime import datetime, timedelta
from pathlib import PurePath, Path

import numpy as np
import pytest

HAS_HDF4 = False
try:
    from pyhdf.SD import SD, SDC
    HAS_HDF4 = True
except ImportError:
    pass


HAS_HDF5 = False
try:
    from h5py import File
    HAS_HDF5 = True
except ImportError:
    HAS_HDF5 = False


@pytest.fixture()
def test_identities(monkeypatch):
    """
    Fixture that makes all tests use the test identities file that contains the
    test login data for data providers.
    """
    test_identity_file = Path(
        PurePath(__file__).parent / "test_data" / "identities.json"
    )
    monkeypatch.setattr("pansat.download.accounts._IDENTITY_FILE", test_identity_file)
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)
    import pansat.download.accounts as accs

    accs.parse_identity_file()


@pytest.fixture()
def product_description(tmp_path):
    """
    Populates a temporary path with a product description file.
    """
    with open(tmp_path / "product.ini", "w") as descr:
        descr.write(PRODUCT_DESCRIPTION)
    yield tmp_path


@pytest.fixture()
def hdf4_product_data(product_description):
    """
    Populates a temporary directory with a product description and test
    files in HDF4 format.
    """
    tmp_path = product_description

    filename_pattern = (
        "data_file_{start_time}_{end_time}_"
        "{lon_min:0.2f}_{lat_min:0.2f}_{lon_max:0.2f}_{lat_max:0.2f}.hdf"
    )

    remote_path = tmp_path / "remote"
    remote_path.mkdir()

    delta_t = timedelta(hours=1)
    for i in range(4):
        start_time = datetime(2020, 1, 1, i)
        end_time = start_time + delta_t

        lats = np.linspace(-5, 5, 200, dtype="float32")
        lons = np.linspace(i * 10, (i + 1) * 10, 200, dtype="float32")

        filename = filename_pattern.format(
            start_time=start_time.strftime("%Y%m%d%H%M%S"),
            end_time=end_time.strftime("%Y%m%d%H%M%S"),
            lon_min=lons.min(),
            lat_min=lats.min(),
            lon_max=lons.max(),
            lat_max=lats.max()
        )
        file_path = remote_path / filename
        output_file = SD(str(file_path), SDC.WRITE | SDC.CREATE)

        att = output_file.attr("attribute_1")
        att.set(SDC.CHAR, 'test')

        v_lons = output_file.create(
            'longitude',
            SDC.FLOAT32,
            200
        )
        v_lons[:] = lons
        v_lats = output_file.create(
            'latitude',
            SDC.FLOAT32,
            200
        )
        v_lats[:] = lats

        surface_precip = np.random.rand(200, 200).astype("float32")
        v_sp = output_file.create(
            'surface_precip',
            SDC.FLOAT32,
            (200, 200)
        )
        v_sp[:] = surface_precip
        output_file.end()

    yield tmp_path


@pytest.fixture()
def hdf5_product_data(product_description):
    """
    Populates a temporary directory with a product description and test
    files in HDF5 format.
    """
    tmp_path = product_description
    remote_path = tmp_path / "remote"
    remote_path.mkdir()

    filename_pattern = (
        "data_file_{start_time}_{end_time}_"
        "{lon_min:0.2f}_{lat_min:0.2f}_{lon_max:0.2f}_{lat_max:0.2f}.h5"
    )
    delta_t = timedelta(hours=1)

    for i in range(4):
        start_time = datetime(2020, 1, 1, i)
        end_time = start_time + delta_t

        lats = np.linspace(-5, 5, 200)
        lons = np.linspace(i * 10, (i + 1) * 10, 200)

        filename = filename_pattern.format(
            start_time=start_time.strftime("%Y%m%d%H%M%S"),
            end_time=end_time.strftime("%Y%m%d%H%M%S"),
            lon_min=lons.min(),
            lat_min=lats.min(),
            lon_max=lons.max(),
            lat_max=lats.max()
        )
        output_file = File(remote_path / filename, "w")
        v_lons = output_file.create_dataset(
            'longitude',
            200,
            dtype="float32"

        )
        v_lons[:] = lons
        v_lats = output_file.create_dataset(
            'latitude',
            200,
            dtype="float32",
        )
        v_lats[:] = lats

        surface_precip = np.random.rand(200, 200)
        v_sp = output_file.create_dataset(
            'surface_precip',
            (200, 200),
            dtype="float32",
        )
        v_sp[:] = surface_precip
        output_file.close()

    yield tmp_path


PRODUCT_DATA = [
        pytest.param(
            'hdf4_product_data',
            marks=pytest.mark.skipif(
                not HAS_HDF4,
                reason="HDF4 library not available."
            )
        ),
        pytest.param(
            'hdf5_product_data',
            marks=pytest.mark.skipif(
                not HAS_HDF4,
                reason="HDF4 library not available."
            )
        ),
]

######################################################################
# Product descriptions
######################################################################


PRODUCT_DESCRIPTION = """
[test-description]
type = properties
name = test-description

[dimension_1]
type = dimension
name = dimension_1

[dimension_2]
type = dimension
name = dimension_2

[data]
type = variable
name = surface_precip
dimensions = ["dimension_1", "dimension_2"]
description = Some test data.
unit = test

[longitude]
type = longitude_coordinate
name = longitude
dimensions = ["dimension_1"]
description = Coordinate 1.

[latitude]
type = latitude_coordinate
name = latitude
dimensions = ["dimension_2"]
description = Coordinate 2.

[attribute_1]
type = attribute
name = attribute_1
dimensions = []
description = An attribute.
"""
