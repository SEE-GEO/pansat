from pansat.download.providers.cloudnet import CloudNetProvider

import xarray as xr


def test_download(tmpdir):
    """
    Test download of specific CloudNet file.
    """
    url = (
        'https://cloudnet.fmi.fi/api/download/product/b61aaa18'
        '-ac82-4f76-8b87-5da24dde928e/20201001_palaiseau_iwc-Z-T-method.nc'
    )
    provider = CloudNetProvider("iwc")
    provider.download_file(url, tmpdir)

    filename = url.split("/")[-1]
    data = xr.load_dataset(tmpdir / filename)
    assert "iwc" in data.variables
