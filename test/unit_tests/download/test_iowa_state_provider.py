from pansat.download.providers.iowa_state import IowaStateProvider
from pansat.products.ground_based.mrms import mrms_precip_rate


def test_get_files_by_day():
    """
    Ensure that get_files_by_day method returns files of the right day.
    """
    provider = IowaStateProvider(mrms_precip_rate)
    files = provider.get_files_by_day(2021, 1)

    time = mrms_precip_rate.filename_to_date(files[0])
    assert time.year == 2021
    assert time.day == 1
    assert time.hour == 0
    assert time.minute == 0

    time = mrms_precip_rate.filename_to_date(files[-1])
    assert time.hour == 23
    assert time.minute == 58


def test_download_file(tmp_path):
    """
    Ensure that downloading a file works as expected.
    """
    destination = tmp_path / "test"
    provider = IowaStateProvider(mrms_precip_rate)
    files = provider.get_files_by_day(2021, 1)
    provider.download_file(files[0], destination)
