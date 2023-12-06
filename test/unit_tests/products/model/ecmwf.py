from datetime import datetime

from pansat.products.model import ecmwf


def test_get_weekdays():
    """
    Ensure that selecting Monday and Thursday over a month yields dates on
    which ECMWF forecasts are available.
    """
    start = datetime(2022, 1, 1)
    end = datetime(2022, 2, 1)
    dates = ecmwf.get_weekdays(start, end, [0, 3])
    assert dates[0] == datetime(2022, 1, 3)
    assert dates[-1] == datetime(2022, 1, 31)
    assert len(dates) == 9
