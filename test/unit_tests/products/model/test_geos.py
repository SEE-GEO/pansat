"""
Tests for the pansat.products.model.geos module.
"""

import pytest

from pansat.products.model.geos import i3nvasm


FILENAMES = {i3nvasm.name: "GEOS.fp.asm.inst3_2d_asm_Nx.20140220_0000.V01.nc4"}


@pytest.mark.parametrize("product", [i3nvasm])
def test_filename_regexp(product):
    fname = FILENAMES[product.name]
    assert product.matches(fname)
