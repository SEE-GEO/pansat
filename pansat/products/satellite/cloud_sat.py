"""
pansat.products.satellite.cloud_sat
===================================

This module defines the CloudSat product class, which represents all
supported CloudSat products.
"""

import re
import os
from datetime import datetime


class CloudSat:
    """
    The CloudSat class defines a generic interface for CloudSat products.

    Attributes:
        name(``str``): The name of the product
        variables(``list``): List of variable names provided by this
            product.
    """
    def __init__(self, name, variables):
        self.name = name
        self.variables = variables
        self.filename_regexp = re.compile(r"([\d]*)_([\d]*)_CS_"
                                          + name +
                                          r"_GRANULE_P_R([\d]*)_E([\d]*)\.*")

    def matches(self, filename):
        return self.filename_regexp.match(filename)

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a CloudSat product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        filename = os.path.basename(filename)
        filename = filename.split("_")[0]
        return datetime.strptime(filename, "%Y%j%H%M%S")

l1b_cpr = CloudSat("1B-CPR", [])
