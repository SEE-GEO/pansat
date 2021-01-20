"""
pansat.download.provider.scapers.open_dap
=========================================

This module implements functions to extract available products from NASA
 OpenDAP servers.
"""
from pathlib import Path
from urllib.parse import urlparse, urlunparse, urljoin
import re

import requests
from bs4 import BeautifulSoup

URL = "https://gpm1.gesdisc.eosdis.nasa.gov/opendap/"


def retrieve_page(url):
    """
    Wrapper function to retrieve URL and raise StopIteration if
    that fails.
    """
    try:
        response = requests.get(url)
    except requests.ConnectionError:
        raise StopIteration()
    return response


def is_date(text):
    """
    Determines whether link text ends on a folder representing a year or a day.

    Args:
        Link text of a link pointing to a folder on NASA OpenDAP server.

    Return:
        True is the last components of the link name represent a year or day of
        year.
    """
    try:
        last_component = text.split("/")[-2]
        year = int(last_component)
        if year > 1900 and year < 2100:
            return True
        day = int(last_component)
        return (day > 0) and (day < 366)
    except ValueError:
        return False


def map_pages(function, parent_url, depth=10):
    """
    Recursive map over child URLs of given parent URL.

    Applies a given function over all sub-domains of a give URL whose link
    name looks like a file path.

    Args:
         function: A function that is applied to the ``text`` attribute of
              the ``requests.response`` object, which contains the web page.
              Results from this function is aggregated across all found sub-
              domains.
         parent_url: The URL from which to start the scraping.


    """
    response = retrieve_page(parent_url)
    soup = BeautifulSoup(response.text, features="lxml")

    products = function(response.text)
    if products:
        results = [(parent_url, products)]
    else:
        results = []

    #
    # Search child pages.
    #

    parent_url = urlparse(parent_url)
    for link in soup.find_all("a"):
        url = urlparse(link["href"])
        if not url.netloc:
            child_path = Path(parent_url.path) / url.path
            parent_path = Path(parent_url.path)
            is_parent = parent_path in child_path.parents
            is_html = str(child_path)[-5:].lower() == ".html"
            is_folder = link.text[-1] == "/"

            if is_folder and is_parent and is_html and depth > 0:
                child_url = urljoin(urlunparse(parent_url), url.path)
                new_depth = depth - 1
                products = map_pages(function, child_url, depth=new_depth)
                results += products
                if is_date(link.text):
                    break

    return results


GPM_PRODUCT_REGEXP = re.compile(
    r'"(\w*)(?:-(\w*))?\.(\w*)\.(\w*)\..*\.HDF5"', re.MULTILINE
)


def extract_gpm_products(text):
    """
    Extract all available GPM product from given domain.

    Args:
        url: The URL of the domain to search.

    Returns:
        A list of four-tuples containing level, platform, sensor and product
        name.
    """
    found = set(re.findall(GPM_PRODUCT_REGEXP, text))
    if len(found) > 0:
        return list(found)[0]
    return []
