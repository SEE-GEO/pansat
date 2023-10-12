import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pansat",
    version="0.0.dev1",
    author="Julia Kukulies, Simon Pfreundschuh, Franz Kanngießer, Hannah Imhof",
    description="Download, extraction, remapping and analysis of satellite and climate data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SEE-MOF/pansat",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "cryptography>=3.1",
        "numpy",
        "cdsapi",
        "requests",
        "requests-cache",
        "xarray",
        "pyproj",
        "appdirs",
        "boto3",
        "paramiko",
        "shapely",
        "rich",
        "click"
    ],
    setup_requires=["pytest-runner"],
    tests_require=[
        "sphinx_rtd_theme",
        "pytest",
        "appdirs",
        "scipy",
        "beautifulsoup4",
        "lxml",
        "netcdf4",
    ],
    python_requires=">=3.7",
    entry_points={"console_scripts": ["pansat=pansat.cli:pansat_cli"]},
    include_package_data=True,
)
