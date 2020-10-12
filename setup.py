import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pansat",
    version="0.0.1",
    author="Julia Kukulies, Simon Pfreundschuh, Franz Kanngießer, Hannah Imhof",
    description="Download, extraction, remapping and analysis of satellite and climate data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SEE-MOF/pansat",
    packages=setuptools.find_packages(),
    install_requires=[
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU Affero",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
