import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

# TODO: create this email address
setuptools.setup(
    name="pansat",
    version="0.0.1",
    author="Julia Kukulies, Simon Pfreundschuh, Franz Kanngießer, Hannah Imhof",
    author_email="contact@pansat.com",
    description="Download, extraction, remapping and analysis of satellite and climate data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SEE-MOF/pansat",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU Affero",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "cryptography>=3.1"
    ],
    python_requires='>=3.6',
)
