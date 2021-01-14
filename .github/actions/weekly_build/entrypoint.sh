#!/bin/bash

# Setup identities file.
mkdir -p /root/.config/pansat
cp /github/workspace/test/unit_tests/test_data/identities.json /root/.config/pansat

cd /github/workspace/
ls
pip install -e /github/workspace
pytest /github/workspace/test/unit_tests

jupyter nbconvert --to notebook --execute /github/workspace/notebooks/*.ipynb
jupyter nbconvert --to notebook --execute /github/workspace/notebooks/products/*.ipynb

