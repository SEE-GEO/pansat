#!/bin/bash

# Setup identities file.
cp /github/workspace/test/unit_tests/test_data/identities.json /
export PANSAT_IDENTITIES_FILE=/identities.json

cd /github/workspace/
ls
pip install -e /github/workspace
pytest -s /github/workspace/test/unit_tests

jupyter nbconvert --to notebook --inplace --execute /github/workspace/notebooks/*.ipynb
jupyter nbconvert --to notebook --inplace --execute /github/workspace/notebooks/products/*.ipynb
conda env export > /github/workspace/conda/pansat.yml

