#!/bin/bash
cd /github/workspace/pansat
pip install .
pytest test/unit_tests

cd notebooks
jupyter nbconvert --to notebook *.ipynb
