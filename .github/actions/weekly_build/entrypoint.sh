#!/bin/bash
cd /github/workspace/
ls
pip install .
pytest test/unit_tests

cd notebooks
jupyter nbconvert --to notebook *.ipynb
