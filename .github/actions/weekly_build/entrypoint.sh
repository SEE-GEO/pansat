#!/bin/bash
cd /github/workspace/
ls
pip install .
pytest test/unit_tests
sudo rm -rf .pytest_cache

cd notebooks
jupyter nbconvert --to notebook *.ipynb
cd products
jupyter nbconvert --to notebook *.ipynb


