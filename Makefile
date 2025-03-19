# A makefile to setup, re-setup and doing the transformations
#
# Options:
#  make transformations
#  make setup
#  make resetup

PYTHON = python3.7

## Make transformations
transformations:
	python transform.py -c transformation_config.yml

## Setup environment with dependencies
setup:
	${PYTHON} -m venv venv
	venv/bin/python -m pip install --upgrade pip setuptools
	venv/bin/python -m pip install -e ./bmf
	venv/bin/python bmf/setup.py clean

## Clean existing venv and re-setup environment
resetup: cleanvenv setup

# Clean
cleanvenv:
	rm -rf venv
