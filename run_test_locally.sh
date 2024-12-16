#!/bin/bash

# Purpose: Run Airflow DAG tests locally before committing and pushing
# Author: Gary A. Stafford
# Modified: 2022-01-16
# Run this command first:
# python3 -m pip install --user -U -r requirements_local_tests.txt

bold=$(tput bold)
normal=$(tput sgr0)


echo "\n⌛ Starting isort test..."
python -m isort --check-only .

echo "\n⌛ Starting Flake8 test..."
python -m flake8 .

echo "\n⌛ Starting Black test..."
black . --check


echo "${bold}\n All tests completed successfully! 🥳\n${normal}"