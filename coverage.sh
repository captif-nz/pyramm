#!/bin/bash

pytest -W error::DeprecationWarning --cov-report term-missing --cov=pyramm --maxfail=1 -v tests/
rm .coverage*