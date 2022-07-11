#!/bin/bash

pytest -W error::DeprecationWarning --cov-report term-missing --cov=pyramm -m 'not slow' --maxfail=1 -v tests/
rm .coverage*
