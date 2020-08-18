#!/bin/bash

pytest --cov-report term-missing --cov=captif_db --maxfail=1 -v tests/