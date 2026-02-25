#!/bin/sh
set -e

ruff check .
pytest
