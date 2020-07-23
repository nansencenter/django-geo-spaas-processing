#!/usr/bin/env python
"""
Script used to run the unit tests for this package. A module of the 'tests' package can be given as
CLI argument, in which case only the tests in this module are run.
"""

import os
import sys

import django
from django.conf import settings
from django.test.utils import get_runner

if __name__ == "__main__":
    os.environ['DJANGO_SETTINGS_MODULE'] = 'geospaas_processing.settings'
    django.setup()

    test_module = f".{sys.argv[1]}" if len(sys.argv) >= 2 else ''

    TestRunner = get_runner(settings)
    test_runner = TestRunner()
    failures = test_runner.run_tests(["tests" + test_module])
    sys.exit(bool(failures))
