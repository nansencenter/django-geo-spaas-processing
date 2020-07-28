#!/usr/bin/env python
"""
Script used to run the unit tests for this package. A module of the 'tests' package can be given as
CLI argument, in which case only the tests in this module are run.
"""

import sys

import django
from django.test.utils import get_runner
from geospaas_processing.settings import django_settings

if __name__ == "__main__":
    if not django.conf.settings.configured:
        django.conf.settings.configure(**django_settings)
        django.setup()

    test_module = f".{sys.argv[1]}" if len(sys.argv) >= 2 else ''

    TestRunner = get_runner(django.conf.settings)
    test_runner = TestRunner()
    failures = test_runner.run_tests(["tests" + test_module])
    sys.exit(bool(failures))
