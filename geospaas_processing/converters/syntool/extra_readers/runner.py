#!/usr/bin/env python
# coding=utf-8
"""Runs custom syntool readers"""
import argparse
import importlib

def parse_cli_args():
    """Parse CLI arguments for custom syntool converters"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', dest='reader_module', required=True)
    parser.add_argument('-i', dest='in_file', required=True)
    parser.add_argument('-o', dest='out_dir', required=True)
    parser.add_argument('-opt', dest='opt', nargs='+', default=tuple())
    return parser.parse_args()

def run_converter_function():
    """Run the provided function with the arguments from the command
    line
    """
    args = parse_cli_args()

    reader_module = importlib.import_module(args.reader_module)

    options = {}
    for option in args.opt:
        key, value = option.split('=')
        options[key.strip()] = value.strip()

    reader_module.convert(args.in_file, args.out_dir, **options)

if __name__ == '__main__':
    run_converter_function()
