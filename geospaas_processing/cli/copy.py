"""
Download files that are selected from the database using input criteria.
"""
import argparse
import json
import ntpath
import os
from os import path
import shutil

import django
import geospaas_processing.cli.download as download_cli
from django.contrib.gis.geos import GEOSGeometry
from geospaas.catalog.managers import LOCAL_FILE_SERVICE
from geospaas.catalog.models import Dataset

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_processing.settings')
django.setup()


def main():
    """
    Copy the files based on the addressed stored in database to the destination folder.
    The destination folder as well as criteria for finding the desired files are input of this
    function.
    """
    arg = cli_parse_args()
    cumulative_query = json.loads(arg.query) if arg.query else {}
    if arg.geometry:
        cumulative_query['geographic_location__geometry__intersects'] = GEOSGeometry(arg.geometry)
    designated_begin, designated_end = download_cli.find_designated_time(
        arg.rel_time_flag, arg.begin, arg.end)
    cumulative_query['time_coverage_start__gte'] = designated_begin
    cumulative_query['time_coverage_end__lte'] = designated_end
    datasets = Dataset.objects.filter(**cumulative_query)
    for dataset in datasets:
        try:
            source_paths = dataset.dataseturi_set.filter(service=LOCAL_FILE_SERVICE)
        except IndexError:
            continue
        for source_path in source_paths:
            if path.isfile(source_path.uri):
                if arg.link:
                    try:
                        os.symlink(dst=arg.destination_path.rstrip(os.path.sep)
                                   + os.path.sep+ntpath.basename(source_path.uri),
                                   src=source_path.uri)
                    except FileExistsError:
                        pass
                else:
                    shutil.copy(src=source_path.uri, dst=arg.destination_path)
                if arg.flag_file:
                    with open(arg.destination_path.rstrip(os.path.sep)
                              + os.path.sep + ntpath.basename(source_path.uri)
                              + ".flag", "w+") as flag_file:
                        flag_file.write(f"type: {arg.type}" + os.linesep)
                        for urlname in dataset.dataseturi_set.exclude(service=LOCAL_FILE_SERVICE):
                            flag_file.write(f"url: {urlname.uri}" + os.linesep)


def cli_parse_args():
    """creates proper arguments parser with 'argparse' of python."""
    parser = argparse.ArgumentParser(description='Process the arguments of entry_point')
    parser.add_argument(
        '-d', '--destination_path', required=True, type=str,
        help="destination path for copying the files.")
    parser.add_argument(
        '-b', '--begin', required=True, type=str,
        help="Absolute starting date for download in the format YYYY-MM-DD.")
    parser.add_argument(
        '-e', '--end', required=True, type=str,
        help="Absolute ending date for download in the format YYYY-MM-DD.")
    parser.add_argument(
        '-r', '--rel_time_flag', required=False, action='store_true',
        help="The flag that distinguishes between the two cases of time calculation (1.time-lag "
        + "from now 2.Two different points in time) based on its ABSENCE or PRESENCE of this flag "
        + "in the arguments.")
    parser.add_argument(
        '-f', '--flag_file', required=False, action='store_true',
        help="The flag that distinguishes between the two cases of 1.writing the flag alongside the"
        + " copying or 2.just copy without writing it based on its ABSENCE or PRESENCE of this flag"
        + " in the arguments.")
    parser.add_argument(
        '-l', '--link', required=False, action='store_true',
        help="The flag that distinguishes between the two cases of 1.copying the file itself or 2."
        + "just copy a symboliclink of it at destination based on its ABSENCE or PRESENCE of this "
        + "flag in the arguments.")
    parser.add_argument(
        '-g', '--geometry', required=False, type=str,
        help="The 'wkt' string of geometry which is acceptable by 'GEOSGeometry' of django")
    parser.add_argument(
        '-t', '--type', required=False, type=str,
        help="The type of dataset (as a str) which is written in flag file for further processing.")
    parser.add_argument(
        '-q', '--query', required=False, type=str,
        help="query exposed by user to confine the search result of database for copying them. "
        + "It is a string which must be acceptable by json.loads() to for deserialization of one- "
        + "or multi-criteria limitation. "
        + "After deserialization, it must be a list of query that are readable by django filter."
        + "for example a dictionary of elements like "
        + "{\"dataseturi__uri__contains\":\"osisaf\", \"source__instrument__short_name"
        + "__icontains\":\"AMSR2\"}")

    return parser.parse_args()


if __name__ == "__main__":
    main()
