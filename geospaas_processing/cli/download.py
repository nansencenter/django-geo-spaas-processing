"""
Script for cli of downloading repo. This script accepts the argunemt from sys.args by the
help of python "argparse" and download the dataset files based on the input arguments.
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from distutils.util import strtobool
import json
import django
import ast

from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from django.contrib.gis.geos import GEOSGeometry

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_processing.settings')
django.setup()
from geospaas.catalog.models import Dataset
import geospaas_processing.downloaders as downloaders


def main(ar):
    cumulative_limitation = {}
    limitation_list = json.loads(ar.limitation)
    for limitation in limitation_list:
        cumulative_limitation.update(limitation)
    if ar.rel_time_flag:
        designated_begin = datetime.now().replace(tzinfo=tzutc()) + relativedelta(
            hours=-abs(int(ar.begin)))
        designated_end = datetime.now().replace(tzinfo=tzutc())
    else:
        designated_begin = datetime.strptime(ar.begin, "%Y.%m.%d:%H").replace(
            tzinfo=tzutc())
        designated_end = datetime.strptime(ar.end, "%Y.%m.%d:%H").replace(tzinfo=tzutc())
    download_manager = downloaders.DownloadManager(
        download_directory=ar.down_dir.rstrip(os.path.sep)+os.path.sep+ar.dir_struct.lstrip(
            os.path.sep),
        provider_settings_path=ar.config_file,
        # 400 dataset per day is allowed
        max_downloads=400*(abs((designated_end-designated_begin).days)+1),
        use_file_prefix=ar.use_filename_prefix,
        time_coverage_start__gte=designated_begin,
        time_coverage_end__lte=designated_end,
        geographic_location__geometry__intersects=GEOSGeometry(ar.geometry),
        **cumulative_limitation
    )
    download_manager.download()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Process the arguments of entry_point (all must be in str)')
    parser.add_argument('-d', '--down_dir', required=True, type=str,
    help="This must be an absolute path which downloaded files store in it")
    parser.add_argument('-b', '--begin', required=True, type=str,
    help="This must be an a time (starting point of the time) in the format of '%Y.%m.%d:%H' which "
    "is acceptable by python strptime or the extend of time lag from now in the case of relative "
    "time flag (-r) in the terms of hours")
    parser.add_argument('-e', '--end', required=True, type=str,
    help="This must be an a time (ending point of the time) in the format of '%Y.%m.%d:%H' which "
    "is acceptable by python strptime. It is an effective option only in the case of ABSENCE of "
    "relative time flag (-r).")
    parser.add_argument('-r', '--rel_time_flag', required=False, action='store_true',
    help="The flag that distinguishes between the two cases of time calcation (1.time-lag from now "
    "2.Two different points in time) based on its ABSENCE or PRESENCE in the arguments.")
    parser.add_argument('-s', '--dir_struct', required=False, type=str,
    help="xxxxxxxxxxx")
    parser.add_argument('-p', '--use_filename_prefix', action='store_true',
    help="The flag that distinguishes between the two cases of having files WITH or WITHOUT file "
    "prefix when downloaded")
    parser.add_argument('-g', '--geometry', required=False, type=str,
    help="The 'wkt' string of geometry which is acceptable by 'GEOSGeometry' of django")
    parser.add_argument('-c', '--config_file', required=False, type=str,
    help="The absolute path to the config file that is needed for configuring the downloading "
    "process. default is the same place of the 'download.py' file")
    parser.add_argument('-l', '--limitation', required=True, type=str,
    help="""limitation exposed by user to confine the search result of database for downloading them."
    "It is a string which must be acceptable by json.loads() to for deserialization of one- or "
    "multi-criteria limitation."
    "After deserialization, it must be a list of limitations that are readable by django filter."
    "for example a list of elements like {'dataseturi__uri__contains': 'osisaf'}""")
    ar = parser.parse_args()
    main(ar)
