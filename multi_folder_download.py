import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from distutils.util import strtobool

import django
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from django.contrib.gis.geos import GEOSGeometry

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_processing.settings')

import geospaas_processing.downloaders as downloaders
from geospaas.catalog.models import Dataset


def main_function(ar):
    if strtobool(ar.rel_time_flag):
        designated_start_time = datetime.now().replace(tzinfo=tzutc())+ relativedelta(
            hours=-int(ar.rel_time_lag))
        designated_end_time = datetime.now().replace(tzinfo=tzutc())
    else:
        designated_start_time = datetime.strptime(ar.start_time, "%Y.%m.%d:%H").replace(
            tzinfo=tzutc())
        designated_end_time = datetime.strptime(ar.end_time, "%Y.%m.%d:%H").replace(tzinfo=tzutc())
    downloaders.DownloadManager.MAX_DOWNLOADS = 500
    download_manager = downloaders.DownloadManager(
        download_directory=ar.mount_dir.rstrip(os.path.sep)+os.path.sep+ar.dir_struct.lstrip(
            os.path.sep),
        use_file_prefix=strtobool(ar.use_filename_prefix),
        provider_settings_path=os.path.join(os.path.dirname(
            __file__), ar.config_folder, 'provider_settings.yml'),
        time_coverage_start__gte=designated_start_time,
        time_coverage_end__lte=designated_end_time,
        geographic_location__geometry__intersects=GEOSGeometry(ar.geometry),
        dataseturi__uri__contains=ar.criteria)
    download_manager.download()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Process the arguments of entry_point (all must be in str)')
    parser.add_argument('-md', '--mount_dir', required=True, type=str)
    parser.add_argument('-cr', '--criteria', required=True, type=str)
    parser.add_argument('-st', '--start_time', required=True, type=str)
    parser.add_argument('-et', '--end_time', required=True, type=str)
    parser.add_argument('-rtf', '--rel_time_flag', required=True, type=str)
    parser.add_argument('-rtl', '--rel_time_lag', required=True, type=str)
    parser.add_argument('-ds', '--dir_struct', required=True, type=str)
    parser.add_argument('-ufp', '--use_filename_prefix', required=False, type=str)
    parser.add_argument('-geo', '--geometry', required=False, type=str)
    parser.add_argument('-cdir', '--config_folder', required=False, type=str)
    ar = parser.parse_args()
    main_function(ar)
