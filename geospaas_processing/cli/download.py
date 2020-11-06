"""
Download files that are selected from the database using input criteria.
"""
import argparse
import json
import os
from datetime import datetime

import django
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzutc
from django.contrib.gis.geos import GEOSGeometry

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_processing.settings')
django.setup()
import geospaas_processing.downloaders as downloaders


def main():
    """
    Instantiation and calling the download() method of DownloadManager based on created argparser.
    """
    arg = cli_parse_args()
    cumulative_query = json.loads(arg.query) if arg.query else {}
    if arg.geometry:
        cumulative_query['geographic_location__geometry__intersects'] = GEOSGeometry(arg.geometry)
    designated_begin, designated_end = find_designated_time(arg.rel_time_flag, arg.begin, arg.end)
    download_manager = downloaders.DownloadManager(
        download_directory=arg.down_dir.rstrip(os.path.sep),
        provider_settings_path=arg.config_file,
        max_downloads=int(arg.safety_limit),
        use_file_prefix=arg.use_filename_prefix,
        store_address=arg.address_storage,
        time_coverage_start__gte=designated_begin,
        time_coverage_end__lte=designated_end,
        **cumulative_query
    )
    download_manager.download()


def find_designated_time(rel_time_flag, begin, end):
    """find the starting time and the ending time of downloading based on two cases of 1)relative or
    2)absolute times definition by user."""
    if rel_time_flag:
        designated_begin = datetime.now().replace(tzinfo=tzutc()) + relativedelta(
            hours=-abs(int(begin)))
        designated_end = datetime.now().replace(tzinfo=tzutc())
    else:
        designated_begin = datetime.strptime(begin, "%Y-%m-%d").replace(tzinfo=tzutc())
        designated_end = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=tzutc())
    return designated_begin, designated_end


def cli_parse_args():
    """creates proper arguments parser with 'argparse' of python."""
    parser = argparse.ArgumentParser(description='Process the arguments of entry_point')
    parser.add_argument(
        '-d', '--down_dir', required=True, type=str,
        help="Absolute path for downloading files. If the path depends on the file date, usage "
        + "of %Y, %m and other placeholders interpretable by strftime is accepted")
    parser.add_argument(
        '-b', '--begin', required=True, type=str,
        help="Absolute starting date for download in the format YYYY-MM-DD or (if used together "
        + "with '-r') lag in hours relative to today")
    parser.add_argument(
        '-e', '--end', required=True, type=str,
        help="Absolute ending date for download in the format YYYY-MM-DD or (if used together "
        + "with '-r') has no influence.")
    parser.add_argument(
        '-r', '--rel_time_flag', required=False, action='store_true',
        help="The flag that distinguishes between the two cases of time calculation (1.time-lag "
        + "from now 2.Two different points in time) based on its ABSENCE or PRESENCE in the "
        + "arguments.")
    parser.add_argument(
        '-s', '--safety_limit', required=False, type=str, default="400",
        help="The upper limit (safety limit) of number of datasets that are going to be downloaded."
        + " If there total number of requested dataset for downloading exceeds this number, the "
        + "downloading process does not commence.")
    parser.add_argument(
        '-p', '--use_filename_prefix', action='store_true',
        help="The flag that distinguishes between the two cases of having files WITH or WITHOUT "
        + "file prefix when downloaded")
    parser.add_argument(
        '-a', '--address_storage', action='store_true',
        help="The flag that distinguishes between the two cases of whether storing the local "
        + "address of file in the dataset or not by its its ABSENCE or PRESENCE in the arguments.")
    parser.add_argument(
        '-g', '--geometry', required=False, type=str,
        help="The 'wkt' string of geometry which is acceptable by 'GEOSGeometry' of django")
    parser.add_argument(
        '-c', '--config_file', required=False, type=str,
        help="The absolute path to the config file that is needed for configuring the downloading "
        + "process. default is the same folder of the 'download.py' file")
    parser.add_argument(
        '-q', '--query', required=False, type=str,
        help="query exposed by user to confine the search result of database for downloading them. "
        + "It is a string which must be acceptable by json.loads() to for deserialization of one- "
        + "or multi-criteria limitation. "
        + "After deserialization, it must be a list of query that are readable by django filter."
        + "for example a dictionary of elements like "
        + "{\"dataseturi__uri__contains\":\"osisaf\", \"source__instrument__short_name"
        + "__icontains\":\"AMSR2\"}"
    )
    return parser.parse_args()

if __name__ == "__main__":
    main()
