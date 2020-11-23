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
import geospaas_processing.cli.util as util

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_processing.settings')
django.setup()
import geospaas_processing.downloaders as downloaders


def main():
    """
    Instantiation and calling the download() method of DownloadManager based on created argparser.
    """
    arg = cli_parse_args()
    cumulative_query = util.create_cumulative_query(arg)
    download_manager = downloaders.DownloadManager(
        download_directory=arg.down_dir.rstrip(os.path.sep),
        provider_settings_path=arg.config_file,
        max_downloads=int(arg.safety_limit),
        use_file_prefix=arg.use_filename_prefix,
        save_path=arg.save_path,
        **cumulative_query
    )
    download_manager.download()



def cli_parse_args():
    """Augment the common parser with additional specific arguments for downloading purposes."""
    parser = util.parse_common_args()
    parser.add_argument(
        '-d', '--down_dir', required=True, type=str,
        help="Absolute path for downloading files. If the path depends on the file date, usage "
        + "of %Y, %m and other placeholders interpretable by strftime is accepted")
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
        '-a', '--save_path', action='store_true',
        help="Save path to local file in the database based on its ABSENCE or PRESENCE.")
    parser.add_argument(
        '-c', '--config_file', required=False, type=str,
        help="The absolute path to the config file that is needed for configuring the downloading "
        + "process. default is the same folder of the 'download.py' file")
    return parser.parse_args()

if __name__ == "__main__":
    main()
