
import json
import argparse

from datetime import datetime
from dateutil.tz import tzutc
from dateutil.relativedelta import relativedelta
from django.contrib.gis.geos import GEOSGeometry
from geospaas.catalog.models import Dataset

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

def create_cumulative_query(arg):
    """find the requested datasets based on the time, geometry, and query of the arguments of cli"""
    cumulative_query = json.loads(arg.query) if arg.query else {}
    if arg.geometry:
        cumulative_query['geographic_location__geometry__intersects'] = GEOSGeometry(arg.geometry)
    designated_begin, designated_end = find_designated_time(
        arg.rel_time_flag, arg.begin, arg.end)
    cumulative_query['time_coverage_start__gte'] = designated_begin
    cumulative_query['time_coverage_end__lte'] = designated_end
    return cumulative_query

def parse_common_args():
    """Instantiates and creates common arguments of parser which works with 'argparse' of python."""
    parser = argparse.ArgumentParser(description='Process the arguments of entry_point')
    parser.add_argument(
        '-d', '--destination_path', required=True, type=str,
        help="Absolute path for downloading or copying files. For downloading, If the path depends "
        + "on the file date, usage of %Y, %m and other placeholders interpretable by strftime is "
        + "accepted")
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
        '-g', '--geometry', required=False, type=str,
        help="The 'wkt' string of geometry which is acceptable by 'GEOSGeometry' of django")
    parser.add_argument(
        '-q', '--query', required=False, type=str,
        help="query exposed by user to confine the search result of database for copying them. "
        + "It is a string which must be acceptable by json.loads() to for deserialization of one- "
        + "or multi-criteria limitation. "
        + "After deserialization, it must be a list of query that are readable by django filter."
        + "for example a dictionary of elements like "
        + "{\"dataseturi__uri__contains\":\"osisaf\", \"source__instrument__short_name"
        + "__icontains\":\"AMSR2\"}")
    return parser
