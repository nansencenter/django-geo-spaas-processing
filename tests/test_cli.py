"""Unit tests for cli"""
import json
import sys
import unittest
import unittest.mock as mock
from datetime import datetime

import geospaas_processing.cli.download as cli_download
import geospaas_processing.downloaders as downloaders
from dateutil.tz import tzutc
from django.contrib.gis.geos import GEOSGeometry
from freezegun import freeze_time


class DownlaodingCLITestCase(unittest.TestCase):
    """Tests for the cli of downloading """

    def setUp(self):
        sys.argv = [
            "",
            '-d', "/test_folder/%Y_nh_polstere",
            '-b', "200",
            '-e', "2020-08-22",
            '-r',
            '-n',"100",
            '-p',
            '-g', "POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))",
            '-c', "/config_folder/config_file.yml",
            '-q',
            '{"dataseturi__uri__contains": "osisaf", "source__instrument__short_name__icontains": "AMSR2"}',
            ]

    def test_extract_arg(self):
        """shall return the correct argument values based on the 'sys.argv' """
        arg = cli_download.parse_args()
        self.assertEqual(arg.begin, '200')
        self.assertEqual(arg.config_file, '/config_folder/config_file.yml')
        self.assertEqual(arg.down_dir, '/test_folder/%Y_nh_polstere')
        self.assertEqual(arg.end, '2020-08-22')
        self.assertEqual(arg.geometry, 'POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))')
        self.assertEqual(arg.number_per_day, '100')
        self.assertEqual(
            arg.query,
            '{"dataseturi__uri__contains": "osisaf", "source__instrument__short_name__icontains": "AMSR2"}')
        # testing the flag performance
        self.assertTrue(arg.rel_time_flag)
        self.assertTrue(arg.use_filename_prefix)
        sys.argv.remove('-r')
        sys.argv.remove('-p')
        arg = cli_download.parse_args()
        self.assertFalse(arg.rel_time_flag)
        self.assertFalse(arg.use_filename_prefix)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_json_deserializer(self, mock_downloadmethod, mock_downloadmanagerinit):
        """'json.loads' shall deserializer the whole string that comes after '-q' """
        arg = cli_download.parse_args()
        with mock.patch('json.loads') as mock_json:
            cli_download.main(arg)
        self.assertIn((
            '{"dataseturi__uri__contains": "osisaf", "source__instrument__short_name__icontains": "AMSR2"}',),
            mock_json.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_lack_of_calling_json_deserializer_when_no_query_appears(
        self, mock_downloadmethod, mock_downloadmanagerinit):
        """'json.loads' should not called when nothing comes after '-q' """
        arg = cli_download.parse_args()
        arg.query=""
        with mock.patch('json.loads') as mock_json:
            cli_download.main(arg)
        self.assertIsNone(mock_json.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_DownloadManager_without_file_prefix(
            self, mock_downloadmethod, mock_downloadmanagerinit):
        """shall return the proper call (with 'use_file_prefix': False) for the case of "
        "lack of file prefix ('-p') in arguments"""
        sys.argv.remove('-p')
        sys.argv.remove('-r')
        sys.argv[4] = '2019-10-22'
        arg = cli_download.parse_args()
        cli_download.main(arg)
        self.assertIn({
            'download_directory': '/test_folder/%Y_nh_polstere',
            'geographic_location__geometry__intersects':
            GEOSGeometry('POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))'),
            'max_downloads': 30600,
            'provider_settings_path': '/config_folder/config_file.yml',
            'time_coverage_end__lte': datetime(2020, 8, 22, 0, 0, tzinfo=tzutc()),
            'time_coverage_start__gte': datetime(2019, 10, 22, 0, 0, tzinfo=tzutc()),
            'dataseturi__uri__contains': 'osisaf',
            'source__instrument__short_name__icontains': 'AMSR2',
            'use_file_prefix': False
        },
            mock_downloadmanagerinit.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_DownloadManager_with_file_prefix(
            self, mock_downloadmethod, mock_downloadmanagerinit):
        "shall return the proper call for the case of lack of two definite time points in arguments"
        sys.argv.remove('-r')
        sys.argv[4] = '2019-10-22'
        arg = cli_download.parse_args()
        cli_download.main(arg)
        self.assertIn({
            'download_directory': '/test_folder/%Y_nh_polstere',
            'geographic_location__geometry__intersects':
            GEOSGeometry('POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))'),
            'max_downloads': 30600,
            'provider_settings_path': '/config_folder/config_file.yml',
            'time_coverage_end__lte': datetime(2020, 8, 22, 0, 0, tzinfo=tzutc()),
            'time_coverage_start__gte': datetime(2019, 10, 22, 0, 0, tzinfo=tzutc()),
            'dataseturi__uri__contains': 'osisaf',
            'source__instrument__short_name__icontains': 'AMSR2',
            'use_file_prefix': True
        },
            mock_downloadmanagerinit.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_DownloadManager_with_relative_time(
            self, mock_downloadmethod, mock_downloadmanagerinit):
        "shall return the proper call for the case of lack of relative time definition in arguments"
        sys.argv[4] = "40"
        arg = cli_download.parse_args()
        with freeze_time("2012-01-14"):
            cli_download.main(arg)
        self.assertIn({
            'download_directory': '/test_folder/%Y_nh_polstere',
            'geographic_location__geometry__intersects':
            GEOSGeometry('POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))'),
            'max_downloads': 200,
            'provider_settings_path': '/config_folder/config_file.yml',
            'time_coverage_end__lte': datetime(2012, 1, 14, 0, 0, tzinfo=tzutc()),
            'time_coverage_start__gte': datetime(2012, 1, 12, 8, 0, tzinfo=tzutc()),
            'dataseturi__uri__contains': 'osisaf',
            'source__instrument__short_name__icontains': 'AMSR2',
            'use_file_prefix': True
        },
            mock_downloadmanagerinit.call_args)
