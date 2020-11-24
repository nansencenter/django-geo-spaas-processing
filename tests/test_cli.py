"""Unit tests for cli"""
import os
import shutil
import sys
import tempfile
import unittest.mock as mock
from unittest.mock import call

from datetime import datetime
from pathlib import Path
from dateutil.tz import tzutc
from freezegun import freeze_time
import django.test
from django.contrib.gis.geos import GEOSGeometry

import geospaas_processing.cli.copy as cli_copy
import geospaas_processing.cli.download as cli_download
import geospaas_processing.cli.util as util
from geospaas.catalog.models import Dataset
from geospaas.catalog.managers import LOCAL_FILE_SERVICE


class DownlaodingCLITestCase(django.test.TestCase):
    """Tests for the cli of downloading """

    def setUp(self):
        sys.argv = [
            "",
            '-d', "/test_folder/%Y_nh_polstere",
            '-b', "200",
            '-e', "2020-08-22",
            '-r',
            '-a',
            '-s', "100",
            '-p',
            '-g', "POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))",
            '-c', "/config_folder/config_file.yml",
            '-q',
            '{"dataseturi__uri__contains": "osisaf", "source__instrument__short_name__icontains": '
            + '"AMSR2"}',
        ]

    def test_extract_arg(self):
        """shall return the correct argument values based on the 'sys.argv' """
        arg = cli_download.cli_parse_args()
        self.assertEqual(arg.begin, '200')
        self.assertEqual(arg.config_file, '/config_folder/config_file.yml')
        self.assertEqual(arg.destination_path, '/test_folder/%Y_nh_polstere')
        self.assertEqual(arg.end, '2020-08-22')
        self.assertEqual(arg.geometry, 'POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))')
        self.assertEqual(arg.safety_limit, '100')
        self.assertEqual(arg.query,
                         '{"dataseturi__uri__contains": "osisaf", '
                         + '"source__instrument__short_name__icontains": "AMSR2"}')
        # testing the flag presence
        self.assertTrue(arg.rel_time_flag)
        self.assertTrue(arg.save_path)
        self.assertTrue(arg.use_filename_prefix)
        sys.argv.remove('-r')
        sys.argv.remove('-a')
        sys.argv.remove('-p')
        arg = cli_download.cli_parse_args()
        self.assertFalse(arg.rel_time_flag)
        self.assertFalse(arg.save_path)
        self.assertFalse(arg.use_filename_prefix)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_json_deserializer(self, mock_download_method, mock_download_manager_init):
        """'json.loads' shall deserialize the whole string that comes after '-q' """
        arg = cli_download.cli_parse_args()
        with mock.patch('json.loads') as mock_json:
            cli_download.main()
        self.assertIn(
            ('{"dataseturi__uri__contains": "osisaf", '
             '"source__instrument__short_name__icontains": "AMSR2"}',),
            mock_json.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_lack_of_calling_json_deserializer_when_no_query_appears(
            self, mock_download_method, mock_download_manager_init):
        """'json.loads' should not called when nothing comes after '-q' """
        sys.argv.pop()
        sys.argv.pop()
        with mock.patch('json.loads') as mock_json:
            cli_download.main()
        mock_json.assert_not_called()

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_DownloadManager_without_file_prefix(
            self, mock_download_method, mock_download_manager_init):
        """shall return the proper call for the case of lack of file prefix ('-p') in arguments"""
        sys.argv.remove('-p')
        sys.argv.remove('-r')
        sys.argv[4] = '2019-10-22'
        arg = cli_download.cli_parse_args()
        cli_download.main()
        self.assertIn({
            'download_directory': '/test_folder/%Y_nh_polstere',
            'geographic_location__geometry__intersects':
            GEOSGeometry('POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))'),
            'max_downloads': 100,
            'provider_settings_path': '/config_folder/config_file.yml',
            'time_coverage_end__lte': datetime(2020, 8, 22, 0, 0, tzinfo=tzutc()),
            'time_coverage_start__gte': datetime(2019, 10, 22, 0, 0, tzinfo=tzutc()),
            'dataseturi__uri__contains': 'osisaf',
            'source__instrument__short_name__icontains': 'AMSR2',
            'use_file_prefix': False,
            'save_path': True
        }, mock_download_manager_init.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_DownloadManager_with_file_prefix(
            self, mock_download_method, mock_download_manager_init):
        """
        shall return the proper call for the case of lack of two definite time points in arguments
        """
        sys.argv.remove('-r')
        sys.argv[4] = '2019-10-22'
        arg = cli_download.cli_parse_args()
        cli_download.main()
        self.assertIn({
            'download_directory': '/test_folder/%Y_nh_polstere',
            'geographic_location__geometry__intersects':
            GEOSGeometry('POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))'),
            'max_downloads': 100,
            'provider_settings_path': '/config_folder/config_file.yml',
            'time_coverage_end__lte': datetime(2020, 8, 22, 0, 0, tzinfo=tzutc()),
            'time_coverage_start__gte': datetime(2019, 10, 22, 0, 0, tzinfo=tzutc()),
            'dataseturi__uri__contains': 'osisaf',
            'source__instrument__short_name__icontains': 'AMSR2',
            'use_file_prefix': True,
            'save_path': True
        }, mock_download_manager_init.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_DownloadManager_without_geometry(
            self, mock_download_method, mock_download_manager_init):
        """shall return the proper call for the case of lack of geometry in arguments"""
        sys.argv.remove('-g')
        sys.argv.remove("POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))")
        sys.argv.remove('-r')
        sys.argv[4] = '2019-10-22'
        arg = cli_download.cli_parse_args()
        cli_download.main()
        self.assertIn({
            'download_directory': '/test_folder/%Y_nh_polstere',
            'max_downloads': 100,
            'provider_settings_path': '/config_folder/config_file.yml',
            'time_coverage_end__lte': datetime(2020, 8, 22, 0, 0, tzinfo=tzutc()),
            'time_coverage_start__gte': datetime(2019, 10, 22, 0, 0, tzinfo=tzutc()),
            'dataseturi__uri__contains': 'osisaf',
            'source__instrument__short_name__icontains': 'AMSR2',
            'use_file_prefix': True,
            'save_path': True
        }, mock_download_manager_init.call_args)

    @mock.patch('geospaas_processing.downloaders.DownloadManager.__init__', return_value=None)
    @mock.patch('geospaas_processing.downloaders.DownloadManager.download')
    def test_correct_call_DownloadManager_with_relative_time(
            self, mock_download_method, mock_download_manager_init):
        """shall return the proper call for the case of relative time definition in arguments"""
        sys.argv[4] = "40"
        arg = cli_download.cli_parse_args()
        with freeze_time("2012-01-14"):
            cli_download.main()
        self.assertIn({
            'download_directory': '/test_folder/%Y_nh_polstere',
            'geographic_location__geometry__intersects':
            GEOSGeometry('POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))'),
            'max_downloads': 100,
            'provider_settings_path': '/config_folder/config_file.yml',
            'time_coverage_end__lte': datetime(2012, 1, 14, 0, 0, tzinfo=tzutc()),
            'time_coverage_start__gte': datetime(2012, 1, 12, 8, 0, tzinfo=tzutc()),
            'dataseturi__uri__contains': 'osisaf',
            'source__instrument__short_name__icontains': 'AMSR2',
            'use_file_prefix': True,
            'save_path': True
        }, mock_download_manager_init.call_args)

    def test_find_designated_time_function(self):
        """test the 'find_designated_time' function logics. answer_1, answer_2 are used for absolute
        and answer_3, answer_4 are used for relative timing"""
        answer_1, answer_2 = util.find_designated_time(False, '2019-10-22', '2020-08-22')
        self.assertEqual(answer_1, datetime(2019, 10, 22, 0, 0, tzinfo=tzutc()))
        self.assertEqual(answer_2, datetime(2020, 8, 22, 0, 0, tzinfo=tzutc()))
        with freeze_time("2012-01-14"):
            answer_3, answer_4 = util.find_designated_time(True, '500', '')
            self.assertEqual(answer_3, datetime(2011, 12, 24, 4, 0, tzinfo=tzutc()))
            self.assertEqual(answer_4, datetime(2012, 1, 14, 0, 0, tzinfo=tzutc()))


class CopyingCLITestCase(django.test.TestCase):
    """Tests for the cli of copying """

    fixtures = [os.path.join(os.path.dirname(__file__), 'data/test_data.json')]

    def test_extract_arg(self):
        """shall return the correct argument values based on the 'sys.argv' """
        sys.argv = [
            "",
            '-d', "/test_folder/",
            '-b', "200",
            '-e', "2018-11-18",
            '-r',
            '-f',
            '-l',
            '-g', "POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))",
            '-t', 'test_type',
            '-q',
            '{"dataseturi__uri__contains": "osisaf", "source__instrument__short_name__icontains": '
            + '"AMSR2"}'
        ]
        arg = cli_copy.cli_parse_args()
        self.assertEqual(arg.begin, '200')
        self.assertEqual(arg.destination_path, '/test_folder/')
        self.assertEqual(arg.end, '2018-11-18')
        self.assertEqual(arg.geometry, 'POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))')
        self.assertEqual(arg.type, 'test_type')
        self.assertEqual(arg.query,
                         '{"dataseturi__uri__contains": "osisaf", '
                         + '"source__instrument__short_name__icontains": "AMSR2"}')
        # testing the flag presence
        self.assertTrue(arg.rel_time_flag)
        self.assertTrue(arg.flag_file)
        self.assertTrue(arg.link)
        sys.argv.remove('-r')
        sys.argv.remove('-f')
        sys.argv.remove('-l')
        arg = cli_copy.cli_parse_args()
        self.assertFalse(arg.rel_time_flag)
        self.assertFalse(arg.flag_file)
        self.assertFalse(arg.link)

    def test_lack_of_calling_json_deserializer_when_no_query_appears_for_copying(self):
        """'json.loads' should not called when nothing comes after '-q' """
        sys.argv = [
            "",
            '-d', "/test_folder/",
            '-b', "200",
            '-e', "2018-11-18",
            '-r',
            '-f',
            '-l',
            '-g', "POLYGON ((-22 84, -22 74, 32 74, 32 84, -22 84))",
            '-t', 'test_type'
        ]
        with mock.patch('json.loads') as mock_json:
            cli_copy.main()
        mock_json.assert_not_called()

    @mock.patch('os.path.isfile', side_effect=[True, False, True, False])
    # The even side effects (the 'True' ones) are associated to the destination and the odd ones are
    # associated to the source path. It is because 'os.path.isfile' is used for evaluating both
    # source paths and destination paths.
    def test_correct_destination_folder_for_all_files_that_are_copied(self, mock_isfile):
        """ the copied file(s) shall be copied at the destination folder. This test for the cases
        that we have one more addition local file address in the database in the case of data
        downloaded once again for a second time in a different address."""
        sys.argv = [
            "",
            '-b', "2018-06-01",
            '-e', "2018-06-09",
            '-d', "/dst_folder/"
        ]
        with mock.patch('shutil.copy') as mock_copy:
            cli_copy.main()
        self.assertEqual(
            [call(dst='/dst_folder/', src='/tmp/testing_file.test'),
             call(dst='/dst_folder/', src='/new_loc_add')],
            mock_copy.call_args_list)

    @mock.patch('os.path.isfile', return_value=True)
    def test_correct_place_of_symlink_after_creation_of_it(self, mock_isfile):
        """ symlink must be placed at the address that is specified from the input arguments.
        This test for the cases that we have one more addition local file address in the database
        in the case of data downloaded once again for a second time in a different address. """
        sys.argv = [
            "",
            '-b', "2018-06-01",
            '-e', "2018-06-09",
            '-l',
            '-d', "/dst_folder/"
        ]
        with mock.patch('os.symlink') as mock_symlink:
            cli_copy.main()
        self.assertEqual(
            [call(dst='/dst_folder/testing_file.test', src='/tmp/testing_file.test'),
             call(dst='/dst_folder/new_loc_add', src='/new_loc_add')],
            mock_symlink.call_args_list)

    @mock.patch('os.symlink')
    @mock.patch('os.path.isfile', return_value=True)
    def test_correct_content_of_flag_file(self, mock_isfile, mock_link):
        """ flag file should contain this 'type: test_type' information """
        sys.argv = [
            "",
            '-b', "2018-04-01",
            '-e', "2018-04-09",
            '-f', '-l',
            '-t', 'test_type'
        ]
        temp_directory = tempfile.TemporaryDirectory()
        sys.argv.append('-d')
        sys.argv.append(temp_directory.name)
        cli_copy.main()
        with open(os.path.join(temp_directory.name + '/testing_file.test.flag'), 'r') as fd:
            self.assertEqual(fd.read(), (
                "type: test_type\nurl: https://scihub.copernicus.eu/apihub/odata/v1/Produ"
                "cts('6127111d-c9bd-4689-bab5-412dd39e1e81')/$value\nurl: https://scihu"
                "b.copernicus.eu/the_second_fakeurl\n"
            )
        )

    def test_copying_accomplishment_and_consistency_for_symlink_of_file_copying(self):
        """ A symlink of actual sample file should be copied to the destination folder """
        sys.argv = [
            "",
            '-b', "2018-04-01",
            '-e', "2018-04-09",
            '-f', '-l'
        ]
        temp_directory_dst = tempfile.TemporaryDirectory()
        sys.argv.append('-d')
        sys.argv.append(temp_directory_dst.name)
        Path("/tmp/testing_file.test").touch()
        cli_copy.main()
        self.assertTrue(os.path.islink(temp_directory_dst.name+'/testing_file.test'))
        cli_copy.main()  # assertion of completion of copy for the second time without error
        self.assertTrue(os.path.islink(temp_directory_dst.name+'/testing_file.test'))
        os.remove("/tmp/testing_file.test")

    def test_copying_accomplishment_and_consistency_for_actual_file_copying(self):
        """ The actual sample file should be copied to the destination folder  """
        sys.argv = [
            "",
            '-b', "2018-04-01",
            '-e', "2018-04-09"
        ]
        temp_directory_dst = tempfile.TemporaryDirectory()
        sys.argv.append('-d')
        sys.argv.append(temp_directory_dst.name)
        Path("/tmp/testing_file.test").touch()
        cli_copy.main()
        self.assertTrue(os.path.isfile(temp_directory_dst.name+'/testing_file.test'))
        cli_copy.main()  # assertion of completion of copy for the second time without error
        self.assertTrue(os.path.isfile(temp_directory_dst.name+'/testing_file.test'))
        os.remove("/tmp/testing_file.test")
