"""Unit tests for converters"""
import os.path
import unittest
import unittest.mock as mock
import subprocess
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import django.test

import geospaas_processing.converters as converters


class IDFConversionManagerTestCase(django.test.TestCase):
    """Tests for the IDFConversionManager class"""

    fixtures = [Path(__file__).parent / 'data/test_data.json']

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_directory.name)

        # Make an empty test file
        self.test_file_path = self.temp_dir_path / 'dataset_1.nc'
        self.test_file_path.touch()

        self.conversion_manager = converters.IDFConversionManager(self.temp_directory.name)

    @converters.IDFConversionManager.register()
    class TestIDFConverter(converters.IDFConverter):
        """Converter used to test the IDFConversionManager's behavior
        """

    def test_register(self):
        """The register() decorator should add the decorated class to
        the converters dict of the IDFConversionManager
        """
        self.assertIn(self.TestIDFConverter, converters.IDFConversionManager.converters)

    def test_get_parameter_file(self):
        """get_parameter_file() should return the parameter file whose
        associated prefix the dataset starts with
        """
        PARAMETER_FILES = (
            (('parameters_file_1',), lambda d: d.entry_id.startswith(('prefix_1', 'prefix_2'))),
            (('parameters_file_2',), lambda d: d.entry_id.startswith('prefix_3'))
        )

        dataset = mock.Mock()
        dataset.entry_id = 'prefix_2_dataset'

        self.assertEqual(
            self.conversion_manager.get_parameter_files(PARAMETER_FILES, dataset),
            ('parameters_file_1',)
        )

    def test_get_parameter_file_none_if_not_found(self):
        """get_parameter_file() should return None
        if no parameter file is found
        """
        PARAMETER_FILES = (
            (('parameters_file_1',), lambda d: False),
        )

        dataset = mock.Mock()
        dataset.entry_id = 'prefix_5_dataset'

        self.assertIsNone(self.conversion_manager.get_parameter_files(PARAMETER_FILES, dataset))

    def test_get_converter(self):
        """get_converter() should return the first converter in the
        converters list which can provide a parameter file for the
        dataset
        """
        converter = converters.IDFConversionManager.get_converter(1)
        self.assertIsInstance(converter, converters.Sentinel3IDFConverter)
        self.assertEqual(
            converter.parameter_paths,
            [os.path.join(os.path.dirname(converters.__file__),
                          'parameters', 'sentinel3_olci_l1_efr')]
        )
        self.assertEqual(converter.collections, ['sentinel3_olci_l1_efr'])

    def test_get_converter_error(self):
        """get_converter() should raise a ConversionError if no
        converter is found
        """
        with mock.patch('re.match', return_value=None):
            with self.assertRaises(converters.ConversionError):
                converters.IDFConversionManager.get_converter(1)

    def create_result_dir(self, *args, **kwargs):  # pylint: disable=unused-argument
        """Creates a dummy result file"""
        result_dir = self.temp_dir_path / 'sentinel3_olci_l1_efr' / 'dataset_1.nc'
        result_dir.mkdir(parents=True)
        return mock.Mock()

    def test_convert(self):
        """Test a simple conversion"""
        with mock.patch('subprocess.run') as run_mock:
            run_mock.side_effect = self.create_result_dir
            self.conversion_manager.convert(1, 'dataset_1.nc')
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    str(Path(converters.__file__).parent / 'parameters' / 'sentinel3_olci_l1_efr@'),
                    '-i', 'path', '=', str(self.test_file_path),
                    '-o', 'path', '=', str(self.temp_dir_path)
                ],
                cwd=str(Path(converters.__file__).parent),
                check=True, capture_output=True
            )

    def test_convert_zip(self):
        """Test a conversion of a dataset contained in a zip file"""
        # Make a test zip file
        test_file_path = self.temp_dir_path / 'dataset_1.nc'
        test_file_path.touch()
        with zipfile.ZipFile(self.temp_dir_path / 'dataset_1.zip', 'w') as zip_file:
            zip_file.write(test_file_path, test_file_path.name)
        test_file_path.unlink()

        unzipped_path = self.temp_dir_path / 'dataset_1' / 'dataset_1.nc'

        with mock.patch('subprocess.run') as run_mock:
            run_mock.side_effect = self.create_result_dir
            self.conversion_manager.convert(1, 'dataset_1.zip')
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    str(Path(converters.__file__).parent / 'parameters' / 'sentinel3_olci_l1_efr@'),
                    '-i', 'path', '=', str(unzipped_path),
                    '-o', 'path', '=', str(self.temp_dir_path)
                ],
                cwd=str(Path(converters.__file__).parent),
                check=True, capture_output=True
            )
        # Check that temporary files have been cleaned up
        self.assertFalse((self.temp_dir_path / 'dataset_1').exists())

    def test_convert_error(self):
        """convert() must raise an exception if an error occurs when
        running the conversion command
        """
        with mock.patch('subprocess.run') as run_mock:
            run_mock.side_effect = subprocess.CalledProcessError(1, '')
            with self.assertRaises(converters.ConversionError):
                self.conversion_manager.convert(1, 'dataset_1.nc')

    def test_registered_converters(self):
        """Test that registered converters contain the right
        information
        """
        for converter_class, parameters_info in converters.IDFConversionManager.converters.items():
            self.assertTrue(
                issubclass(converter_class, converters.IDFConverter),
                f"{converter_class} is not a subclass of IDFConverter")
            for parameter_files, matching_function in parameters_info:
                self.assertIsInstance(
                    parameter_files, tuple,
                    f"In {converter_class}, {parameter_files} should be a tuple")
                self.assertTrue(
                    callable(matching_function),
                    f"In {converter_class}, {matching_function} should be a function")


class IDFConverterTestCase(unittest.TestCase):
    """Tests for the base IDFConverter class"""

    fixtures = [Path(__file__).parent / 'data/test_data.json']

    def setUp(self):
        mock.patch(
            'geospaas_processing.converters.IDFConverter.PARAMETERS_DIR',
            str(Path(__file__).parent / 'data')
        ).start()
        self.addCleanup(mock.patch.stopall)

        self.converter = converters.IDFConverter(['parameters_file'])

    def test_init(self):
        """Test the correct instantiation of an IDFConverter object"""
        self.assertListEqual(
            self.converter.parameter_paths,
            [str(Path(__file__).parent / 'data' / 'parameters_file')]
        )
        self.assertListEqual(self.converter.collections, ['some_collection'])

    def test_run(self):
        """Test that the correct command is run and the resulting files
        are returned
        """
        expected_results = ['result1']
        with mock.patch('subprocess.run') as run_mock, \
             mock.patch.object(self.converter, 'get_results') as mock_get_results:
            mock_get_results.return_value = expected_results
            self.assertListEqual(self.converter.run('foo', 'bar'), expected_results)
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    self.converter.parameter_paths[0] + '@',
                    '-i', 'path', '=', 'foo',
                    '-o', 'path', '=', 'bar'
                ],
                cwd=str(Path(converters.__file__).parent),
                check=True, capture_output=True
            )
            mock_get_results.assert_called_once_with('foo', 'bar')

    def test_run_skip_file(self):
        """If a file is skipped, a ConversionError should be raised"""
        with mock.patch('subprocess.run') as run_mock:
            run_mock.return_value.stderr = 'Some message. Skipping this file.'
            with self.assertRaises(converters.ConversionError):
                self.converter.run('foo', 'bar')

    def test_extract_parameter_value(self):
        """Extract the right value from an idf-converter parameter file"""
        self.assertEqual(
            'some_collection',
            self.converter.extract_parameter_value(
                self.converter.parameter_paths[0],
                converters.ParameterType.OUTPUT,
                'collection'
            )
        )

    def test_get_results(self):
        """get_results() should return the file in the collection
        folders for which matches_result() returns True
        """
        collection_folders_contents = [['dir1'], ['dir2', 'dir3']]
        with mock.patch.object(self.converter, 'collections', ['collection1', 'collection2']), \
             mock.patch.object(self.converter, 'matches_result', side_effect=[True, False, True]), \
             mock.patch('os.listdir', side_effect=collection_folders_contents):
            self.assertListEqual(
                self.converter.get_results('', 'dataset2'),
                ['collection1/dir1', 'collection2/dir3']
            )

    def test_get_results_nothing_found(self):
        """get_results() should return an empty list
        when no result file is found
        """
        with mock.patch.object(self.converter, 'collections', ['collection1']), \
                mock.patch.object(self.converter, 'matches_result', return_value=False), \
                mock.patch('os.listdir', return_value='dir'):
            self.assertListEqual(self.converter.get_results('', 'dataset2'), [])

    def test_abstract_matches_result(self):
        """matches_result() should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.converter.matches_result('', '', '')


class MultiFilesIDFConverterTestCase(unittest.TestCase):
    """Tests for the MultiFilesIDFConverter class"""

    def setUp(self):
        self.converter = converters.MultiFilesIDFConverter([])

    def test_abstract_list_files_to_convert(self):
        """list_files_to_convert() should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.converter.list_files_to_convert('')

    def test_abstract_matches_result(self):
        """matches_result() should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.converter.matches_result('', '', '')

    def test_run(self):
        """MultiFilesIDFConverter.run() should call the parent's class
        run() method on all datasets returned by
        MultiFilesIDFConverter.list_files_to_convert()
        """
        subdatasets_names = ('dataset1.nc', 'dataset2.nc')
        subdatasets_paths = [
            os.path.join('foo', 'measurement', name)
            for name in subdatasets_names
        ]
        with mock.patch.object(self.converter, 'list_files_to_convert') as mock_list, \
             mock.patch('geospaas_processing.converters.IDFConverter.run') as mock_run:
            mock_list.return_value = subdatasets_paths
            mock_run.side_effect = lambda i, o: [os.path.join(o, os.path.basename(i))]

            results = self.converter.run('foo', 'bar')

            calls = [
                mock.call(subdataset_path, 'bar')
                for subdataset_path in subdatasets_paths
            ]
            mock_run.assert_has_calls(calls)

            self.assertListEqual(
                results,
                [os.path.join('bar', d) for d in subdatasets_names]
            )

    def test_run_no_files_to_convert(self):
        """run() should raise a ConversionError when no dataset files
        to convert are found
        """
        with mock.patch.object(self.converter, 'list_files_to_convert', return_value=[]):
            with self.assertRaises(converters.ConversionError):
                self.converter.run('', '')


class Sentinel1IDFConverterTestCase(unittest.TestCase):
    """Tests for the Sentinel1IDFConverter class"""

    def setUp(self):
        self.converter = converters.Sentinel1IDFConverter([])

    def test_list_files_to_convert(self):
        """list_files_to_convert() should return all the files
        contained in the "measurement" subdirectory of the dataset
        directory
        """
        contents = ['foo.nc', 'bar.nc']

        with mock.patch('os.listdir', return_value=contents):
            self.assertListEqual(
                ['dataset_dir/measurement/foo.nc', 'dataset_dir/measurement/bar.nc'],
                self.converter.list_files_to_convert('dataset_dir')
            )

    def test_list_files_to_convert_error(self):
        """list_files_to_convert() should raise a ConversionError when
        the measurement directory is not present or is not a directory
        """
        with mock.patch('os.listdir', side_effect=FileNotFoundError):
            with self.assertRaises(converters.ConversionError):
                self.converter.list_files_to_convert('')

        with mock.patch('os.listdir', side_effect=NotADirectoryError):
            with self.assertRaises(converters.ConversionError):
                self.converter.list_files_to_convert('')

    def test_matches_result(self):
        """matches_result() should return True if the result directory
        name contains the dataset's identifier
        """
        self.assertTrue(self.converter.matches_result(
            '',
            os.path.join(
                'foo', 's1a-iw-ocn-vv-20210302t052855-20210302t052920-036815-045422-001.nc'),
            's1a-iw-ocn-vv-20210302t052855-20210302t052920-036815-045422-001.nc_2'
        ))

        self.assertTrue(self.converter.matches_result(
            '',
            os.path.join(
                'foo', 's1a-iw-ocn-vv-20200617t172631-20200617t172650-033060-03D466-001.nc'),
            's1a-iw-ocn-vv-20200617t172631-20200617t172650-033060-03D466-001.nc_0'
        ))

        self.assertFalse(self.converter.matches_result(
            '',
            os.path.join(
                'foo', 's1a-iw-ocn-vv-20210302t052855-20210302t052920-036815-045422-001.nc'),
            's1a-iw-ocn-vv-20200302t052855-20210302t052920-036815-045422-001.nc_2'
        ))

        self.assertFalse(self.converter.matches_result(
            '',
            os.path.join(
                'foo', 's1a-iw-ocn-vv-20210302t052855-20210302t052920-036815-045422-002.nc'),
            's1a-iw-ocn-vv-20210302t052855-20210302t052920-036815-045422-001.nc_2'
        ))


class Sentinel3SLSTRL2WSTIDFConverterTestCase(unittest.TestCase):
    """Tests for the Sentinel3SLSTRL2WSTIDFConverter class"""

    def setUp(self):
        self.converter = converters.Sentinel3SLSTRL2WSTIDFConverter([])

    def test_list_files_to_convert(self):
        """list_files_to_convert() should return all the files
        contained in the dataset directory
        """
        contents = ['foo.nc', 'bar.nc', 'baz.xml']
        with mock.patch('os.listdir', return_value=contents):
            self.assertListEqual(
                ['dataset_dir/foo.nc', 'dataset_dir/bar.nc'],
                self.converter.list_files_to_convert('dataset_dir')
            )

    def test_list_files_to_convert_error(self):
        """list_files_to_convert() should raise a ConversionError when
        the dataset directory is not present or is not a directory
        """
        with mock.patch('os.listdir', side_effect=FileNotFoundError):
            with self.assertRaises(converters.ConversionError):
                self.converter.list_files_to_convert('')

        with mock.patch('os.listdir', side_effect=NotADirectoryError):
            with self.assertRaises(converters.ConversionError):
                self.converter.list_files_to_convert('')

    def test_matches_result(self):
        """matches_result() should return True if the result directory
        name is equal to dataset file name minus the extension
        """
        self.assertTrue(self.converter.matches_result('', os.path.join('foo', 'bar.nc'), 'bar'))
        self.assertFalse(self.converter.matches_result('', os.path.join('foo', 'bar.nc'), 'baz'))
        self.assertFalse(self.converter.matches_result('', os.path.join('foo', 'bar.nc'), 'bar.nc'))


class Sentinel3IDFConverterTestCase(unittest.TestCase):
    """Tests for the base Sentinel3IDFConverter class"""

    def test_matches_result(self):
        """matches_result() should return True if the result directory
        name is equal to dataset file name
        """
        converter = converters.Sentinel3IDFConverter([])
        self.assertTrue(converter.matches_result('', os.path.join('foo', 'bar'), 'bar'))
        self.assertFalse(converter.matches_result('', os.path.join('foo', 'foo'), 'bar'))


class SingleResultIDFConverterTestCase(unittest.TestCase):
    """Tests for the SingleResultIDFConverter class"""

    def test_matches_result(self):
        """matches_result() should return True if the result directory
        name is equal to dataset file name minus the extension
        """
        converter = converters.SingleResultIDFConverter([])
        self.assertTrue(converter.matches_result('', os.path.join('foo', 'bar.nc'), 'bar'))
        self.assertFalse(converter.matches_result('', os.path.join('foo', 'bar.nc'), 'baz'))
        self.assertFalse(converter.matches_result('', os.path.join('foo', 'bar.nc'), 'bar.nc'))


class CMEMSMultiResultIDFConverterTestCase(unittest.TestCase):
    """Tests for the CMEMSMultiResultIDFConverter class"""

    def test_extract_date(self):
        """Test the extraction of the date from a file name"""
        converter = converters.CMEMSMultiResultIDFConverter([])
        self.assertEqual(
            converter.extract_date(
                'prefix_20201130_suffix.nc',
                r'^prefix_(?P<date>20201130)_suffix.nc$',
                '%Y%m%d'
            ),
            datetime(2020, 11, 30)
        )

    def test_extract_date_error(self):
        """extract_date() should raise a ConversionError if the date is
        not found
        """
        converter = converters.CMEMSMultiResultIDFConverter([])

        # no date group
        with self.assertRaises(converters.ConversionError):
            self.assertEqual(
                converter.extract_date(
                    'prefix_20201130_suffix.nc',
                    r'^prefix_(?P<dat>20201130)_suffix.nc$',
                    '%Y%m%d'
                ),
                datetime(2020, 11, 30)
            )

        # the regex does not match
        with self.assertRaises(converters.ConversionError):
            self.assertEqual(
                converter.extract_date(
                    'prefix_20201130_suffix.nc',
                    r'^prefi_(?P<date>20201130)_suffix.nc$',
                    '%Y%m%d'
                ),
                datetime(2020, 11, 30)
            )

        #wrong parse pattern
        with self.assertRaises(converters.ConversionError):
            self.assertEqual(
                converter.extract_date(
                    'prefix_20201130_suffix.nc',
                    r'^prefix_(?P<date>20201130)_suffix.nc$',
                    '%d%Y%m'
                ),
                datetime(2020, 11, 30)
            )

    def test_matches_result(self):
        """matches_result() should return True if the timestamp of the
        file is within the time range of the dataset
        """
        converter = converters.CMEMSMultiResultIDFConverter([])

        # timestamp == lower time range limit
        self.assertTrue(converter.matches_result(
            'cmems_001_024_hourly_mean_surface',
            'mercatorpsy4v3r1_gl12_hrly_20190430_R20190508.nc',
            'cmems_001_024_hourly_mean_surface_20190430000000_L4_v1.0_fv1.0'
        ))

        # upper time range limit > timestamp > lower time range limit
        self.assertTrue(converter.matches_result(
            'cmems_001_024_hourly_mean_surface',
            'mercatorpsy4v3r1_gl12_hrly_20190430_R20190508.nc',
            'cmems_001_024_hourly_mean_surface_20190430013000_L4_v1.0_fv1.0'
        ))

        # upper time range limit == timestamp
        self.assertFalse(converter.matches_result(
            'cmems_001_024_hourly_mean_surface',
            'mercatorpsy4v3r1_gl12_hrly_20190430_R20190508.nc',
            'cmems_001_024_hourly_mean_surface_20190501000000_L4_v1.0_fv1.0'
        ))

        # upper time range limit > timestamp
        self.assertFalse(converter.matches_result(
            'cmems_001_024_hourly_mean_surface',
            'mercatorpsy4v3r1_gl12_hrly_20190430_R20190508.nc',
            'cmems_001_024_hourly_mean_surface_20200501000000_L4_v1.0_fv1.0'
        ))
