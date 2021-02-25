"""Unit tests for converters"""
import unittest
import unittest.mock as mock
import subprocess
import tempfile
import zipfile
from pathlib import Path

import django.test

import geospaas_processing.converters as converters


class IDFConverterTestCase(unittest.TestCase):
    """Tests for the IDF converter"""

    fixtures = [Path(__file__).parent / 'data/test_data.json']

    def setUp(self):
        mock.patch(
            'geospaas_processing.converters.IDFConverter.PARAMETERS_DIR',
            str(Path(__file__).parent / 'data')
        ).start()
        self.addCleanup(mock.patch.stopall)

        self.converter = converters.IDFConverter('parameters_file')

    def test_init(self):
        """Test the correct instantiation of an IDFConverter object"""
        self.assertEqual(
            self.converter.parameter_path,
            str(Path(__file__).parent / 'data' / 'parameters_file')
        )
        self.assertEqual(self.converter.collection, 'some_collection')

    def test_abstract_get_parameter_file(self):
        """get_parameter_file() should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.converter.get_parameter_file(None)

    def test_run(self):
        """Test that the correct command is run"""
        with mock.patch('subprocess.run') as run_mock:
            self.converter.run('foo', 'bar')
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    self.converter.parameter_path + '@',
                    '-i', 'path', '=', 'foo',
                    '-o', 'path', '=', 'bar'
                ],
                cwd=str(Path(converters.__file__).parent),
                check=True, capture_output=True
            )

    def test_extract_parameter_value(self):
        """Extract the right value from an idf-converter parameter file"""
        self.assertEqual(
            'some_collection',
            self.converter.extract_parameter_value(
                converters.ParameterType.OUTPUT,
                'collection'
            )
        )

    def test_get_results(self):
        """get_results() should return the file in the collection
        folder which bears the same name as the dataset file to convert
        """
        collection_folder_contents = ['dataset1', 'dataset2', 'dataset3']
        with mock.patch('os.listdir', return_value=collection_folder_contents):
            self.assertListEqual(
                self.converter.get_results('', 'dataset2'),
                ['some_collection/dataset2']
            )

    def test_get_results_nothing_found(self):
        """get_results() should return an empty list
        when no result file is found
        """
        collection_folder_contents = ['dataset1', 'dataset2', 'dataset3']
        with mock.patch('os.listdir', return_value=collection_folder_contents):
            self.assertListEqual(self.converter.get_results('', 'dataset4'), [])


class PrefixMatchingIDFConverterTestCase(unittest.TestCase):
    """Tests for the PrefixMatchingIDFConverter class"""

    class TestPrefixMatchingIDFConverter(converters.PrefixMatchingIDFConverter):
        PARAMETER_FILES = (
            ('parameters_file_1', ('prefix_1', 'prefix_2')),
            ('parameters_file_2', ('prefix_3',))
        )

    def test_get_parameter_file(self):
        """get_parameter_file() should return the parameter file whose
        associated prefix the dataset starts with
        """
        dataset = mock.Mock()
        dataset.entry_id = 'prefix_2_dataset'

        self.assertEqual(
            self.TestPrefixMatchingIDFConverter.get_parameter_file(dataset),
            'parameters_file_1'
        )

    def test_get_parameter_file_none_if_not_found(self):
        """get_parameter_file() should return None
        if no parameter file is found
        """
        dataset = mock.Mock()
        dataset.entry_id = 'prefix_5_dataset'

        self.assertIsNone(self.TestPrefixMatchingIDFConverter.get_parameter_file(dataset))


class CMEMS001024IDFConverterTestCase(unittest.TestCase):
    """Tests for the CMEMS001024IDFConverter class"""

    def test_get_results(self):
        """get_results() should return all files with a timestamp
        within the time range of the dataset
        """
        working_directory = '/foo/bar'
        collection = 'cmems_001_024_hourly_mean_surface'
        conversion_results = [
            'cmems_001_024_hourly_mean_surface_20190430003000_L4_v1.0_fv1.0',
            'cmems_001_024_hourly_mean_surface_20190430013000_L4_v1.0_fv1.0',
            'cmems_001_024_hourly_mean_surface_20190430023000_L4_v1.0_fv1.0',
            'cmems_001_024_hourly_mean_surface_20190430033000_L4_v1.0_fv1.0',
            'cmems_001_024_hourly_mean_surface_20190430043000_L4_v1.0_fv1.0',
            'cmems_001_024_hourly_mean_surface_20190430053000_L4_v1.0_fv1.0',
        ]
        non_result_files = [
            'cmems_001_024_hourly_mean_surface_20190330003000_L4_v1.0_fv1.0',
            'cmems_001_024_hourly_mean_surface_20200430063000_L4_v1.0_fv1.0',
        ]

        with mock.patch('geospaas_processing.converters.IDFConverter.extract_parameter_value',
                        return_value=collection):
            converter = converters.CMEMS001024IDFConverter('')

        with mock.patch('os.listdir', return_value=conversion_results + non_result_files):
            self.assertListEqual(
                converter.get_results(working_directory,
                                      'mercatorpsy4v3r1_gl12_hrly_20190430_R20190508.nc'),
                [str(Path(collection) / file_name) for file_name in conversion_results]
            )

        with mock.patch('os.listdir', return_value=non_result_files):
            self.assertListEqual(
                converter.get_results(working_directory,
                                      'mercatorpsy4v3r1_gl12_hrly_20190430_R20190508.nc'),
                []
            )


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

    def test_get_converter(self):
        """get_converter() should return the first converter in the
        CONVERTERS list which can provide a parameter file for the
        dataset
        """
        converter = converters.IDFConversionManager.get_converter(1)
        self.assertIsInstance(converter, converters.Sentinel3IDFConverter)
        self.assertEqual(
            converter.parameter_path,
            '/workspace/geospaas_processing/parameters/sentinel3_olci_l1_efr')
        self.assertEqual(converter.collection, 'sentinel3_olci_l1_efr')

    def test_get_converter_error(self):
        """get_converter() should raise a ConversionError
        if no converter is found
        """
        with mock.patch('geospaas_processing.converters.Dataset') as mock_dataset:
            mock_dataset.objects.get.return_value.entry_id.startswith.return_value = False
            with self.assertRaises(converters.ConversionError):
                converters.IDFConversionManager.get_converter(1)

    def create_result_dir(self, *args, **kwargs):  # pylint: disable=unused-argument
        """Creates a dummy result file"""
        result_dir = self.temp_dir_path / 'sentinel3_olci_l1_efr' / 'dataset_1.nc'
        result_dir.mkdir(parents=True)

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
