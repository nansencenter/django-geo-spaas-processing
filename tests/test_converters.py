"""Unit tests for converters"""
import unittest
import unittest.mock as mock
import subprocess
import tempfile
import zipfile
from pathlib import Path

import django.test

import geospaas_processing.converters as converters


class IDFConverterTestCase(django.test.TestCase):
    """Tests for the IDF converter"""

    fixtures = [Path(__file__).parent / 'data/test_data.json']

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_directory.name)
        self.converter = converters.IDFConverter(self.temp_directory.name)

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_run_converter(self):
        """Test that the correct command is run"""
        with mock.patch('subprocess.run') as run_mock:
            self.converter.run_converter('foo', 'bar', 'sentinel3_olci_l1_efr')
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    'sentinel3_olci_l1_efr@',
                    '-i', 'path', '=', 'foo',
                    '-o', 'path', '=', 'bar'
                ],
                cwd=str(Path(converters.__file__).parent),
                check=True, capture_output=True
            )

    def test_extract_parameter_value(self):
        """Extract the right value from an idf-converter parameter file"""
        parameters_dir_path = Path(converters.__file__).parent / 'parameters'
        self.assertEqual(
            'sentinel3_olci_l1_efr',
            converters.IDFConverter.extract_parameter_value(
                str(parameters_dir_path / 'sentinel3_olci_l1_efr'),
                converters.ParameterType.OUTPUT,
                'collection'
            )
        )

    def test_choose_parameter_file(self):
        """The correct parameter file must be chosen for each dataset"""
        parameters_dir = Path(converters.__file__).parent / 'parameters'
        self.assertEqual(
            self.converter.choose_parameter_file(1),
            str(parameters_dir / 'sentinel3_olci_l1_efr')
        )

    def test_no_parameter_file(self):
        """An error must be raised if no parameter file is available for the dataset"""
        with mock.patch.object(converters.IDFConverter, 'PARAMETERS_CONDITIONS') as pc_mock:
            pc_mock.return_value = {}
            with self.assertRaises(converters.ConversionError):
                print(self.converter.choose_parameter_file(1))

    def test_get_dataset_files(self):
        """Test that the dataset files are correctly listed"""
        matching_file_names = ['dataset_1', 'dataset_1_foo_bar.zip']
        non_matching_file_names = ['dataset_2', 'foo_bar_dataset_1']
        temp_directory_path = self.temp_dir_path

        # Create empty files in the temporary directory
        for file_name in matching_file_names + non_matching_file_names:
            (temp_directory_path / file_name).touch()

        dataset_files = self.converter.get_dataset_files(1)
        dataset_files.sort()
        self.assertListEqual(
            dataset_files,
            [str(temp_directory_path / file_name) for file_name in matching_file_names]
        )

    def create_result_dir(self, *args, **kwargs):  # pylint: disable=unused-argument
        """Creates a dummy result file"""
        result_dir = self.temp_dir_path / 'sentinel3_olci_l1_efr' / 'dataset_1.nc'
        result_dir.mkdir(parents=True)

    def test_convert(self):
        """Test a simple conversion"""
        # Make an empty test file
        test_file_path = self.temp_dir_path / 'dataset_1.nc'
        test_file_path.touch()

        with mock.patch('subprocess.run') as run_mock:
            run_mock.side_effect = self.create_result_dir
            result = self.converter.convert(1)
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    str(Path(converters.__file__).parent / 'parameters' / 'sentinel3_olci_l1_efr@'),
                    '-i', 'path', '=', str(test_file_path),
                    '-o', 'path', '=', str(self.temp_dir_path)
                ],
                cwd=str(Path(converters.__file__).parent),
                check=True, capture_output=True
            )
        self.assertEqual(result, 'sentinel3_olci_l1_efr/dataset_1.nc')

    def test_convert_explicit_file_name(self):
        """Test a simple conversion with an explicit file name"""
        # Make an empty test file
        test_file_path = self.temp_dir_path / 'some_file.nc'
        test_file_path.touch()

        with mock.patch('subprocess.run') as run_mock:
            run_mock.side_effect = self.create_result_dir
            self.converter.convert(1, 'some_file.nc')
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    str(Path(converters.__file__).parent / 'parameters' / 'sentinel3_olci_l1_efr@'),
                    '-i', 'path', '=', str(test_file_path),
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
            self.converter.convert(1)
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
        """
        convert() must raise an exception if an error occurs when running the conversion command
        """
        # Make an empty test file
        test_file_path = self.temp_dir_path / 'dataset_1.nc'
        test_file_path.touch()

        with mock.patch('subprocess.run') as run_mock:
            run_mock.side_effect = subprocess.CalledProcessError(1, '')
            with self.assertRaises(converters.ConversionError):
                self.converter.convert(1)
