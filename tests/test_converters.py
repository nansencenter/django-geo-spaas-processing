"""Unit tests for converters"""
import logging
import os
import os.path
import shutil
import tarfile
import unittest
import unittest.mock as mock
import subprocess
import tempfile
import zipfile
from pathlib import Path

import django.test

import geospaas_processing
# avoid downloading auxiliary files when testing
os.makedirs(os.path.join(os.path.dirname(geospaas_processing.__file__), 'auxiliary'), exist_ok=True)
import geospaas_processing.converters as converters


class AuxiliaryFilesDownloadTestCase(unittest.TestCase):
    """Test auxiliary files downloading"""

    def test_do_not_download_auxiliary_files_if_folder_present(self):
        """Test that auxiliary files are not downloaded if the folder
        is present
        """
        with mock.patch('os.path.isdir', return_value=True), \
                mock.patch('ftplib.FTP') as mock_ftp:
            converters.download_auxiliary_files('/foo')
        mock_ftp.assert_not_called()

    def test_download_auxiliary_files_if_folder_not_present(self):
        """Test that auxiliary files are downloaded if the folder is
        not present
        """
        with mock.patch('os.path.isdir', return_value=False), \
                mock.patch('os.makedirs'), \
                mock.patch('ftplib.FTP') as mock_ftp, \
                mock.patch('tempfile.TemporaryFile'):
            converters.download_auxiliary_files('/foo')
        mock_ftp.assert_called()
        mock_ftp.return_value.retrbinary.assert_called()

    def test_download_auxiliary_error(self):
        """Test that partly extracted auxiliary files are removed in
        case of error
        """
        with mock.patch('os.path.isdir', return_value=False), \
                mock.patch('os.makedirs'), \
                mock.patch('ftplib.FTP'), \
                mock.patch('tempfile.TemporaryFile'), \
                mock.patch('tarfile.TarFile', side_effect=tarfile.ExtractError), \
                mock.patch('shutil.rmtree') as mock_rmtree:
            with self.assertRaises(tarfile.ExtractError):
                converters.download_auxiliary_files('/foo')
            mock_rmtree.assert_called_with('/foo')


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
        with mock.patch('subprocess.run') as run_mock, \
                mock.patch('tempfile.TemporaryDirectory') as mock_tmp_dir, \
                mock.patch('geospaas_processing.converters.IDFConverter.move_results'):
            run_mock.side_effect = self.create_result_dir
            mock_tmp_dir.return_value.__enter__.return_value = 'tmp_dir'
            self.conversion_manager.convert(1, 'dataset_1.nc')
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    str(Path(converters.__file__).parent / 'parameters' / 'sentinel3_olci_l1_efr@'),
                    '-i', 'path', '=', str(self.test_file_path),
                    '-o', 'path', '=', 'tmp_dir'
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

        with mock.patch('subprocess.run') as run_mock, \
                mock.patch('tempfile.TemporaryDirectory') as mock_tmp_dir, \
                mock.patch('geospaas_processing.converters.IDFConverter.move_results'):
            mock_tmp_dir.return_value.__enter__.return_value = 'tmp_dir'
            run_mock.side_effect = self.create_result_dir
            self.conversion_manager.convert(1, 'dataset_1.zip')
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    str(Path(converters.__file__).parent / 'parameters' / 'sentinel3_olci_l1_efr@'),
                    '-i', 'path', '=', str(unzipped_path),
                    '-o', 'path', '=', 'tmp_dir'
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

    def test_run(self):
        """Test that the correct command is run and the resulting files
        are returned
        """
        expected_results = ['result1']
        with mock.patch('subprocess.run') as run_mock, \
             mock.patch('tempfile.TemporaryDirectory') as mock_tmp_dir, \
             mock.patch.object(self.converter, 'move_results') as mock_move_results:
            mock_tmp_dir.return_value.__enter__.return_value = 'tmp_dir'
            mock_move_results.return_value = expected_results
            self.assertListEqual(self.converter.run('foo', 'bar'), expected_results)
            run_mock.assert_called_with(
                [
                    'idf-converter',
                    self.converter.parameter_paths[0] + '@',
                    '-i', 'path', '=', 'foo',
                    '-o', 'path', '=', 'tmp_dir'
                ],
                cwd=str(Path(converters.__file__).parent),
                check=True, capture_output=True
            )
            mock_move_results.assert_called_once_with('tmp_dir', 'bar')

    def test_run_skip_file(self):
        """If a file is skipped, a ConversionError should be raised"""
        with mock.patch('subprocess.run') as run_mock:
            run_mock.return_value.stderr = 'Some message. Skipping this file.'
            with self.assertRaises(converters.ConversionError):
                self.converter.run('foo', 'bar')

    def test_move_results(self):
        """Test moving result files from the temporary directory to the
        result directory
        """
        with tempfile.TemporaryDirectory() as tmp_root:
            # create testing file structure
            tmp_results_dir = os.path.join(tmp_root, 'tmp_results')
            permanent_results_dir = os.path.join(tmp_root, 'results')
            os.makedirs(tmp_results_dir)
            os.makedirs(permanent_results_dir)

            # create existing files in collection2 in the permanent
            # result directory
            collection2_results_dir = os.path.join(permanent_results_dir, 'collection2')
            collection2_file_path = os.path.join(collection2_results_dir, 'file2')
            collection2_existing_dir_path = os.path.join(collection2_results_dir, 'file3')
            os.makedirs(collection2_results_dir)
            # create an existing file
            with open(collection2_file_path, 'w') as f_h:
                f_h.write('old')
            # create an existing directory
            os.makedirs(collection2_existing_dir_path)

            # create a "collection1" directory containing a file
            tmp_collection1_dir = os.path.join(tmp_results_dir, 'collection1')
            os.makedirs(tmp_collection1_dir)
            with open(os.path.join(tmp_collection1_dir, 'file1'), 'wb'):
                pass

            # create a "collection2" directory containing files
            tmp_collection2_dir = os.path.join(tmp_results_dir, 'collection2')
            os.makedirs(tmp_collection2_dir)
            # this file needs to contain something to check that the
            # existing file in the results folder gets replaced
            with open(os.path.join(tmp_collection2_dir, 'file2'), 'w') as f_h:
                f_h.write('new')
            # this file can be empty as it will replace a directory
            with open(os.path.join(tmp_collection2_dir, 'file3'), 'wb') as f_h:
                pass

            with self.assertLogs(converters.LOGGER, level=logging.INFO):
                self.converter.move_results(tmp_results_dir, permanent_results_dir)

            # check that the files are present in the permanent
            # results folder
            self.assertListEqual(os.listdir(permanent_results_dir), ['collection1', 'collection2'])
            self.assertCountEqual(
                os.listdir(os.path.join(permanent_results_dir, 'collection1')),
                ['file1'])
            self.assertCountEqual(
                os.listdir(os.path.join(permanent_results_dir, 'collection2')),
                ['file2', 'file3'])

            # check that the file in collection2 has been replaced
            with open(collection2_file_path, 'r') as f_h:
                self.assertEqual(f_h.read(), 'new')
            # check that the directory in collection2 has been replaced
            self.assertTrue(os.path.isfile(collection2_existing_dir_path))

            # check that the files are no longer present in the
            # temporary results folder
            self.assertFalse(os.listdir(tmp_collection1_dir))
            self.assertFalse(os.listdir(tmp_collection2_dir))

    def test_move_results_error(self):
        """Test that errors during the move other than the file being
        already present are raised
        """
        with mock.patch('os.listdir', side_effect=[['collection'], ['file']]), \
                mock.patch('os.makedirs'), \
                mock.patch('shutil.move', side_effect=shutil.Error('some error')):
            with self.assertRaises(shutil.Error):
                self.converter.move_results('foo', 'bar')

    def test_move_results_nothing_found(self):
        """move_results() should return an empty list
        when no result file is found
        """
        with mock.patch('os.listdir', return_value=[]):
            self.assertListEqual(self.converter.move_results('/tmp/dir', '/output/dir'), [])


class MultiFilesIDFConverterTestCase(unittest.TestCase):
    """Tests for the MultiFilesIDFConverter class"""

    def setUp(self):
        self.converter = converters.MultiFilesIDFConverter([])

    def test_abstract_list_files_to_convert(self):
        """list_files_to_convert() should raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.converter.list_files_to_convert('')

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
