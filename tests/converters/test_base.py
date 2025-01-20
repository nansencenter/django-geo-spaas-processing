"""Tests for the base conversion classes"""
import logging
import os
import os.path
import shutil
import unittest
import unittest.mock as mock
import sys
import tarfile
import tempfile
from pathlib import Path

import django.test
from geospaas.catalog.models import Dataset

import geospaas_processing
# avoid downloading auxiliary files when testing
os.makedirs(os.path.join(os.path.dirname(geospaas_processing.__file__), 'auxiliary'), exist_ok=True)
import geospaas_processing.converters.base as converters_base


class TestConversionManager(converters_base.ConversionManager):
    """Conversion manager class used for tests"""

    auxiliary_version = '0.0.1'
    auxiliary_url = ('https://uri/{}.tar.gz')
    auxiliary_path = Path('/foo')


@TestConversionManager.register()
class TestConverter(converters_base.Converter):
    """Converter used to test the IDFConversionManager's behavior
    """
    PARAMETER_SELECTORS = (
        converters_base.ParameterSelector(
            matches=lambda d: d.entry_id.startswith(('prefix_1', 'prefix_2')),
            parameter_files=('parameters_file_1',)),
        converters_base.ParameterSelector(
            matches=lambda d: d.entry_id.startswith('S3A_OL_1_EFR'),
            parameter_files=('parameters_file_2',))
    )

    def __init__(self, parameter_files):
        self.parameter_files = parameter_files

    def __eq__(self, other):
        return self.parameter_files == other.parameter_files

    def run(self, in_file, out_dir, **kwargs):
        return (in_file, out_dir, kwargs)


class ConversionManagerTestCase(django.test.TestCase):
    """Tests for the IDFConversionManager class"""

    fixtures = [Path(__file__).parent.parent / 'data/test_data.json']

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_directory.name)

        # Make an empty test file
        self.test_file_path = self.temp_dir_path / 'dataset_1.nc'
        self.test_file_path.touch()

        self.conversion_manager = TestConversionManager(self.temp_directory.name,
                                                        download_auxiliary=False)

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_register(self):
        """The register() decorator should add the decorated class to
        the converters dict of the IDFConversionManager
        """
        self.assertIn(TestConverter, TestConversionManager.converters)

    def test_get_converter(self):
        """Test creating a converter of the right class with the right
        argumentstest_download_auxiliary_files_if_folder_not_present
        """
        dataset = mock.Mock()
        dataset.entry_id = 'prefix_2_dataset'

        self.assertEqual(
            self.conversion_manager.get_converter(dataset),
            TestConverter(parameter_files=('parameters_file_1',)))

    def test_get_converter_not_found(self):
        """get_converter() should raise an exception if no converter
        is found
        """
        dataset = mock.Mock()
        dataset.entry_id = 'prefix_5_dataset'

        with self.assertRaises(converters_base.ConversionError):
            self.conversion_manager.get_converter(dataset)

    def create_result_dir(self, *args, **kwargs):  # pylint: disable=unused-argument
        """Creates a dummy result file"""
        result_dir = self.temp_dir_path / 'dataset_1.nc'
        result_dir.mkdir(parents=True)
        return mock.Mock()

    def test_convert(self):
        """Test a simple conversion"""
        self.assertTupleEqual(
            self.conversion_manager.convert(1, 'dataset_1.nc'),
            (
                str(self.temp_dir_path / 'dataset_1.nc'),
                self.temp_directory.name,
                {'dataset': Dataset.objects.get(id=1)}
            )
        )


class AuxiliaryDownloadTestCase(unittest.TestCase):
    """Test the download of auxiliary files"""

    def test_do_not_download_auxiliary_files_if_folder_not_empty(self):
        """Test that auxiliary files are not downloaded if the folder
        is present
        """
        with mock.patch('pathlib.Path.is_dir', return_value=True), \
             mock.patch('pathlib.Path.iterdir', return_value=iter(['baz'])), \
                mock.patch('geospaas_processing.utils.http_request') as mock_http_request, \
                mock.patch.object(TestConversionManager, 'make_symlink') as mock_make_symlink:
            TestConversionManager.download_auxiliary_files()
        mock_http_request.assert_not_called()
        mock_make_symlink.assert_called()

    def test_download_auxiliary_files_if_folder_not_present(self):
        """Test that auxiliary files are downloaded if the folder is
        not present
        """
        with mock.patch('pathlib.Path.is_dir', return_value=False), \
                mock.patch('os.makedirs'), \
                mock.patch('geospaas_processing.utils.http_request') as mock_http_request, \
                mock.patch('tarfile.open') as mock_tarfile_open, \
                mock.patch('builtins.open') as mock_open, \
                mock.patch('pathlib.Path.iterdir', return_value=['/baz']), \
                mock.patch('pathlib.Path.rmdir') as mock_rmdir, \
                mock.patch('pathlib.Path.unlink') as mock_unlink, \
                mock.patch('shutil.move') as mock_move, \
                mock.patch.object(TestConversionManager, 'make_symlink') as mock_make_symlink:
            mock_chunk = mock.MagicMock()
            (mock_http_request.return_value.__enter__.return_value
             .iter_content.return_value) = iter([mock_chunk])
            TestConversionManager.download_auxiliary_files()

        mock_http_request.assert_called()
        mock_http_request.return_value.__enter__.return_value.iter_content.assert_called()
        mock_open.return_value.__enter__.return_value.write.assert_called_with(mock_chunk)
        mock_tarfile_open.assert_called()
        mock_rmdir.assert_called()
        mock_unlink.assert_called()
        mock_move.assert_called()
        mock_make_symlink.assert_called_with(
            Path('/foo'),
            Path(converters_base.__file__).parent / 'auxiliary')

    def test_download_auxiliary_files_if_folder_empty(self):
        """Test that auxiliary files are downloaded if the folder is
        not present
        """
        with mock.patch('pathlib.Path.is_dir', return_value=True), \
                mock.patch('os.makedirs'), \
                mock.patch('geospaas_processing.utils.http_request') as mock_http_request, \
                mock.patch('tarfile.open') as mock_tarfile_open, \
                mock.patch('builtins.open') as mock_open, \
                mock.patch('pathlib.Path.iterdir', side_effect=(iter([]), iter(['/baz']))), \
                mock.patch('pathlib.Path.rmdir') as mock_rmdir, \
                mock.patch('pathlib.Path.unlink') as mock_unlink, \
                mock.patch('shutil.move') as mock_move, \
                mock.patch.object(TestConversionManager, 'make_symlink') as mock_make_symlink:
            mock_chunk = mock.MagicMock()
            (mock_http_request.return_value.__enter__.return_value
             .iter_content.return_value) = iter([mock_chunk])
            TestConversionManager.download_auxiliary_files()

        mock_http_request.assert_called()
        mock_http_request.return_value.__enter__.return_value.iter_content.assert_called()
        mock_open.return_value.__enter__.return_value.write.assert_called_with(mock_chunk)
        mock_tarfile_open.assert_called()
        mock_rmdir.assert_called()
        mock_unlink.assert_called()
        mock_move.assert_called()
        mock_make_symlink.assert_called_with(
            Path('/foo'),
            Path(converters_base.__file__).parent / 'auxiliary')

    def test_download_auxiliary_error(self):
        """Test that partly extracted auxiliary files are removed in
        case of error
        """
        with mock.patch('os.path.isdir', return_value=False), \
                mock.patch('os.makedirs'), \
                mock.patch('geospaas_processing.utils.http_request'), \
                mock.patch('tarfile.open', side_effect=tarfile.ExtractError), \
                mock.patch('builtins.open'), \
                mock.patch('shutil.rmtree') as mock_rmtree:
            with self.assertRaises(tarfile.ExtractError):
                TestConversionManager.download_auxiliary_files()
            mock_rmtree.assert_called_with(Path('/foo'))

    def test_make_symlink(self):
        """Test making a symbolic link if none exists"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source = tmp_path / 'foo'
            dest = tmp_path / 'bar'
            source.mkdir()
            TestConversionManager.make_symlink(source, dest)
            self.assertTrue(dest.is_symlink())
            self.assertEqual(dest.resolve(), source)

    def test_make_symlink_replace_dir(self):
        """Test making a symbolic link if a directory exists at the
        destination
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source = tmp_path / 'foo'
            dest = tmp_path / 'bar'
            source.mkdir()
            dest.mkdir()
            TestConversionManager.make_symlink(source, dest)
            self.assertTrue(dest.is_symlink())
            self.assertEqual(dest.resolve(), source)

    def test_make_symlink_replace_file(self):
        """Test making a symbolic link if a file exists at the
        destination
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source = tmp_path / 'foo'
            dest = tmp_path / 'bar'
            source.mkdir()
            dest.touch()
            TestConversionManager.make_symlink(source, dest)
            self.assertTrue(dest.is_symlink())
            self.assertEqual(dest.resolve(), source)

    def test_no_download_arg(self):
        """Test that the download does not happen if
        `download_auxiliary` is False
        """
        sys_modules = sys.modules.copy()
        del sys_modules['unittest']
        with mock.patch('sys.modules', sys_modules), \
             mock.patch('geospaas_processing.converters.base'
                        '.ConversionManager.download_auxiliary_files') as mock_download:
            TestConversionManager('', download_auxiliary=True)
        mock_download.assert_called()

        with mock.patch('geospaas_processing.converters.base'
                        '.ConversionManager.download_auxiliary_files') as mock_download:
            TestConversionManager('', download_auxiliary=False)
        mock_download.assert_not_called()


class ConverterTestCase(unittest.TestCase):
    """Tests for the Converter class"""

    def setUp(self) -> None:
        self.converter = converters_base.Converter()

    def test_abstract_run(self):
        """run() is not implemented in the base Converter class
        """
        with self.assertRaises(NotImplementedError):
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

            with self.assertLogs(converters_base.logger, level=logging.INFO):
                self.converter.move_results(tmp_results_dir, permanent_results_dir)

            # check that the files are present in the permanent
            # results folder
            self.assertCountEqual(os.listdir(permanent_results_dir), ['collection1', 'collection2'])
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
