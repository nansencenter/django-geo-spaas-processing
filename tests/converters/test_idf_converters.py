"""Unit tests for IDF converters"""
import os
import os.path
import subprocess
import sys
import tarfile
import tempfile
import unittest
import unittest.mock as mock
from pathlib import Path

import geospaas_processing
# avoid downloading auxiliary files when testing
os.makedirs(os.path.join(os.path.dirname(geospaas_processing.__file__), 'auxiliary'), exist_ok=True)
import geospaas_processing.converters.base as converters_base
import geospaas_processing.converters.idf.converter as idf_converter


class IDFConversionManagerTestCase(unittest.TestCase):
    """Tests for the IDFConversionManager class"""

    def test_do_not_download_auxiliary_files_if_folder_present(self):
        """Test that auxiliary files are not downloaded if the folder
        is present
        """
        with mock.patch('os.path.isdir', return_value=True), \
                mock.patch('ftplib.FTP') as mock_ftp, \
                mock.patch('geospaas_processing.converters.idf.converter.IDFConversionManager'
                           '.make_symlink') as mock_make_symlink:
            idf_converter.IDFConversionManager.download_auxiliary_files('/foo')
        mock_ftp.assert_not_called()
        mock_make_symlink.assert_called()

    def test_download_auxiliary_files_if_folder_not_present(self):
        """Test that auxiliary files are downloaded if the folder is
        not present
        """
        with mock.patch('os.path.isdir', return_value=False), \
                mock.patch('os.makedirs'), \
                mock.patch('ftplib.FTP') as mock_ftp, \
                mock.patch('tempfile.TemporaryFile'), \
                mock.patch('geospaas_processing.converters.idf.converter.IDFConversionManager'
                           '.make_symlink') as mock_make_symlink:
            idf_converter.IDFConversionManager.download_auxiliary_files('/foo')
        mock_ftp.assert_called()
        mock_ftp.return_value.retrbinary.assert_called()
        mock_make_symlink.assert_called_with(
            '/foo',
            os.path.join(os.path.dirname(idf_converter.__file__), 'auxiliary'))

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
                idf_converter.IDFConversionManager.download_auxiliary_files('/foo')
            mock_rmtree.assert_called_with('/foo')

    def test_make_symlink(self):
        """Test making a symbolic link if none exists"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            source = tmp_path / 'foo'
            dest = tmp_path / 'bar'
            source.mkdir()
            idf_converter.IDFConversionManager.make_symlink(source, dest)
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
            idf_converter.IDFConversionManager.make_symlink(source, dest)
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
            idf_converter.IDFConversionManager.make_symlink(source, dest)
            self.assertTrue(dest.is_symlink())
            self.assertEqual(dest.resolve(), source)


    def test_no_download_arg(self):
        """Test that the download does not happen if
        `download_auxiliary` is False
        """
        sys_modules = sys.modules.copy()
        del sys_modules['unittest']
        with mock.patch('sys.modules', sys_modules), \
             mock.patch('geospaas_processing.converters.idf.converter'
                        '.IDFConversionManager.download_auxiliary_files') as mock_download:
            idf_converter.IDFConversionManager('', download_auxiliary=True)
        mock_download.assert_called()

        with mock.patch('geospaas_processing.converters.idf.converter'
                        '.IDFConversionManager.download_auxiliary_files') as mock_download:
            idf_converter.IDFConversionManager('', download_auxiliary=False)
        mock_download.assert_not_called()


class IDFConverterTestCase(unittest.TestCase):
    """Tests for the base IDFConverter class"""

    fixtures = [Path(__file__).parent.parent / 'data/test_data.json']

    def setUp(self):
        mock.patch(
            'geospaas_processing.converters.idf.converter.IDFConverter.PARAMETERS_DIR',
            str(Path(__file__).parent / 'data')
        ).start()
        self.addCleanup(mock.patch.stopall)

        self.converter = idf_converter.IDFConverter(['parameters_file'])

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
                cwd=str(Path(idf_converter.__file__).parent),
                check=True, capture_output=True
            )
            mock_move_results.assert_called_once_with('tmp_dir', 'bar')

    def test_run_subprocess_error(self):
        """Test error handling when the subprocess ends in error"""
        with mock.patch('subprocess.run') as run_mock:
            run_mock.side_effect = subprocess.CalledProcessError(1, '')
            with self.assertRaises(converters_base.ConversionError):
                self.converter.run('foo', 'bar')

    def test_run_skip_file(self):
        """If a file is skipped, a ConversionError should be raised"""
        with mock.patch('subprocess.run') as run_mock:
            run_mock.return_value.stderr = 'Some message. Skipping this file.'
            with self.assertRaises(converters_base.ConversionError):
                self.converter.run('foo', 'bar')


class MultiFilesIDFConverterTestCase(unittest.TestCase):
    """Tests for the MultiFilesIDFConverter class"""

    def setUp(self):
        self.converter = idf_converter.MultiFilesIDFConverter([])

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
             mock.patch(
                'geospaas_processing.converters.idf.converter.IDFConverter.run') as mock_run:
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
            with self.assertRaises(converters_base.ConversionError):
                self.converter.run('', '')


class Sentinel1IDFConverterTestCase(unittest.TestCase):
    """Tests for the Sentinel1IDFConverter class"""

    def setUp(self):
        self.converter = idf_converter.Sentinel1IDFConverter([])

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
            with self.assertRaises(converters_base.ConversionError):
                self.converter.list_files_to_convert('')

        with mock.patch('os.listdir', side_effect=NotADirectoryError):
            with self.assertRaises(converters_base.ConversionError):
                self.converter.list_files_to_convert('')


class Sentinel2IDFConverterTestCase(unittest.TestCase):
    """Tests for the Sentinel2IDFConverter class"""

    def setUp(self):
        self.converter = idf_converter.Sentinel2IDFConverter([])

    def test_list_files_to_convert(self):
        """list_files_to_convert() should return all the files
        contained in the "measurement" subdirectory of the dataset
        directory
        """
        contents = ['L1C_T33TVJ_A032566_20230601T095606']

        with mock.patch('os.listdir', return_value=contents):
            self.assertListEqual(
                ['dataset_dir/GRANULE/L1C_T33TVJ_A032566_20230601T095606'],
                self.converter.list_files_to_convert('dataset_dir')
            )

    def test_list_files_to_convert_error(self):
        """list_files_to_convert() should raise a ConversionError when
        the measurement directory is not present or is not a directory
        """
        with mock.patch('os.listdir', side_effect=FileNotFoundError):
            with self.assertRaises(converters_base.ConversionError):
                self.converter.list_files_to_convert('')

        with mock.patch('os.listdir', side_effect=NotADirectoryError):
            with self.assertRaises(converters_base.ConversionError):
                self.converter.list_files_to_convert('')


class Sentinel3SLSTRL2WSTIDFConverterTestCase(unittest.TestCase):
    """Tests for the Sentinel3SLSTRL2WSTIDFConverter class"""

    def setUp(self):
        self.converter = idf_converter.Sentinel3SLSTRL2WSTIDFConverter([])

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
            with self.assertRaises(converters_base.ConversionError):
                self.converter.list_files_to_convert('')

        with mock.patch('os.listdir', side_effect=NotADirectoryError):
            with self.assertRaises(converters_base.ConversionError):
                self.converter.list_files_to_convert('')
