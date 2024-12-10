"""Tests for the Syntool converters"""
import logging
import subprocess
import tempfile
import textwrap
import unittest
import unittest.mock as mock
from pathlib import Path

import geospaas_processing.converters.base as converters_base
import geospaas_processing.converters.syntool.converter as syntool_converter


class SyntoolConverterTestCase(unittest.TestCase):
    """Tests for the SyntoolConverter class"""

    def test_convert(self):
        """Test that the right command is called"""
        converter = syntool_converter.SyntoolConverter()
        with mock.patch('subprocess.run') as mock_run, \
             mock.patch('tempfile.TemporaryDirectory') as mock_tmp_dir, \
             mock.patch.object(converter, 'move_results', return_value='foo.tiff'):
            mock_tmp_dir.return_value.__enter__.return_value = '/tmp_dir'
            with self.assertLogs(syntool_converter.logger, level=logging.INFO):
                result = converter.convert('/foo.nc', '/bar', ['--baz'])
            mock_run.assert_called_once_with(
                ['syntool-converter', '--baz', '-i', '/foo.nc', '-o', '/tmp_dir'],
                cwd=Path(syntool_converter.__file__).parent,
                check=True,
                capture_output=True,
                env=None)
            self.assertEqual(result, 'foo.tiff')

    def test_convert_subprocess_error(self):
        """Test error handling when the sub process encounters an error"""
        converter = syntool_converter.SyntoolConverter()
        with mock.patch('subprocess.run', side_effect=subprocess.CalledProcessError(1, '')):
            with self.assertLogs(syntool_converter.logger, level=logging.INFO), \
                 self.assertRaises(converters_base.ConversionError):
                converter.convert('/foo.nc', '/bar', ['--baz'])

    def test_convert_move_results_error(self):
        """Test error handling when no result is produced
        """
        converter = syntool_converter.SyntoolConverter()
        with mock.patch('subprocess.run'), \
             mock.patch.object(converter, 'move_results', return_value=[]):
            with self.assertLogs(syntool_converter.logger, level=logging.WARNING):
                converter.convert('/foo.nc', '/bar', ['--baz'])

    def test_ingest(self):
        """Test that the right command is called"""
        converter = syntool_converter.SyntoolConverter()
        with mock.patch('subprocess.run') as mock_run, \
             mock.patch('tempfile.TemporaryDirectory') as mock_tmp_dir, \
             mock.patch.object(converter, 'move_results',
                               return_value=['3413_foo/1', '3413_foo/2']):
            mock_tmp_dir.return_value.__enter__.return_value = '/tmp_dir'
            with self.assertLogs(syntool_converter.logger, level=logging.INFO):
                result = converter.ingest('/bar/foo.tiff', '/bar', ['--baz'])
            mock_run.assert_called_once_with(
                ['syntool-ingestor', '--baz', '--output-dir', '/tmp_dir', '/bar/foo.tiff'],
                cwd=Path(syntool_converter.__file__).parent,
                check=True,
                capture_output=True,
                env=None)
            self.assertEqual(result, ['ingested/3413_foo/1', 'ingested/3413_foo/2'])

    def test_ingest_subprocess_error(self):
        """Test error handling when the sub process encounters an error"""
        converter = syntool_converter.SyntoolConverter()
        with mock.patch('subprocess.run', side_effect=subprocess.CalledProcessError(1, '')):
            with self.assertLogs(syntool_converter.logger, level=logging.WARNING):
                converter.ingest('/bar/foo.tiff', '/bar', ['--baz'])

    def test_ingest_move_results_error(self):
        """Test error handling when no result is produced
        """
        converter = syntool_converter.SyntoolConverter()
        with mock.patch('subprocess.run'), \
                mock.patch.object(converter, 'move_results', return_value=[]):
            with self.assertLogs(syntool_converter.logger, level=logging.WARNING):
                converter.ingest('/bar/foo.tiff', '/bar', ['--baz'])

    def test_post_ingest(self):
        """Test the default post-ingestion step"""
        mock_dataset = mock.Mock(entry_id='foo')
        mock_dataset.dataseturi_set.exclude.return_value.first.return_value = mock.Mock(
            uri='https://bar')
        with tempfile.TemporaryDirectory() as tmp_dir:
            result_dir = Path(tmp_dir, 'result')
            result_dir.mkdir()
            syntool_converter.SyntoolConverter().post_ingest(
                ['result'], tmp_dir, {'dataset': mock_dataset})
            with open(result_dir / 'features' / 'data_access.ini', 'r', encoding='utf-8') as handle:
                contents = handle.read()
            self.assertEqual(
                contents,
                textwrap.dedent("""\
                [metadata]
                syntool_id = data_access

                [geospaas]
                entry_id = foo
                dataset_url = https://bar

                """))

    def test_abstract_run(self):
        """SyntoolConverter.run() should not be implemented"""
        with self.assertRaises(NotImplementedError):
            syntool_converter.SyntoolConverter().run('', '')


class BasicSyntoolConverterTestCase(unittest.TestCase):
    """Tests for the BasicSyntoolConverter"""

    def test_find_ingest_config_str(self):
        """Test getting the ingester parameters file from a string"""
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        self.assertEqual(converter.find_ingest_config('baz'), ['bar'])

    def test_find_ingest_config_list(self):
        """Test getting the ingester parameters file from a list of
        selectors
        """
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files=[
                converters_base.ParameterSelector(
                    matches=lambda f: f.startswith('b'),
                    ingest_file='bar'),
                converters_base.ParameterSelector(
                    matches=lambda f: f.startswith('a'),
                    ingest_file='qux'),
            ])
        self.assertEqual(converter.find_ingest_config('baz'), ['bar'])

    def test_find_ingest_config_error(self):
        """An exception must be raised if no config is found"""
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files=[
                converters_base.ParameterSelector(
                    matches=lambda f: f.startswith('b'),
                    ingest_file='bar'),
            ])
        with self.assertRaises(converters_base.ConversionError):
            converter.find_ingest_config('foo')

    def test_find_ingest_config_type_error(self):
        """An exception must be raised if ingest_parameter_files is
        not a string or ParameterSelector or a list of these
        """
        with self.assertRaises(converters_base.ConversionError):
            syntool_converter.BasicSyntoolConverter(
                converter_type='foo',
                ingest_parameter_files=1).find_ingest_config('foo')
        with self.assertRaises(converters_base.ConversionError):
            syntool_converter.BasicSyntoolConverter(
                converter_type='foo',
                ingest_parameter_files=[1, 2]).find_ingest_config('foo')

    def test_parse_converter_options(self):
        """Test parsing and merging converter options"""
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            converter_options={'bar': 'baz'},
            ingest_parameter_files='qux')
        result = converter.parse_converter_options({
            'converter_options': {'quux': 'corge'}
        })
        self.assertListEqual(result, ['-opt', 'bar=baz', 'quux=corge'])

    def test_parse_converter_options_no_default(self):
        """Test parsing converter options when the converter does not
        have options defined
        """
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        result = converter.parse_converter_options({
            'converter_options': {'quux': 'corge'}
        })
        self.assertListEqual(result, ['-opt', 'quux=corge'])

    def test_parse_converter_options_no_kwarg(self):
        """Test parsing converter options when the key word arguments
        do not have options defined
        """
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            converter_options={'bar': 'baz'},
            ingest_parameter_files='qux')
        result = converter.parse_converter_options({})
        self.assertListEqual(result, ['-opt', 'bar=baz'])

    def test_parse_converter_options_not_dict(self):
        """Test parsing converter options when the converter_options
        kwarg is not a dictionary
        """
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            converter_options={'bar': 'baz'},
            ingest_parameter_files='qux')
        with self.assertLogs(level=logging.WARNING):
            result = converter.parse_converter_options({'converter_options': None})
        self.assertListEqual(result, ['-opt', 'bar=baz'])

    def test_parse_converter_args(self):
        """Test parsing converter arguments"""
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            converter_options={'bar': 'baz'},
            ingest_parameter_files='qux')
        self.assertListEqual(
            converter.parse_converter_args({'converter_options': {'ham': 'egg'}}),
            ['-t', 'foo', '-opt', 'bar=baz', 'ham=egg'])

    def test_run_conversion(self):
        """Test calling the Syntool converter on the input file"""
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with mock.patch.object(converter, 'convert',
                               return_value=['conv1.tiff', 'conv2']) as mock_convert:
            results = converter.run_conversion('in.nc', 'out',
                                               {'converter_options': {'baz': 'quz'}})
        mock_convert.assert_called_once_with('in.nc', 'out', ['-t', 'foo', '-opt', 'baz=quz'])
        self.assertListEqual(results, [Path('out', 'conv1.tiff'), Path('out', 'conv2')])

    def test_run_conversion_no_converter(self):
        """If no converter is set, run_conversion() should return the path to
        the input file
        """
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type=None,
            ingest_parameter_files='bar')
        with mock.patch.object(converter, 'convert',) as mock_convert:
            results = converter.run_conversion('in.nc', 'out', {})
        mock_convert.assert_not_called()
        self.assertListEqual(results, [Path('out', 'in.nc')])

    def test_run_ingestion(self):
        """Test calling the Syntool ingestor on the conversion output
        file(s)
        """
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with mock.patch.object(converter, 'ingest',
                               side_effect=[['ingested_dir1'], ['ingested_dir2']]) as mock_ingest, \
             mock.patch('os.remove', side_effect=(None, IsADirectoryError)) as mock_remove, \
             mock.patch('shutil.rmtree') as mock_rmtree:
            converter.run_ingestion(
                [Path('out', 'conv1.tiff'), Path('out', 'conv2')], 'results', {})
        mock_ingest.assert_has_calls([
            mock.call(
                Path('out', 'conv1.tiff'),
                'results',
                ['--config', Path('parameters/3413.ini'),
                 '--options-file', converter.PARAMETERS_DIR / 'bar']),
            mock.call(
                Path('out', 'conv2'),
                'results',
                ['--config', Path('parameters/3413.ini'),
                 '--options-file', converter.PARAMETERS_DIR / 'bar']),
        ])
        mock_remove.assert_has_calls((
            mock.call(Path('out', 'conv1.tiff')),
            mock.call(Path('out', 'conv2'))))
        mock_rmtree.assert_called_once_with(Path('out', 'conv2'))

    def test_run(self):
        """Test running the conversion and ingestion"""
        converter = syntool_converter.BasicSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with mock.patch.object(converter, 'run_conversion',
                               return_value=[Path('out', 'conv1.tiff'),
                                             Path('out', 'conv2')]) as mock_conversion, \
             mock.patch.object(converter, 'run_ingestion',
                               return_value=['ingested_dir1', 'ingested_dir2']) as mock_ingestion, \
             mock.patch.object(converter, 'post_ingest') as mock_post_ingest:
            results = converter.run(
                in_file='in.nc',
                out_dir='out',
                results_dir='results',
                converter_options={'baz': 'quz'})
        self.assertListEqual(results, ['ingested_dir1', 'ingested_dir2'])
        mock_conversion.assert_called_with('in.nc', 'out', {'converter_options': {'baz': 'quz'}})
        mock_ingestion.assert_called_with(
            [Path('out', 'conv1.tiff'), Path('out', 'conv2')],
            'results',
            {'converter_options': {'baz': 'quz'}})
        mock_post_ingest.assert_called_with(
            ['ingested_dir1', 'ingested_dir2'], 'results', {'converter_options': {'baz': 'quz'}})


class Sentinel1SyntoolConverterTestCase(unittest.TestCase):
    """Tests for the Sentinel1SyntoolConverter class"""

    def test_list_files(self):
        """Should return a list of paths representing the files in a
        directory
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            for file_name in ('foo', 'bar'):
                with open(Path(tmp_dir, file_name), 'wb'):
                    pass
            self.assertCountEqual(
                syntool_converter.Sentinel1SyntoolConverter.list_files(Path(tmp_dir)),
                [Path(tmp_dir, 'bar'), Path(tmp_dir, 'foo')])

    def test_list_files_not_found(self):
        """An exception must be raised if the directory does not
        contain any file
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaises(converters_base.ConversionError):
                syntool_converter.Sentinel1SyntoolConverter.list_files(Path(tmp_dir))

    def test_convert(self):
        """Test that the convert method is called on all the files in
        the measurement directory
        """
        converter = syntool_converter.Sentinel1SyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with tempfile.TemporaryDirectory() as tmp_dir:
            measurement_dir = Path(tmp_dir, 'measurement')
            measurement_dir.mkdir()
            file_names = ('file1.nc', 'file2.nc')
            for file_name in file_names:
                (measurement_dir / file_name).touch()
            with mock.patch(
                    'geospaas_processing.converters.syntool.converter.SyntoolConverter.convert'
                    ) as mock_convert:
                converter.convert(tmp_dir, 'out_dir', ['--baz'])
            for file_name in file_names:
                mock_convert.assert_any_call(measurement_dir / file_name, 'out_dir', ['--baz'])

    def test_ingest(self):
        """Test that the subdirectories created by ingestion are copied
        to the ingested results folder
        """
        converter = syntool_converter.Sentinel1SyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with tempfile.TemporaryDirectory() as out_dir:
            # prepare directory structure...
            # ...conversion results
            converted_dir = Path(out_dir, 'product', 'converted')
            converted_dir.mkdir(parents=True)
            # ...and ingestion results
            ingested_dir = Path(out_dir, 'ingested')
            ingested_product_dir = ingested_dir / 'product'
            (ingested_product_dir / 'ingested_hh').mkdir(parents=True)

            with mock.patch(
                        'geospaas_processing.converters.syntool.converter.SyntoolConverter.ingest',
                        return_value=['ingested/product']) as mock_ingest, \
                 mock.patch.object(converter, 'list_files',
                                   return_value=[str((converted_dir / 'converted_hh.tiff'))]):
                converter.ingest(str(converted_dir), out_dir, ['--baz'])

            self.assertListEqual(
                list(ingested_dir.iterdir()),
                [ingested_dir / 'ingested_hh'])


class Sentinel3OLCISyntoolConverterTestCase(unittest.TestCase):
    """Tests for the Sentinel1SyntoolConverter class"""

    def test_run_conversion_multi_channels(self):
        """Test that the converter is run for every channel in
        converter options
        """
        converter = syntool_converter.Sentinel3OLCISyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with mock.patch.object(converter, 'convert',
                               side_effect=[['conv1.tiff'], ['conv2.tiff']]) as mock_convert:
            results = converter.run_conversion('in.nc', 'out',
                                               {'converter_options': {'channels': ['baz', 'qux']}})
        mock_convert.assert_has_calls((
            mock.call('in.nc', 'out', ['-t', 'foo', '-opt', 'channels=baz']),
            mock.call('in.nc', 'out', ['-t', 'foo', '-opt', 'channels=qux'])
        ))
        self.assertListEqual(results, [Path('out', 'conv1.tiff'), Path('out', 'conv2.tiff')])

    def test_run_conversion_single_channels(self):
        """Test that the converter is run once if channels is a string
        """
        converter = syntool_converter.Sentinel3OLCISyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with mock.patch.object(converter, 'convert',
                               return_value=['conv1.tiff', 'conv2.tiff']) as mock_convert:
            results = converter.run_conversion('in.nc', 'out',
                                               {'converter_options': {'channels': 'baz,qux'}})
        mock_convert.assert_called_with('in.nc', 'out', ['-t', 'foo', '-opt', 'channels=baz,qux'])
        self.assertListEqual(results, [Path('out', 'conv1.tiff'), Path('out', 'conv2.tiff')])

    def test_run_conversion_system_exit(self):
        """Test that SystemExit exceptions do not interrupt the
        conversion process but simply return empty results
        """
        error = syntool_converter.ConversionError()
        error.__cause__ = SystemExit()
        converter = syntool_converter.Sentinel3OLCISyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        with mock.patch.object(converter, 'convert', side_effect=[error, ['conv2.tiff']]):
            with self.assertLogs(level=logging.WARNING):
                results = converter.run_conversion(
                    'in.nc', 'out', {'converter_options': {'channels': ['baz', 'qux']}})
        self.assertListEqual(results, [Path('out', 'conv2.tiff')])


class CustomReaderSyntoolConverterTestCase(unittest.TestCase):
    """Tests for the CustomReaderSyntoolConverter class"""

    def test_parse_converter_args(self):
        """Check that the -r option to runner.py is added"""
        converter = syntool_converter.CustomReaderSyntoolConverter(
            converter_type='foo',
            ingest_parameter_files='bar')
        self.assertListEqual(
            converter.parse_converter_args({'converter_options': {'baz': 'quz'}}),
            ['-r', 'foo', '-opt', 'baz=quz'])
