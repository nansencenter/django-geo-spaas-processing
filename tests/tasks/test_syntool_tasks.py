"""Tests for Syntool conversion tasks"""
import logging
import subprocess
import unittest
import unittest.mock as mock
from datetime import timedelta
from pathlib import Path

import django.test
import celery.exceptions

import geospaas_processing.converters.syntool
import geospaas_processing.tasks
import geospaas_processing.tasks.syntool as tasks_syntool
from geospaas_processing.models import ProcessingResult


class SyntoolTasksTestCase(unittest.TestCase):
    """Tests for syntool tasks"""

    def test_get_db_config(self):
        """Check we get the right database config"""
        with mock.patch('os.environ', {'SYNTOOL_DATABASE_HOST': 'a', 'SYNTOOL_DATABASE_NAME': 'b'}):
            self.assertTupleEqual(tasks_syntool.get_db_config(), ('a', 'b'))

    def test_save_results(self):
        """Check the files resulting from conversion are saved to the
        database
        """
        with mock.patch(
                'geospaas_processing.models.ProcessingResult.objects.get_or_create'
                ) as mock_get_or_create, \
             mock.patch('geospaas.catalog.models.Dataset.objects.get') as mock_get_dataset:
            tasks_syntool.save_results(1, ('foo', 'bar'))
        mock_get_or_create.assert_has_calls((
            mock.call(
                dataset=mock_get_dataset.return_value,
                path='foo',
                type=ProcessingResult.ProcessingResultType.SYNTOOL,
                ttl=None),
            mock.call(
                dataset=mock_get_dataset.return_value,
                path='bar',
                type=ProcessingResult.ProcessingResultType.SYNTOOL,
                ttl=None),
        ))

    def test_save_results_ttl(self):
        """Test saving processing results with a TTL"""
        with mock.patch(
                'geospaas_processing.models.ProcessingResult.objects.get_or_create'
                ) as mock_get_or_create, \
             mock.patch('geospaas.catalog.models.Dataset.objects.get') as mock_get_dataset:
            tasks_syntool.save_results(1, ('foo',), ttl=timedelta(days=10))
            tasks_syntool.save_results(1, ('bar',), ttl={'days': 15})
        mock_get_or_create.assert_has_calls((
            mock.call(
                dataset=mock_get_dataset.return_value,
                path='foo',
                type=ProcessingResult.ProcessingResultType.SYNTOOL,
                ttl=timedelta(days=10)),
            mock.call(
                dataset=mock_get_dataset.return_value,
                path='bar',
                type=ProcessingResult.ProcessingResultType.SYNTOOL,
                ttl=timedelta(days=15)),
        ))

    def test_save_results_ttl_error(self):
        """A ValueError should be raised if the ttl argument has
        the wrong type
        """
        with self.assertRaises(ValueError):
            tasks_syntool.save_results(1, ('foo',), ttl='10 days')

    def test_check_ingested_already_exist(self):
        """If ingested files already exist for the current dataset,
        the current tasks chain should be stopped.
        This does not test the actual chain interruption because it's
        a pain to set up
        """
        mock_queryset = mock.MagicMock()
        mock_queryset.__iter__.return_value = [mock.Mock(path='foo'), mock.Mock(path='bar')]
        with mock.patch('geospaas_processing.models.ProcessingResult.objects.filter',
                        return_value=mock_queryset):
            self.assertTupleEqual(
                tasks_syntool.check_ingested((1,), to_execute=mock.Mock()),
                (1, ['foo', 'bar']))

    def test_check_ingested_dont_exist(self):
        """If no result files exist for the dataset, replace the current task
        with the signature to execute
        """
        mock_queryset = mock.MagicMock()
        mock_queryset.exists.return_value = False
        with mock.patch('geospaas_processing.models.ProcessingResult.objects.filter',
                        return_value=mock_queryset):
            with self.assertRaises(celery.exceptions.Ignore):
                self.assertTupleEqual(tasks_syntool.check_ingested((1,), to_execute=mock.Mock()),
                                    (1,))

    def test_convert(self):
        """Test that the dataset files are converted to Syntool format
        and the resulting files are saved to the database
        """
        with mock.patch('geospaas_processing.tasks.syntool.SyntoolConversionManager',
                ) as mock_conversion_manager, \
             mock.patch('geospaas_processing.tasks.syntool.save_results') as mock_save_results:
            mock_conversion_manager.return_value.convert.return_value = ('bar', 'baz')
            result = tasks_syntool.convert((1, ('foo',)))
        mock_conversion_manager.assert_called_once_with(geospaas_processing.tasks.WORKING_DIRECTORY)
        mock_conversion_manager.return_value.convert.assert_called_once_with(
            1, 'foo', results_dir=geospaas_processing.tasks.WORKING_DIRECTORY)
        mock_save_results.assert_called_once_with(1, ('bar', 'baz'), ttl=None)
        self.assertTupleEqual(result, (1, ('bar', 'baz')))

    def test_compare_profiles(self):
        """Test that the right calls are made in compare_profiles"""
        with mock.patch('celery.Task.request') as mock_task_request, \
             mock.patch('tempfile.TemporaryDirectory') as mock_tmpdir, \
             mock.patch('subprocess.run') as mock_run, \
             mock.patch('pathlib.Path.iterdir') as mock_iterdir, \
             mock.patch('shutil.copytree')as mock_copytree, \
             mock.patch('geospaas_processing.tasks.syntool.save_results') as mock_save_results, \
             mock.patch('geospaas_processing.utils.redis_lock') as mock_lock:

            with self.subTest('compare_profiles should succeed'):
                mock_task_request.id = 'task_id'
                mock_tmpdir.return_value.__enter__.return_value = '/tmp'
                mock_run.return_value.returncode = 0
                mock_iterdir.side_effect = (
                    (Path('3413_product_1'), Path('3413_product_2')),
                    (Path('product_1_granule_1'), Path('product_1_granule_2')),
                    (Path('product_2_granule_1'),),
                )

                results = tasks_syntool.compare_profiles(((1, ('/foo',)),
                                                        ((2, ('/bar',)), (3, ('/baz',)))))
                self.assertTupleEqual(
                    results,
                    (1, ['ingested/3413_product_1/product_1_granule_1',
                        'ingested/3413_product_1/product_1_granule_2',
                        'ingested/3413_product_2/product_2_granule_1'])
                )
                mock_lock.assert_has_calls((
                    mock.call('lock-1', 'task_id'),
                    mock.call('lock-2', 'task_id'),
                    mock.call('lock-3', 'task_id')
                ))
                mock_run.assert_called_once_with(
                    [
                        'python2',
                        str(Path(geospaas_processing.converters.syntool.__file__).parent /
                            Path('extra_readers', 'compare_model_argo.py')),
                        '/foo', '/bar,/baz', '/tmp'
                    ],
                    capture_output=True)
                mock_copytree.assert_has_calls((
                    mock.call('3413_product_1', '/tmp/test_data/ingested/3413_product_1',
                            dirs_exist_ok=True),
                    mock.call('3413_product_2', '/tmp/test_data/ingested/3413_product_2',
                            dirs_exist_ok=True)
                ))
                mock_save_results.assert_called_once_with(
                    1,
                    ['ingested/3413_product_1/product_1_granule_1',
                    'ingested/3413_product_1/product_1_granule_2',
                    'ingested/3413_product_2/product_2_granule_1'],
                    ttl=None,
                )
            with self.subTest('compare_profiles should fail'):
                mock_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd='foo')
                with self.assertRaises(subprocess.CalledProcessError), \
                     self.assertLogs(tasks_syntool.logger, logging.ERROR):
                    tasks_syntool.compare_profiles(((1, ('/foo',)),
                                                    ((2, ('/bar',)), (3, ('/baz',)))))


class DBInsertTestCase(unittest.TestCase):
    """Tests for the db_insert() task"""

    def setUp(self):
        mock.patch('os.environ',
                   {'SYNTOOL_DATABASE_HOST': 'db', 'SYNTOOL_DATABASE_NAME': 'syntool'}).start()
        self.mock_popen = mock.patch('subprocess.Popen').start()
        self.mock_run = mock.patch('subprocess.run').start()

    def tearDown(self):
        mock.patch.stopall()

    def test_db_insert(self):
        """Test insertion of conversion results into the Syntool
        database
        """
        self.mock_popen.return_value.wait.return_value = 0
        self.mock_run.return_value.returncode = 0
        tasks_syntool.db_insert((1, ('foo',)))

        self.mock_popen.assert_called_with(
            ['syntool-meta2sql', '--chunk_size=100', '-', '--',
             str(Path(geospaas_processing.tasks.WORKING_DIRECTORY, 'foo', 'metadata.json'))],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True)
        self.mock_run.assert_called_with(
            ['mysql', '-h', 'db', 'syntool'],
            stdin=self.mock_popen.return_value.stdout,
            capture_output=True,
            check=True)

    def test_db_insert_subprocess_error(self):
        """Test handling errors when the subprocess is run"""
        self.mock_run.side_effect = subprocess.CalledProcessError(1, '')
        with self.assertRaises(subprocess.CalledProcessError), \
             self.assertLogs(tasks_syntool.logger):
            tasks_syntool.db_insert((1, ('foo',)))

    def test_db_insert_meta2sql_return_code_error(self):
        """Test handling return code != 0 for meta2sql process"""
        self.mock_popen.return_value.wait.return_value = 1
        with self.assertRaises(RuntimeError):
            tasks_syntool.db_insert((1, ('foo',)))

    def test_db_insert_mysql_return_code_error(self):
        """Test handling return code != 0 for mysql process"""
        self.mock_popen.return_value.wait.return_value = 0
        self.mock_run.return_value.returncode = 1
        with self.assertRaises(RuntimeError):
            tasks_syntool.db_insert((1, ('foo',)))


class CleanupIngestedTestCase(django.test.TestCase):
    """Tests for the cleanup() task"""

    fixtures = [Path(__file__).parent.parent / 'data/test_data.json']

    def setUp(self):
        mock.patch('os.environ',
                   {'SYNTOOL_DATABASE_HOST': 'db', 'SYNTOOL_DATABASE_NAME': 'syntool'}).start()
        self.mock_rmtree = mock.patch('shutil.rmtree').start()
        self.mock_remove = mock.patch('os.remove').start()
        self.mock_run = mock.patch('subprocess.run').start()

    def tearDown(self):
        mock.patch.stopall()

    def test_cleanup(self):
        """Test standard call, deleting based on creation date"""
        expected_path = 'ingested/product_name/granule_name/'  # see fixture
        with self.assertLogs(tasks_syntool.logger):
            self.assertListEqual(tasks_syntool.cleanup({'id': 1}), [expected_path])
        self.mock_rmtree.assert_called_with(Path(
            geospaas_processing.tasks.WORKING_DIRECTORY,
            expected_path))
        self.mock_run.assert_called_with(
            [
                'mysql', '-h', 'db', 'syntool', '-e',
                "DELETE FROM `product_product_name` WHERE dataset_name = 'granule_name';"
            ],
            capture_output=True,
            check=True)
        self.assertFalse(ProcessingResult.objects.filter(id=1).exists())

    def test_cleanup_file(self):
        """Test deleting a file (usually won't happen)"""
        self.mock_rmtree.side_effect = NotADirectoryError
        expected_path = 'ingested/product_name/granule_name/'  # see fixture
        with self.assertLogs(tasks_syntool.logger):
            self.assertListEqual(tasks_syntool.cleanup({'id': 1}), [expected_path])

    def test_cleanup_file_not_found(self):
        """Test behavior when the result files are already deleted
        """
        self.mock_rmtree.side_effect = FileNotFoundError
        expected_path = 'ingested/product_name/granule_name/'  # see fixture
        with self.assertLogs(tasks_syntool.logger, level=logging.WARNING):
            self.assertListEqual(tasks_syntool.cleanup({'id': 1}), [expected_path])

    def test_cleanup_stale_file_handle(self):
        """Test behavior when a stale file handle error happens
        """
        self.mock_rmtree.side_effect = OSError(116, '[Errno 116] Stale file handle')
        expected_path = 'ingested/product_name/granule_name/'  # see fixture
        with self.assertLogs(tasks_syntool.logger, level=logging.WARNING):
            self.assertListEqual(tasks_syntool.cleanup({'id': 1}), [expected_path])

    def test_cleanup_file_subprocess_error(self):
        """Test behavior when an error occurs running the mysql command
        """
        expected_path = 'ingested/product_name/granule_name/'  # see fixture
        self.mock_run.side_effect = subprocess.CalledProcessError(1, '')
        with self.assertLogs(tasks_syntool.logger, level=logging.ERROR), \
             self.assertRaises(subprocess.CalledProcessError):
            self.assertListEqual(tasks_syntool.cleanup({'id': 1}), [expected_path])
