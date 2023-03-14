"""Tests for the celery tasks"""
import logging
import unittest
import unittest.mock as mock

import graypy.handler

import geospaas_processing.tasks as tasks
import geospaas_processing.utils as utils


class FaultTolerantTaskTestCase(unittest.TestCase):
    """Tests for the FaultTolerantTask base class"""

    def test_django_connection_closed_after_task(self):
        """The after_return handler must be defined and close the connection to the database"""
        with mock.patch('django.db.connection.close') as mock_close:
            tasks.FaultTolerantTask().after_return()
            mock_close.assert_called_once()


class LockDecoratorTestCase(unittest.TestCase):
    """Tests for the `lock_dataset_files()` decorator"""

    def setUp(self):
        redis_patcher = mock.patch.object(utils, 'Redis')
        self.redis_mock = redis_patcher.start()
        mock.patch.object(utils, 'REDIS_HOST', 'test').start()
        mock.patch.object(utils, 'REDIS_PORT', 6379).start()
        self.addCleanup(mock.patch.stopall)

    @staticmethod
    @tasks.lock_dataset_files
    def decorated_function(task, args):  # pylint: disable=unused-argument
        """Dummy function used to test the `lock_dataset_files()` decorator"""
        return (args[0],)

    def test_function_called_if_acquired(self):
        """If the lock is acquired, the wrapped function must be called"""
        self.redis_mock.return_value.setnx.return_value = 1
        self.assertEqual(self.decorated_function(mock.Mock(), (1,)), (1,))
        self.assertEqual(self.decorated_function(mock.Mock(), args=(1,)), (1,))

    def test_retry_if_locked(self):
        """If the lock is is not acquired, retries must be made"""
        self.redis_mock.return_value.setnx.return_value = 0
        mock_task = mock.Mock()
        args = (1,)
        self.decorated_function(mock_task, args)
        mock_task.retry.assert_called()


class SignalsTestCase(unittest.TestCase):
    """Unit tests for Celery signal functions"""

    def test_setup_logger(self):
        """
        The setup_logger() functions must add a GELF handler if the
        right environment variables are defined
        """
        with mock.patch('os.getenv', return_value='test'):
            logger = logging.Logger('test_logger')
            tasks.setup_logger(logger)
            handler = logger.handlers[0]
        self.assertIsInstance(handler, graypy.handler.GELFTCPHandler)
        self.assertEqual(handler.host, 'test')
        self.assertEqual(handler.port, 'test')
        self.assertEqual(handler.facility, tasks.__name__)
