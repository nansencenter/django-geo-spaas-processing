"""Tests for the utils module"""
import os
import os.path
import tarfile
import tempfile
import unittest
import unittest.mock as mock
from contextlib import contextmanager

import geospaas_processing.utils as utils

class RedisLockTestCase(unittest.TestCase):
    """Tests for the redis_lock context manager"""

    def setUp(self):
        patcher = mock.patch.object(utils, 'Redis')
        mock.patch.object(utils, 'REDIS_HOST', 'test').start()
        mock.patch.object(utils, 'REDIS_PORT', 6379).start()
        self.redis_mock = patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_redis_lock_standard_usage(self):
        """
        Test that the lock is acquired if the key is successfully set,
        and that it's freed afterwards
        """
        self.redis_mock.return_value.setnx.return_value = 1
        with utils.redis_lock('id', 'oid') as acquired:
            self.assertTrue(acquired)
        self.redis_mock.return_value.delete.assert_called_with('id')

    def test_redis_lock_existing_lock(self):
        """Test that the lock is not acquired if it already exists, and is not deleted"""
        self.redis_mock.return_value.setnx.return_value = 0
        with utils.redis_lock('id', 'oid') as acquired:
            self.assertFalse(acquired)
        self.redis_mock.return_value.delete.assert_not_called()


class UnzipTestCase(unittest.TestCase):
    """Test for the unzip() method of IDFConverter"""

    def setUp(self):
        self.zipfile_patcher = mock.patch('zipfile.ZipFile')
        zipfile_mock = self.zipfile_patcher.start()
        self.extractall_mock = mock.Mock()
        zipfile_mock.return_value.__enter__.return_value.extractall = self.extractall_mock

    def tearDown(self):
        self.zipfile_patcher.stop()

    def test_unzip_with_out_dir(self):
        """When out_dir is provided, `unzip()` should extract the archive contents there"""
        utils.unzip('/foo/bar.zip', '/output_dir')
        self.extractall_mock.assert_called_with('/output_dir')

    def test_unzip_without_out_dir(self):
        """When out_dir is provided, `unzip()` should extract the archive contents there"""
        utils.unzip('/foo/bar.zip')
        self.extractall_mock.assert_called_with('/foo')


class UtilsTestCase(unittest.TestCase):
    """Tests for the utility functions"""

    def test_unarchive_zip_file(self):
        """
        Test that a zip file is recognized and extracted to a directory named like the archive file
        """
        with mock.patch('geospaas_processing.utils.unzip') as unzip_mock:
            with mock.patch('zipfile.is_zipfile', return_value=True):
                utils.unarchive('/foo/bar.zip')
                unzip_mock.assert_called_with('/foo/bar.zip', '/foo/bar')

    def test_tar_gzip_file(self):
        """`utils.tar_gzip()` must archive the given file in the tar.gz format"""
        with tempfile.TemporaryDirectory() as temp_dir_name:
            file_to_archive = 'foo.txt'
            file_path = os.path.join(temp_dir_name, file_to_archive)
            with open(file_path, 'w') as file_handler:
                file_handler.write('hello')

            archive_path = utils.tar_gzip(file_path)
            self.assertEqual(archive_path, f"{file_path}.tar.gz")
            self.assertTrue(tarfile.is_tarfile(archive_path))
            with tarfile.open(archive_path) as tar_file:
                self.assertListEqual(tar_file.getnames(), [os.path.basename(file_path)])

    def test_tar_gzip_file_noop(self):
        """`utils.tar_gzip()` must do nothing if the given file is already an archive"""
        with tempfile.TemporaryDirectory() as temp_dir_name:
            archive_file_path = os.path.join(temp_dir_name, 'test.tgz')

            # Create empty tar.gz file
            archive_file = tarfile.open(archive_file_path, 'w:gz')
            archive_file.close()

            with mock.patch('tarfile.TarFile.add') as mock_add:
                result = utils.tar_gzip(archive_file_path)
            self.assertEqual(result, archive_file_path)
            mock_add.assert_not_called()

    def test_yaml_env_safe_load(self):
        """yaml_env_safe_load() should return the same as result of
        yaml.safe_load(), except !ENV tagged values are replaced with
        the contents of the corresponding environment variable.
        """
        yaml_string = '''---
        var1: !ENV foo
        var2: baz
        '''
        with mock.patch('os.environ', {'foo': 'bar'}):
            self.assertDictEqual(
                utils.yaml_env_safe_load(yaml_string),
                {'var1': 'bar', 'var2': 'baz'}
            )


class AbstractStorageMethodsTestCase(unittest.TestCase):
    """Tests for the abstract methods of the base Storage class"""
    def setUp(self):
        with mock.patch.object(utils.Storage, 'get_block_size'):
            self.storage = utils.Storage(path='')

    def test_get_block_size_is_abstract(self):
        """get_block_size() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.get_block_size()

    def test_get_free_space_is_abstract(self):
        """get_free_space() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.get_free_space()

    def test_listdir_is_abstract(self):
        """listdir() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.listdir('')

    def test_stat_is_abstract(self):
        """stat() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.stat('')

    def test_isfile_is_abstract(self):
        """isfile() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.isfile('')

    def test_isdir_is_abstract(self):
        """isdir() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.isdir('')

    def test_remove_is_abstract(self):
        """remove() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.remove('')

    def test_put_is_abstract(self):
        """put() must be abstract"""
        with self.assertRaises(NotImplementedError):
            self.storage.put('', '')


class NonAbstractStorageMethodsTestCase(unittest.TestCase):
    """Tests for the non abstract methods of the base Storage class"""

    class StubStorage(utils.Storage):
        """Stub class used to test the non abstract methods of the Storage class"""
        path_contents = ['dir1', 'file1']
        dir1_contents = ['file2']

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.free_disk_space = 500

        def get_block_size(self):
            return 32

        def get_free_space(self):
            return self.free_disk_space

        def listdir(self, dir_path):
            if dir_path == '':
                return self.path_contents
            elif dir_path == 'dir1':
                return self.dir1_contents
            else:
                raise FileNotFoundError()

        def stat(self, path):
            # The order of the arguments in the tuple to build stat_result objects is:
            # st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid,
            # st_size, st_atime, st_mtime, st_ctime
            if path == 'dir1':  # directory
                return os.stat_result((
                    16877, 3146999, 66306, 4, 10000, 10000,
                    32, 1599210909, 1599210909, 1599210909
                ))
            elif path == 'file1':  # 200 MiB file
                return os.stat_result((
                    16877, 3147000, 66306, 1, 10000, 10000,
                    200, 1599210909, 1599210908, 1599210909
                ))
            elif path == 'dir1/file2':  # 100 MiB file
                return os.stat_result((
                    16877, 3146999, 66306, 1, 10000, 10000,
                    100, 1599210909, 1599210910, 1599210909
                ))
            else:
                raise FileNotFoundError()

        def isfile(self, path):
            if path == self.path:
                return False
            elif path == 'dir1':
                return False
            elif path == 'file1':
                return True
            elif path == 'dir1/file2':
                return True
            else:
                raise FileNotFoundError()

        def isdir(self, path):
            if path == self.path:
                return True
            elif path == 'dir1':
                return True
            elif path == 'file1':
                return False
            elif path == 'dir1/file2':
                return False
            else:
                raise FileNotFoundError()

        def remove(self, path):
            if path == 'file1':
                self.free_disk_space += 200
            elif path == 'dir1/file2':
                self.free_disk_space += 100

        def put(self, local_path, storage_path):
            return None

    def setUp(self):
        self.storage = self.StubStorage(path='/foo/bar')

    def test_get_file_disk_usage(self):
        """Must return the space occupied by a file's blocks"""
        self.assertEqual(self.storage._get_file_disk_usage(0), 0)
        self.assertEqual(self.storage._get_file_disk_usage(1), 32)
        self.assertEqual(self.storage._get_file_disk_usage(32), 32)
        self.assertEqual(self.storage._get_file_disk_usage(33), 64)

    def test_get_removable_files(self):
        """get_removable_files() must return the files and their characteristics"""
        self.assertListEqual(
            self.storage._get_removable_files(),
            [('file1', 224, 1599210908),
             ('dir1/file2', 128, 1599210910)]
        )

    def test_sort_by_mtime(self):
        """
        Test sorting a list of tuples returned by get_removable_files() by their modification time
        """
        self.assertEqual(
            self.storage._sort_by_mtime([('1', 1, 2.0), ('2', 1, 1.0)]),
            [('2', 1, 1.0), ('1', 1, 2.0)]
        )

    def test_total_freeable_space(self):
        """Must return the sum of the sizes of a list of removable files"""
        self.assertEqual(
            self.storage._total_freeable_space([('', 1, 1), ('', 2, 2), ('', 3, 3)]),
            6
        )

    def test_delete_files(self):
        """
        delete_files() must remove files from the given list to free the required space
        """
        self.storage._delete_files(750, [('file1', 200, 1), ('dir1/file2', 100, 2)])
        self.assertEqual(self.storage.free_disk_space, 800)

    def test_free_space(self):
        """
        free_space() must delete enough files from the directory to make space for the new file
        """
        self.storage.free_space(750)
        self.assertEqual(self.storage.free_disk_space, 800)

    def test_free_space_error_if_file_too_big(self):
        """
        free_space() must raise an exception without trying to free space if the file is bigger
        than the sum of the free space and the freeable space
        """
        with self.assertRaises(utils.CleanUpError):
            self.storage.free_space(900)

    def test_free_space_error_if_no_removable_file(self):
        """free_space() must raise an exception if no file to remove was found"""
        with mock.patch.object(self.StubStorage, '_get_removable_files', return_value=[]):
            with self.assertRaises(utils.CleanUpError):
                self.storage.free_space(750)

    def test_free_space_noop(self):
        """Do nothing if there is enough free space"""
        self.assertEqual(self.storage.free_space(400), (0, []))
        self.assertEqual(self.storage.free_disk_space, 500)

    def test_free_space_wait_if_cleanup_in_progress(self):
        """free_space() must wait if another process is doing some cleanup"""
        waited = False
        @contextmanager
        def redis_lock_once(*args):  #pylint: disable=unused-argument
            """Context manager that returns False once, then always True"""
            nonlocal waited
            if waited:
                yield True
            else:
                waited = True
                yield False

        # redis_lock_once() is used to trigger the waiting mechanism once,
        # then have the normal free_space() behavior.
        # We attempt to free less space than available so that the function does nothing.
        with mock.patch.object(utils, 'redis_lock', side_effect=redis_lock_once):
            with mock.patch('time.sleep') as mock_sleep:
                with self.assertLogs(utils.LOGGER):
                    self.storage.free_space(750)
                mock_sleep.assert_called_once()

    def test_free_space_error_if_wait_too_long(self):
        """
        free_space() must raise an error if the wait for the other process to finish is too long
        """
        @contextmanager
        def redis_always_lock(*args):  # pylint: disable=unused-argument
            """Context manager that always returns False"""
            yield False

        with mock.patch.object(utils, 'redis_lock', side_effect=redis_always_lock):
            with mock.patch('time.sleep'):
                with self.assertLogs(utils.LOGGER), self.assertRaises(utils.CleanUpError):
                    self.storage.free_space(750)


class LocalStorageTestCase(unittest.TestCase):
    """Tests for the LocalStorage class"""

    def setUp(self):
        with mock.patch.object(utils.LocalStorage, 'get_block_size', return_value=4096):
            self.storage = utils.LocalStorage(path='/foo/bar/')

    def test_get_block_size(self):
        """Must get the block size of the filesystem on which the storage is located"""
        with mock.patch('os.stat') as mock_stat:
            self.storage.get_block_size()
        mock_stat.assert_called_with(self.storage.path)

    def test_get_free_space(self):
        """Must return the free space on the filesystem on which the storage is located"""
        with mock.patch('shutil.disk_usage') as mock_disk_usage:
            self.storage.get_free_space()
        mock_disk_usage.assert_called_with(self.storage.path)

    def test_listdir(self):
        """Must list the directories in the given path (relative to the storage path)"""
        dir_name = 'baz'
        with mock.patch('os.listdir') as mock_listdir:
            self.storage.listdir(dir_name)
        mock_listdir.assert_called_with(os.path.join(self.storage.path, dir_name))

    def test_stat(self):
        """Must return an os.stat_result object containing information about the file"""
        file_name = 'baz'
        with mock.patch('os.stat') as mock_stat:
            self.storage.stat(file_name)
        mock_stat.assert_called_with(os.path.join(self.storage.path, file_name))

    def test_isfile(self):
        """Must return True if the given path is a file"""
        file_name = 'baz'
        with mock.patch('os.path.isfile') as mock_isfile:
            self.storage.isfile(file_name)
        mock_isfile.assert_called_with(os.path.join(self.storage.path, file_name))

    def test_isdir(self):
        """Must return True if the given path is a directory"""
        file_name = 'baz'
        with mock.patch('os.path.isdir') as mock_isdir:
            self.storage.isdir(file_name)
        mock_isdir.assert_called_with(os.path.join(self.storage.path, file_name))

    def test_remove(self):
        """Must remove the given file"""
        file_name = 'baz'
        with mock.patch('os.remove') as mock_remove:
            self.storage.remove(file_name)
        mock_remove.assert_called_with(os.path.join(self.storage.path, file_name))

    def test_put(self):
        """Must copy the given file from the local filesystem to the storage"""
        file_name = 'baz'
        storage_path = 'dir1'
        with mock.patch('shutil.copy') as mock_copy:
            self.storage.put(file_name, storage_path)
        mock_copy.assert_called_with(file_name, os.path.join(self.storage.path, storage_path))


class RemoteStorageTestCase(unittest.TestCase):
    """Tests for the RemoteStorage class"""

    def setUp(self):
        mock.patch('paramiko.SSHConfig').start()
        mock.patch('paramiko.SSHClient').start()
        with mock.patch.object(utils.RemoteStorage, 'get_block_size', return_value=4096):
            self.storage = utils.RemoteStorage(host='server', path='/foo/bar/')
        self.addCleanup(mock.patch.stopall)

    def test_remote_storage_destructor(self):
        """The SSH connection should be closed when a RemoteStorage object is destroyed"""
        self.storage.__del__()
        self.storage.ssh_client.close.assert_called_once()  # pylint: disable=no-member

    def test_get_block_size(self):
        """Must get the block size of the filesystem on which the storage is located"""
        stdout_mock = mock.MagicMock()
        stdout_mock.read.return_value = '2'
        self.storage.ssh_client.exec_command.return_value = (stdout_mock, stdout_mock, stdout_mock)

        self.assertEqual(self.storage.get_block_size(), 2)
        self.storage.ssh_client.exec_command.assert_called_with(  # pylint: disable=no-member
            f"stat -f --printf '%S' '{self.storage.path}'")

    def test_get_free_space(self):
        """Must return the free space on the filesystem on which the storage is located"""
        stdout_mock = mock.MagicMock()
        stdout_mock.readlines.return_value = ['', '1 2 3 4']
        self.storage.ssh_client.exec_command.return_value = (stdout_mock, stdout_mock, stdout_mock)

        self.assertEqual(self.storage.get_free_space(), 4)
        self.storage.ssh_client.exec_command.assert_called_with(  # pylint: disable=no-member
            f"df -B 1 -P '{self.storage.path}'")

    def test_listdir(self):
        """Must list the directories in the given path (relative to the storage path)"""
        dir_name = 'foo'
        self.storage.listdir(dir_name)
        self.storage.sftp_client.listdir.assert_called_with(  # pylint: disable=no-member
            os.path.join(self.storage.path, dir_name))

    def test_stat(self):
        """Must return an os.stat_result object containing information about the file"""
        dir_name = 'foo/bar'
        self.storage.stat(dir_name)
        self.storage.sftp_client.stat.assert_called_with(  # pylint: disable=no-member
            os.path.join(self.storage.path, dir_name))

    def test_isfile(self):
        """Must return True if the given path is a file, based on the mode returned by stat"""
        self.storage.sftp_client.stat.return_value.st_mode = 16877  # pylint: disable=no-member
        self.assertFalse(self.storage.isfile(''))
        self.storage.sftp_client.stat.return_value.st_mode = 33188  # pylint: disable=no-member
        self.assertTrue(self.storage.isfile(''))

    def test_isdir(self):
        """Must return True if the given path is a directory, based on the mode returned by stat"""
        self.storage.sftp_client.stat.return_value.st_mode = 16877  # pylint: disable=no-member
        self.assertTrue(self.storage.isdir(''))
        self.storage.sftp_client.stat.return_value.st_mode = 33188  # pylint: disable=no-member
        self.assertFalse(self.storage.isdir(''))

    def test_remove(self):
        """Must remove the given file"""
        dir_name = 'foo/bar'
        self.storage.remove(dir_name)
        self.storage.sftp_client.remove.assert_called_with(  # pylint: disable=no-member
            os.path.join(self.storage.path, dir_name))

    def test_put(self):
        """Must copy the given file from the local filesystem to the storage"""
        local_path = 'foo/bar'
        storage_path = 'baz/'
        remote_path = os.path.join(self.storage.path, storage_path)
        self.storage.ssh_client.exec_command.return_value = (None, mock.Mock(), None)
        with mock.patch('scp.SCPClient.put') as mock_put:
            self.storage.put(local_path, storage_path)
        self.storage.ssh_client.exec_command.assert_called_with(  # pylint: disable=no-member
            f"mkdir -p {os.path.dirname(remote_path)}")
        mock_put.assert_called_with(local_path, recursive=True, remote_path=remote_path)
