from unittest import TestCase
from geospaas_processing.copiers import Copier
import unittest.mock as mock


class CopyingWarningTestCase(TestCase):
    """Tests for the warnings of copying inside 'file_or_symlink_copy' function """
    @mock.patch('os.path.basename')
    @mock.patch('os.path.join')
    @mock.patch('geospaas_processing.copiers.exists', return_value=True)
    def test_warning_for_copying_action( self, mock_exs, mock_join, mock_basename):
        """Tests warning logs for the cases that there is already a file or folder with same name in
        the destination folder """
        source_path = mock.MagicMock()
        dataset = mock.MagicMock()
        dataset.id=3
        test_copier = Copier(type_in_flag_file='', destination_path='')
        with self.assertLogs(level='WARNING') as warn:
            test_copier.file_or_symlink_copy([source_path], dataset)
            self.assertIn(
                'WARNING:geospaas_processing.copiers:Failed to copy dataset 3: the destination path'
                ' already exists.', warn.output)

    @mock.patch('os.path.basename')
    @mock.patch('os.path.join')
    @mock.patch('geospaas_processing.copiers.exists', return_value=False)
    def test_warning_for_copying_action2( self, mock_exs, mock_join, mock_basename):
        """
        Tests warning logs for the cases that the is an incorrect address in the database for source
        """
        source_path = mock.MagicMock()
        dataset = mock.MagicMock()
        dataset.id=3
        source_path.uri = 'fake_path'
        test_copier = Copier(type_in_flag_file='', destination_path='')
        with self.assertLogs(level='WARNING') as warn:
            test_copier.file_or_symlink_copy([source_path], dataset)
            self.assertIn(
                'WARNING:geospaas_processing.copiers:For stored address of dataset with id = 3, '
                'there is no file or no folder in the stored address: fake_path.', warn.output)
