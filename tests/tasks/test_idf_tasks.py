"""Tests for the IDF tasks"""
import unittest
import unittest.mock as mock
import geospaas_processing.tasks.idf as tasks_idf

class ConvertToIDFTestCase(unittest.TestCase):
    """Tests for the convert_to_idf() task"""

    def setUp(self):
        idf_converter_patcher = mock.patch('geospaas_processing.tasks.idf.IDFConversionManager')
        self.idf_converter_mock = idf_converter_patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_convert_if_acquired(self):
        """A conversion must be triggered if the lock is acquired"""
        dataset_file_name = 'dataset.nc'
        converted_file_name = f"{dataset_file_name}.idf"
        self.idf_converter_mock.return_value.convert.return_value = [converted_file_name]
        self.assertEqual(
            tasks_idf.convert_to_idf((1, (dataset_file_name,))),  # pylint: disable=no-value-for-parameter
            (1, [converted_file_name]))
