"""Tools for file format conversion"""
import glob
import logging
import os.path
import re
import shutil
import subprocess
from enum import Enum
from datetime import datetime, timedelta

from geospaas.catalog.models import Dataset

import geospaas_processing.utils as utils


LOGGER = logging.getLogger(__name__)


class ConversionError(Exception):
    """Error during conversion"""


class ParameterType(Enum):
    """Types of parameters to the idf-converter script"""
    INPUT = '-i'
    OUTPUT = '-o'
    READER = '-t'


class IDFConverter():
    """IDF converter which uses the idf_converter package from ODL"""
    PARAMETERS_DIR = os.path.join(os.path.dirname(__file__), 'parameters')

    def __init__(self, parameter_file):
        """"""
        self.parameter_path = os.path.join(self.PARAMETERS_DIR, parameter_file)
        self.collection = self.extract_parameter_value(ParameterType.OUTPUT, 'collection')

    @classmethod
    def get_parameter_file(cls, dataset):
        """Returns the name of the parameter file to use for the
        dataset argument.
        """
        raise NotImplementedError()

    def run(self, in_file, out_dir):
        """Run the IDF converter"""
        input_cli_args = ['-i', 'path', '=', in_file]
        output_cli_args = ['-o', 'path', '=', out_dir]
        LOGGER.debug(
            "Converting %s to IDF using parameter file %s", in_file, self.parameter_path)
        result = subprocess.run(
            ['idf-converter', f"{self.parameter_path}@", *input_cli_args, *output_cli_args],
            cwd=os.path.dirname(__file__), check=True, capture_output=True
        )
        return result

    def extract_parameter_value(self, parameter_type, parameter_name):
        """Get the value of a parameter from the parameter file"""
        with open(self.parameter_path, 'r') as file_handler:
            line = file_handler.readline()
            current_param_type = ''
            parameter_value = None
            while line:
                for param_type in ParameterType:
                    if param_type.value in line:
                        current_param_type = param_type
                if current_param_type == parameter_type and parameter_name in line:
                    parameter_value = line.split('=')[1].strip()
                line = file_handler.readline()
        return parameter_value

    def get_results(self, working_directory, dataset_file_name):
        """Look for the resulting files after a conversion.
        This is the basic version which looks for folders having the
        same name as the file to convert.
        This method can be overridden in child classes to account for
        the behavior of different conversion configurations.
        This method returns an iterable of paths relative to the
        working directory.
        """
        result_file = ''
        for dir_element in os.listdir(os.path.join(working_directory, self.collection)):
            if dataset_file_name == dir_element:
                result_file = os.path.join(self.collection, dir_element)
                break

        return [result_file]


class PrefixMatchingIDFConverter(IDFConverter):
    """IDF converter which selects the parameter file by checking that
    the datasets' entry_id starts with a particular prefix.
    """

    PARAMETER_FILES = tuple()

    @classmethod
    def get_parameter_file(cls, dataset):
        for parameter_file, prefixes in cls.PARAMETER_FILES:
            for prefix in prefixes:
                if dataset.entry_id.startswith(prefix):
                    return parameter_file
        return None


class Sentinel3IDFConverter(PrefixMatchingIDFConverter):
    """IDF converter for Sentinel-3 datasets"""

    PARAMETER_FILES = (
        ('sentinel3_olci_l1_efr', ('S3A_OL_1_EFR', 'S3B_OL_1_EFR'),),
        ('sentinel3_olci_l2_wfr', ('S3A_OL_2_WFR', 'S3B_OL_2_WFR')),
        ('sentinel3_slstr_l1_bt', ('S3A_SL_1_RBT', 'S3B_SL_1_RBT')),
        ('sentinel3_slstr_l2_wst', ('S3A_SL_2', 'S3B_SL_2')),
    )


class CMEMS001024IDFConverter(PrefixMatchingIDFConverter):
    """IDF converter for CMEMS GLOBAL_ANALYSIS_FORECAST_PHY_001_024
    product.
    """

    PARAMETER_FILES = (
        ('cmems_001_024_hourly_mean_surface', ('mercatorpsy4v3r1_gl12_hrly',)),
    )

    def get_results(self, working_directory, dataset_file_name):
        """The converter configuration used by this class produces
        multiple result folders for one dataset, with time stamps
        comprised in the dataset's time coverage.
        This method looks in the collection folder for folders with a
        time stamp within the dataset's time range.
        """
        file_date = datetime.strptime(
            re.match(r'^.*_([0-9]{8})_.*$', dataset_file_name)[1],
            '%Y%m%d'
        )
        file_time_range = (file_date, file_date + timedelta(days=1))

        result_files = []
        for dir_element in os.listdir(os.path.join(working_directory, self.collection)):
            element_date = datetime.strptime(
                re.match(rf'^{self.collection}_([0-9]{{14}})_.*$', dir_element)[1],
                '%Y%m%d%H%M%S'
            )
            if element_date > file_time_range[0] and element_date < file_time_range[1]:
                result_files.append(os.path.join(self.collection, dir_element))

        return result_files


class IDFConversionManager():
    """IDF converter which uses the idf_converter package from ODL"""

    CONVERTERS = [
        Sentinel3IDFConverter,
        CMEMS001024IDFConverter
    ]

    def __init__(self, working_directory):
        self.working_directory = working_directory

    @classmethod
    def get_converter(cls, dataset_id):
        """Choose a parameter file based on the dataset"""
        dataset = Dataset.objects.get(pk=dataset_id)
        for converter in cls.CONVERTERS:
            parameter_file = converter.get_parameter_file(dataset)
            if parameter_file:
                LOGGER.debug("Using %s for dataset %s", converter, parameter_file)
                return converter(parameter_file)
        raise ConversionError(
            f"Could not find a converter for dataset {dataset_id}")

    def convert(self, dataset_id, file_name):
        """Converts a file to IDF"""
        file_path = os.path.join(self.working_directory, file_name)

        # Unzip the file if necessary
        extract_dir = utils.unarchive(file_path)
        if extract_dir:
            # Set the extracted file as the path to convert
            file_path = os.path.join(extract_dir, os.listdir(extract_dir)[0])

        # Find out the converter to use
        converter = self.get_converter(dataset_id)

        # Convert the file
        try:
            converter.run(file_path, self.working_directory)
        except subprocess.CalledProcessError as error:
            raise ConversionError(
                f"Conversion failed with the following message: {error.stdout}") from error

        # Remove intermediate files
        if extract_dir:
            shutil.rmtree(extract_dir)

        # Find results directory
        return converter.get_results(self.working_directory, os.path.basename(file_path))
