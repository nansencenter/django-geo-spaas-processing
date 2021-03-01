"""Tools for file format conversion"""
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

    def __init__(self, parameter_files):
        self.parameter_paths = [
            os.path.join(self.PARAMETERS_DIR, parameter_file) for parameter_file in parameter_files
        ]
        self.collections = [
            self.extract_parameter_value(parameter_path, ParameterType.OUTPUT, 'collection')
            for parameter_path in self.parameter_paths
        ]

    @classmethod
    def get_parameter_files(cls, dataset):
        """Returns the list of parameter files to use for the
        dataset argument.
        """
        raise NotImplementedError()

    def run(self, in_file, out_dir):
        """Run the IDF converter"""
        input_cli_args = ['-i', 'path', '=', in_file]
        output_cli_args = ['-o', 'path', '=', out_dir]

        results = []
        for parameter_path in self.parameter_paths:
            LOGGER.debug(
                "Converting %s to IDF using parameter file %s", in_file, parameter_path)
            results.append(subprocess.run(
                ['idf-converter', f"{parameter_path}@", *input_cli_args, *output_cli_args],
                cwd=os.path.dirname(__file__), check=True, capture_output=True
            ))
        return results

    @staticmethod
    def extract_parameter_value(parameter_path, parameter_type, parameter_name):
        """Get the value of a parameter from the parameter file"""
        with open(parameter_path, 'r') as file_handler:
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
        This needs to be overridden in child classes to account for
        the behavior of different conversion configurations.
        This method returns an iterable of paths relative to the
        working directory.
        """
        raise NotImplementedError()


class PrefixMatchingIDFConverter(IDFConverter):
    """IDF converter which selects the parameter file by checking that
    the datasets' entry_id starts with a particular prefix.
    """

    PARAMETER_FILES = tuple()

    @classmethod
    def get_parameter_files(cls, dataset):
        for parameter_files, prefixes in cls.PARAMETER_FILES:
            for prefix in prefixes:
                if dataset.entry_id.startswith(prefix):
                    return parameter_files
        return None

    def get_results(self, working_directory, dataset_file_name):
        raise NotImplementedError()


class Sentinel3IDFConverter(PrefixMatchingIDFConverter):
    """IDF converter for Sentinel-3 datasets"""

    PARAMETER_FILES = (
        (('sentinel3_olci_l1_efr',), ('S3A_OL_1_EFR', 'S3B_OL_1_EFR'),),
        (('sentinel3_olci_l2_wfr',), ('S3A_OL_2_WFR', 'S3B_OL_2_WFR')),
        (('sentinel3_slstr_l1_bt',), ('S3A_SL_1_RBT', 'S3B_SL_1_RBT')),
        (('sentinel3_slstr_l2_wst',), ('S3A_SL_2', 'S3B_SL_2')),
    )

    def get_results(self, working_directory, dataset_file_name):
        """Looks for folders having the same name as the file to
        convert
        """
        for dir_element in os.listdir(os.path.join(working_directory, self.collections[0])):
            if dataset_file_name == dir_element:
                return [os.path.join(self.collections[0], dir_element)]
        return []


class SingleResultIDFConverter(PrefixMatchingIDFConverter):
    """IDF converter for CMEMS
    SEALEVEL_GLO_PHY_L4_NRT_OBSERVATIONS_008_046 product
    """
    PARAMETER_FILES = (
        (('cmems_008_046',), ('nrt_global_allsat_phy_l4_',)),
    )

    def get_results(self, working_directory, dataset_file_name):
        """Looks for folders having the same name as the file to
        convert minus the '.nc' extension
        """
        for dir_element in os.listdir(os.path.join(working_directory, self.collections[0])):
            if os.path.splitext(dataset_file_name)[0] == dir_element:
                return [os.path.join(self.collections[0], dir_element)]
        return []


class MultiResultIDFConverter(PrefixMatchingIDFConverter):
    """IDF converter for CMEMS GLOBAL_ANALYSIS_FORECAST_PHY_001_024
    product
    """

    PARAMETER_FILES = (
        (('cmems_001_024_hourly_mean_surface',), ('mercatorpsy4v3r1_gl12_hrly',)),
        (('cmems_001_024_hourly_smoc',), ('SMOC_',)),
        (('cmems_015_003_0m', 'cmems_015_003_15m'), ('dataset-uv-nrt-hourly_',)),
    )

    def get_results(self, working_directory, dataset_file_name):
        """The converter configuration used by this class produces
        multiple result folders for one dataset, with time stamps
        comprised in the dataset's time coverage.
        This method looks in the collection folder for folders with a
        time stamp within the dataset's time range.
        """
        file_date = datetime.strptime(
            re.match(r'^.*_([0-9]{8})(T[0-9]+Z)_.*$', dataset_file_name)[1],
            '%Y%m%d'
        )
        file_time_range = (file_date, file_date + timedelta(days=1))

        result_files = []
        for collection in self.collections:
            for dir_element in os.listdir(os.path.join(working_directory, collection)):
                element_date = datetime.strptime(
                    re.match(rf'^(.*_)?{collection}_([0-9]{{14}})_.*$', dir_element)[2],
                    '%Y%m%d%H%M%S'
                )
                if element_date >= file_time_range[0] and element_date < file_time_range[1]:
                    result_files.append(os.path.join(collection, dir_element))

        return result_files


class IDFConversionManager():
    """IDF converter which uses the idf_converter package from ODL"""

    CONVERTERS = (
        Sentinel3IDFConverter,
        SingleResultIDFConverter,
        MultiResultIDFConverter,
    )

    def __init__(self, working_directory):
        self.working_directory = working_directory

    @classmethod
    def get_converter(cls, dataset_id):
        """Choose a parameter file based on the dataset"""
        dataset = Dataset.objects.get(pk=dataset_id)
        for converter in cls.CONVERTERS:
            parameter_files = converter.get_parameter_files(dataset)
            if parameter_files:
                LOGGER.debug("Using %s for dataset %s", converter, parameter_files)
                return converter(parameter_files)
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
