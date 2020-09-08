"""Tools for file format conversion"""
import glob
import logging
import os.path
import shutil
import subprocess
from enum import Enum

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
    PARAMETERS_CONDITIONS = {
        "sentinel3_olci_l1_efr": [
            lambda d: d.entry_title.startswith('S3A_OL_1_EFR'),
            lambda d: d.entry_title.startswith('S3B_OL_1_EFR')
        ],
        "sentinel3_olci_l2_wfr": [
            lambda d: d.entry_title.startswith('S3A_OL_2_WFR'),
            lambda d: d.entry_title.startswith('S3B_OL_2_WFR')
        ],
        "sentinel3_slstr_l1_bt": [
            lambda d: d.entry_title.startswith('S3A_SL_1_RBT'),
            lambda d: d.entry_title.startswith('S3B_SL_1_RBT')
        ],
        "sentinel3_slstr_l2_wst": [
            lambda d: d.entry_title.startswith('S3A_SL_2'),
            lambda d: d.entry_title.startswith('S3B_SL_2')
        ]
    }

    def __init__(self, working_directory):
        self.working_directory = working_directory

    @staticmethod
    def run_converter(in_file, out_dir, parameter_file):
        """Runs the IDF converter"""
        input_cli_args = ['-i', 'path', '=', in_file]
        output_cli_args = ['-o', 'path', '=', out_dir]
        LOGGER.debug(
            "Converting %s to IDF using parameter file %s", in_file, parameter_file)
        result = subprocess.run(
            ['idf-converter', f"{parameter_file}@", *input_cli_args, *output_cli_args],
            cwd=os.path.dirname(__file__), check=True, capture_output=True
        )
        return result

    @staticmethod
    def extract_parameter_value(parameter_file, parameter_type, parameter_name):
        """Get the value for a parameter from a parameter file"""
        with open(parameter_file, 'r') as file_handler:
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

    @classmethod
    def choose_parameter_file(cls, dataset_id):
        """Choose a parameter file based on the dataset"""
        dataset = Dataset.objects.get(pk=dataset_id)
        for parameter_file, conditions in cls.PARAMETERS_CONDITIONS.items():
            for condition in conditions:
                if condition(dataset):
                    LOGGER.debug("Using %s for dataset %s", parameter_file, dataset_id)
                    return os.path.join(cls.PARAMETERS_DIR, parameter_file)
        raise ConversionError(
            f"Could not find a conversion parameters file for dataset {dataset_id}")

    def get_dataset_files(self, dataset_id):
        """Find files corresponding to the dataset in the working directory"""
        return glob.glob(os.path.join(self.working_directory, f"dataset_{dataset_id}*"))

    def convert(self, dataset_id, file_name=None):
        """Converts a file to IDF"""
        file_path = (os.path.join(self.working_directory, file_name) if file_name
                     else self.get_dataset_files(dataset_id)[0])

        # Unzip the file if necessary
        extract_dir = utils.unarchive(file_path)
        if extract_dir:
            # Set the extracted file as the path to convert
            file_path = os.path.join(extract_dir, os.listdir(extract_dir)[0])

        # Find out the parameters file to use
        parameter_file = self.choose_parameter_file(dataset_id)

        # Get the collection name, which is the directory where the conversion results will be
        collection = self.extract_parameter_value(
            os.path.join(self.PARAMETERS_DIR, parameter_file),
            ParameterType.OUTPUT,
            'collection'
        )

        # Convert the file
        try:
            self.run_converter(file_path, self.working_directory, parameter_file)
        except subprocess.CalledProcessError as error:
            raise ConversionError(
                f"Conversion failed with the following message: {error.stdout}") from error

        # Remove intermediate files
        if extract_dir:
            shutil.rmtree(extract_dir)

        # Find results directory
        result_file = ''
        for dir_element in os.listdir(os.path.join(self.working_directory, collection)):
            if os.path.basename(file_path) in dir_element:
                # This is destined to be used in a URL so we don't use os.path.join()
                result_file = f"{collection}/{dir_element}"
                break

        return result_file
