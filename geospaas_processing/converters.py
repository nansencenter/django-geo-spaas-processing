"""Tools for file format conversion"""
import logging
import os
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


class IDFConversionManager():
    """Chooses the right IDF converter class to manage the conversion
    to IDF of a dataset file. This basically implements the Factory
    design pattern with auto-registration.
    """

    converters = {}

    def __init__(self, working_directory):
        self.working_directory = working_directory

    @classmethod
    def register(cls):
        """Decorator which adds the decorated IDF converter class and
        its parameter files configuration to the dict of the
        IDFConversionManager
        """
        def inner_wrapper(wrapped_class):
            cls.converters[wrapped_class] = wrapped_class.PARAMETER_FILES
            return wrapped_class
        return inner_wrapper

    @staticmethod
    def get_parameter_files(parameter_files_conditions, dataset):
        """Returns the list of parameter files to use for the
        dataset given as argument
        """
        for parameter_files, matches in parameter_files_conditions:
            if matches(dataset):
                return parameter_files
        return None

    @classmethod
    def get_converter(cls, dataset_id):
        """Chooses a converter class and parameter file based on the
        dataset
        """
        dataset = Dataset.objects.get(pk=dataset_id)
        for converter, parameter_files_conditions in cls.converters.items():
            parameter_files = cls.get_parameter_files(parameter_files_conditions, dataset)
            if parameter_files:
                return converter(parameter_files)
        raise ConversionError(f"Could not find a converter for dataset {dataset_id}")

    def convert(self, dataset_id, file_name):
        """Converts a file to IDF using the right converter class"""
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
                f"Conversion failed with the following message: {error.stderr}") from error

        # Remove intermediate files
        if extract_dir:
            shutil.rmtree(extract_dir)

        # Find results directory
        return converter.get_results(self.working_directory, os.path.basename(file_path))


class IDFConverter():
    """Base class for IDF converters. Uses the idf_converter package
    from ODL for the actual conversion. The child classes deal with
    the configuration files and gathering the results
    """
    PARAMETERS_DIR = os.path.join(os.path.dirname(__file__), 'parameters')
    PARAMETER_FILES = tuple()

    def __init__(self, parameter_files):
        self.parameter_paths = [
            os.path.join(self.PARAMETERS_DIR, parameter_file) for parameter_file in parameter_files
        ]
        self.collections = [
            self.extract_parameter_value(parameter_path, ParameterType.OUTPUT, 'collection')
            for parameter_path in self.parameter_paths
        ]

    def run(self, in_file, out_dir):
        """Run the IDF converter"""
        input_cli_args = ['-i', 'path', '=', in_file]
        output_cli_args = ['-o', 'path', '=', out_dir]

        completed_processes = []
        for parameter_path in self.parameter_paths:
            LOGGER.debug(
                "Converting %s to IDF using parameter file %s", in_file, parameter_path)
            completed_processes.append(subprocess.run(
                ['idf-converter', f"{parameter_path}@", *input_cli_args, *output_cli_args],
                cwd=os.path.dirname(__file__), check=True, capture_output=True
            ))

        for process in completed_processes:
            stderr = str(process.stderr)
            if 'Skipping this file.' in stderr:
                raise ConversionError(
                    f"Could not convert {os.path.basename(in_file)}\n{stderr}")

        return completed_processes

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
        This method returns an iterable of paths relative to the
        working directory.
        """
        results = []
        for collection in self.collections:
            collection_dir = os.path.join(working_directory, collection)
            for directory in os.listdir(collection_dir):
                if self.matches_result(collection, dataset_file_name, directory):
                    results.append(os.path.join(collection, directory))
        return results

    def matches_result(self, collection, dataset_file_name, directory):
        """Checks whether a directory is a result of the current
        conversion. This needs to be overridden in child classes to
        account for the behavior of different conversion configurations
        """
        raise NotImplementedError


@IDFConversionManager.register()
class Sentinel1IDFConverter(IDFConverter):
    """IDF converter for Sentinel-1 datasets"""

    PARAMETER_FILES = (
        (('sentinel1_l2_rvl',), lambda d: re.match('^S1[AB]_IW_OCN.*$', d.entry_id)),
    )

    def run(self, in_file, out_dir):
        """calls the IDFConverter.run() method on all dataset files
        contained in the "measurement" folder located in the `in_file`
        directory
        """
        measurement_dir = os.path.join(in_file, 'measurement')
        results = []
        if os.path.isdir(measurement_dir):
            for dataset_file in os.listdir(measurement_dir):
                results.append(super().run(os.path.join(measurement_dir, dataset_file), out_dir))
        else:
            raise ConversionError(f"Could not find a measurement directory inside {in_file}")

        if results:
            return results
        else:
            raise ConversionError(f"Could not find a file to convert in {measurement_dir}")

    def matches_result(self, collection, dataset_file_name, directory):
        """Returns True if the directory name contains the dataset's
        identifier
        """
        try:
            dataset_file_identifier = re.match(
                r'^S1.*_(([0-9]{8}T[0-9]{6}_){2}[0-9]{6}_[0-9]{6})_[0-9]{4}\.SAFE$',
                dataset_file_name
            )[1]
        except TypeError as error:
            raise ConversionError(
                f"Could not extract the dataset's identifier from {dataset_file_name}") from error

        result_identifier = dataset_file_identifier.lower().translate(str.maketrans('_', '-'))

        return result_identifier in directory


@IDFConversionManager.register()
class Sentinel3IDFConverter(IDFConverter):
    """IDF converter for Sentinel-3 datasets"""

    PARAMETER_FILES = (
        (('sentinel3_olci_l1_efr',), lambda d: re.match('^S3[AB]_OL_1_EFR.*$', d.entry_id)),
        (('sentinel3_olci_l2_wfr',), lambda d: re.match('^S3[AB]_OL_2_WFR.*$', d.entry_id)),
        (('sentinel3_slstr_l1_bt',), lambda d: re.match('^S3[AB]_SL_1_RBT.*$', d.entry_id)),
        (('sentinel3_slstr_l2_wst',), lambda d: re.match('^S3[AB]_SL_2.*$', d.entry_id)),
    )

    def matches_result(self, collection, dataset_file_name, directory):
        return dataset_file_name == directory


@IDFConversionManager.register()
class SingleResultIDFConverter(IDFConverter):
    """IDF converter for readers which produce a single output folder
    """
    PARAMETER_FILES = (
        (('cmems_008_046',),
         lambda d: d.entry_id.startswith('nrt_global_allsat_phy_l4_')),
        (('esa_cci_sst',),
         lambda d: re.match(
             '^D[0-9]{3}-ESACCI-L4_GHRSST-SSTdepth-OSTIA-GLOB_CDR2\.1-v02\.0-fv01\.0$',
             d.entry_id)),
        (('ghrsst_l2p_modis_a_jpl_day',),
         lambda d: re.match(r'^.*-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02\.0-fv01\.0$', d.entry_id)),
        (('ghrsst_l2p_modis_a_jpl_night',),
         lambda d: re.match(r'^.*-JPL-L2P_GHRSST-SSTskin-MODIS_A-N-v02\.0-fv01\.0$', d.entry_id)),
        (('ghrsst_l2p_viirs_jpl',),
         lambda d: re.match(
             r'^.*-JPL-L2P_GHRSST-SSTskin-VIIRS_NPP-[DN]-v02\.0-fv01\.0$', d.entry_id)),
        (('ghrsst_l2p_viirs_navo',),
         lambda d: re.match(r'^.*-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02\.0-fv0[13]\.0$', d.entry_id)),
        (('ghrsst_l2p_viirs_ospo',),
         lambda d: re.match(
             r'^.*-OSPO-L2P_GHRSST-SSTsubskin-VIIRS_NPP-ACSPO_V2\.61-v02\.0-fv01\.0$', d.entry_id)),
    )

    def matches_result(self, collection, dataset_file_name, directory):
        """Returns True if the directory has the same name as the file
        to convert minus the extension
        """
        return os.path.splitext(dataset_file_name)[0] == directory


@IDFConversionManager.register()
class CMEMSMultiResultIDFConverter(IDFConverter):
    """IDF converter for CMEMS readers which produce multiple result
    folders
    """

    PARAMETER_FILES = (
        (('cmems_001_024_hourly_mean_surface',),
         lambda d: d.entry_id.startswith('mercatorpsy4v3r1_gl12_hrly')),
        (('cmems_001_024_hourly_smoc',),
         lambda d: d.entry_id.startswith('SMOC_')),
        (('cmems_015_003_0m', 'cmems_015_003_15m'),
         lambda d: d.entry_id.startswith('dataset-uv-nrt-hourly_')),
    )

    @staticmethod
    def extract_date(file_name, regex, parse_pattern):
        """Extracts a date string from a file name using a regular
        expression, then parses this string using the `parse_pattern`
        and returns a datetime object.
        The date part in the regular expression should be a group
        named "date".
        """
        try:
            return datetime.strptime(re.match(regex, file_name).group('date'),parse_pattern)
        except (AttributeError, IndexError, ValueError) as error:
            raise ConversionError(f"Could not extract date from {file_name}") from error

    def matches_result(self, collection, dataset_file_name, directory):
        file_date = self.extract_date(
            dataset_file_name,
            r'^.*_(?P<date>[0-9]{8})(T[0-9]+Z)?_.*$',
            '%Y%m%d'
        )
        file_time_range = (file_date, file_date + timedelta(days=1))

        directory_date = self.extract_date(
            directory,
            rf'^(.*_)?{collection}_(?P<date>[0-9]{{14}})_.*$',
            '%Y%m%d%H%M%S'
        )

        return directory_date >= file_time_range[0] and directory_date < file_time_range[1]
