"""Tools for file format conversion"""
import logging
import os
import os.path
import re
import shutil
import subprocess
import tempfile
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

        # Convert the file(s)
        results = converter.run(file_path, self.working_directory)

        # Remove intermediate files
        if extract_dir:
            shutil.rmtree(extract_dir)

        return results


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

    def run(self, in_file, out_dir):
        """Run the IDF converter"""
        input_cli_args = ['-i', 'path', '=', in_file]

        for parameter_path in self.parameter_paths:
            LOGGER.debug(
                "Converting %s to IDF using parameter file %s", in_file, parameter_path)

            with tempfile.TemporaryDirectory() as tmp_dir:
                output_cli_args = ['-o', 'path', '=', tmp_dir]

                try:
                    # run the idf-converter tool. The output is in a temporary directory
                    process = subprocess.run(
                        ['idf-converter', f"{parameter_path}@", *input_cli_args, *output_cli_args],
                        cwd=os.path.dirname(__file__), check=True, capture_output=True
                    )
                except subprocess.CalledProcessError as error:
                    raise ConversionError(
                        f"Conversion failed with the following message: {error.stderr}") from error

                # if the file was skipped, raise an exception
                stderr = str(process.stderr)
                if 'Skipping this file' in stderr:
                    raise ConversionError((
                        f"Could not convert {os.path.basename(in_file)}\n{stderr}: "
                        "the file was skipped the idf-converter"))

                # at this point it is safe to assume that the
                # conversion went well. We move the results to
                # the permanent output directory
                return self.move_results(tmp_dir, out_dir)

    def move_results(self, tmp_output_directory, permanent_output_directory):
        """Move the collection folders from the temporary directory to
        the permanent results directory and return the paths to the
        result folders located inside the collection folders.
        The paths are given relative to the permanent output directory.
        """
        results = []
        for collection in os.listdir(tmp_output_directory):
            tmp_collection_dir = os.path.join(tmp_output_directory, collection)
            permanent_collection_dir = os.path.join(permanent_output_directory, collection)

            os.makedirs(permanent_collection_dir, exist_ok=True)
            for result_dir in os.listdir(tmp_collection_dir):
                result_path = os.path.join(collection, result_dir)
                tmp_result_path = os.path.join(tmp_output_directory, result_path)
                results.append(result_path)

                LOGGER.debug("Moving %s to %s", tmp_result_path, permanent_collection_dir)
                try:
                    shutil.move(tmp_result_path, permanent_collection_dir)
                except shutil.Error as error:
                    # if the directory already exists, we remove it and
                    # retry to move the result file
                    if 'already exists' in str(error):
                        existing_dir = os.path.join(permanent_collection_dir, result_dir)
                        LOGGER.info("%s already exists, removing it and retrying", existing_dir)
                        if os.path.isdir(existing_dir):
                            shutil.rmtree(existing_dir)
                        elif os.path.isfile(existing_dir):
                            os.remove(existing_dir)
                        else:
                            raise
                        shutil.move(tmp_result_path, permanent_collection_dir)
                    else:
                        raise

        return results


class MultiFilesIDFConverter(IDFConverter):
    """Base class for converters which need to run the conversion on
    multiple files
    """

    @staticmethod
    def list_files_to_convert(dataset_file_path):
        """Returns the list of dataset paths on which the converter
        needs to be called
        """
        raise NotImplementedError

    def run(self, in_file, out_dir):
        """calls the IDFConverter.run() method on all dataset files
        contained returned by list_files_to_convert()
        """
        subdatasets = self.list_files_to_convert(in_file)
        if not subdatasets:
            raise ConversionError(f"The 'measurement' directory of {in_file} is empty")

        results = []
        for dataset_file in subdatasets:
            for result in super().run(dataset_file, out_dir):
                results.append(result)
        return results


@IDFConversionManager.register()
class Sentinel1IDFConverter(MultiFilesIDFConverter):
    """IDF converter for Sentinel-1 datasets"""

    PARAMETER_FILES = (
        (('sentinel1_l2_rvl',), lambda d: re.match('^S1[AB]_[A-Z0-9]{2}_OCN.*$', d.entry_id)),
    )

    @staticmethod
    def list_files_to_convert(dataset_file_path):
        """Returns the path to the 'measurement' directory of the
        dataset
        """
        measurement_dir = os.path.join(dataset_file_path, 'measurement')
        try:
            return [
                os.path.join(measurement_dir, path)
                for path in os.listdir(measurement_dir)
            ]
        except (FileNotFoundError, NotADirectoryError) as error:
            raise ConversionError(
                f"Could not find a measurement directory inside {dataset_file_path}") from error


@IDFConversionManager.register()
class Sentinel3SLSTRL2WSTIDFConverter(MultiFilesIDFConverter):
    """IDF converter for Sentinel 3 SLSTR L2 WST datasets"""

    PARAMETER_FILES = (
        (('sentinel3_slstr_l2_wst',), lambda d: re.match('^S3[AB]_SL_2_WST.*$', d.entry_id)),
    )

    @staticmethod
    def list_files_to_convert(dataset_file_path):
        try:
            return [
                os.path.join(dataset_file_path, path)
                for path in os.listdir(dataset_file_path)
                if path.endswith('.nc')
            ]
        except (FileNotFoundError, NotADirectoryError) as error:
            raise ConversionError(
                f"Could not find any dataset files in {dataset_file_path}") from error


@IDFConversionManager.register()
class Sentinel3IDFConverter(IDFConverter):
    """IDF converter for Sentinel-3 datasets"""

    PARAMETER_FILES = (
        (('sentinel3_olci_l1_efr',), lambda d: re.match('^S3[AB]_OL_1_EFR.*$', d.entry_id)),
        (('sentinel3_olci_l2_wfr',), lambda d: re.match('^S3[AB]_OL_2_WFR.*$', d.entry_id)),
        (('sentinel3_slstr_l1_bt',), lambda d: re.match('^S3[AB]_SL_1_RBT.*$', d.entry_id)),
    )


@IDFConversionManager.register()
class SingleResultIDFConverter(IDFConverter):
    """IDF converter for readers which produce a single output folder
    """
    PARAMETER_FILES = (
        (('cmems_008_046',),
         lambda d: d.entry_id.startswith('nrt_global_allsat_phy_l4_')),
        (('cmems_013_048_drifter_0m', 'cmems_013_048_drifter_15m'),
         lambda d: d.entry_id.startswith('GL_TS_DC_')),
        (('esa_cci_sst',),
         lambda d: re.match(
             '^D[0-9]{3}-ESACCI-L4_GHRSST-SSTdepth-OSTIA-GLOB_CDR2\.1-v02\.0-fv01\.0$',
             d.entry_id)),
        (('ghrsst_l2p_modis_a_day',),
         lambda d: d.entry_id.endswith('-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0')),
        (('ghrsst_l2p_modis_a_night',),
         lambda d: d.entry_id.endswith('-JPL-L2P_GHRSST-SSTskin-MODIS_A-N-v02.0-fv01.0')),
        (('ghrsst_l2p_viirs_jpl_sst',), lambda d: '-JPL-L2P_GHRSST-SSTskin-VIIRS' in d.entry_id),
        (('ghrsst_l2p_viirs_navo_sst',), lambda d: '-NAVO-L2P_GHRSST-SST1m-VIIRS' in d.entry_id),
        (('ghrsst_l2p_viirs_ospo_sst',),
         lambda d: 'OSPO-L2P_GHRSST-SSTsubskin-VIIRS' in d.entry_id),
        (('ghrsst_l3c_avhrr_metop_b_sst',),
         lambda d: '-OSISAF-L3C_GHRSST-SSTsubskin-AVHRR_SST_METOP_B_GLB-' in d.entry_id),
        (('ghrsst_l3c_seviri_atlantic_sst',),
         lambda d: '-OSISAF-L3C_GHRSST-SSTsubskin-SEVIRI_SST-' in d.entry_id),
        (('ghrsst_l3c_seviri_indian_sst',),
         lambda d: '-OSISAF-L3C_GHRSST-SSTsubskin-SEVIRI_IO_SST-' in d.entry_id),
        (('hycom_osu',),
         lambda d: d.entry_id.startswith('hycom_glb_sfc_u_')),
        (('rtofs_diagnostic',),
         lambda d: '/rtofs_glo_2ds_' in d.entry_id and d.entry_id.endswith('_diag')),
        (('rtofs_prognostic',),
         lambda d: '/rtofs_glo_2ds_' in d.entry_id and d.entry_id.endswith('_prog')),
    )


@IDFConversionManager.register()
class MultiResultFoldersIDFConverter(IDFConverter):
    """IDF converter for CMEMS readers which produce multiple result
    folders from one dataset file
    """

    PARAMETER_FILES = (
        (('cmems_001_024_hourly_mean_surface',),
         lambda d: d.entry_id.startswith('mercatorpsy4v3r1_gl12_hrly')),
        (('cmems_001_024_hourly_smoc',),
         lambda d: d.entry_id.startswith('SMOC_')),
        (('cmems_015_003_0m', 'cmems_015_003_15m'),
         lambda d: d.entry_id.startswith('dataset-uv-nrt-hourly_')),
        (('cmems_013_048_radar_total',),
         lambda d: d.entry_id.startswith('GL_TV_HF_')),
        (('ghrsst_l3u_amsr2_sst',),
         lambda d: '-REMSS-L3U_GHRSST-SSTsubskin-AMSR2-' in d.entry_id),
        (('ghrsst_l3u_gmi_sst',),
         lambda d: '-REMSS-L3U_GHRSST-SSTsubskin-GMI-' in d.entry_id),
        (('ibi_hourly_mean_surface',),
         lambda d: 'CMEMS_v5r1_IBI_PHY_NRT_PdE_01hav_' in d.entry_id),
        (('mfs_med-cmcc-cur',),
         lambda d: '_hts-CMCC--RFVL-MFSeas6-MEDATL-' in d.entry_id),
        (('mfs_med-cmcc-ssh',),
         lambda d: '_hts-CMCC--ASLV-MFSeas6-MEDATL-' in d.entry_id),
        (('mfs_med-cmcc-temp',),
         lambda d: '_hts-CMCC--TEMP-MFSeas6-MEDATL-' in d.entry_id),
    )
