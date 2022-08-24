"""Tools for dataset conversion to IDF"""
import ftplib
import logging
import os
import os.path
import re
import shutil
import subprocess
import tarfile
import tempfile
from contextlib import closing

from ..base import ConversionError, ConversionManager, Converter, ParameterSelector

AUXILIARY_PATH = os.path.join(os.path.dirname(__file__), 'auxiliary')
logger = logging.getLogger(__name__)


def download_auxiliary_files(auxiliary_path):
    """Download the auxiliary files necessary for IDF conversion.
    They are too big to be included in the package.
    """
    if not os.path.isdir(auxiliary_path):
        logger.info("Downloading auxiliary files for IDF conversion, this may take a while")
        os.makedirs(auxiliary_path)
        try:
            with closing(ftplib.FTP('ftp.nersc.no')) as ftp:
                ftp.login()
                # we write the archive to a tmp file...
                with tempfile.TemporaryFile() as tmp_file:
                    ftp.retrbinary('RETR /pub/Adrien/idf_converter_auxiliary.tar', tmp_file.write)
                    # ...then set the cursor back at the beginning of
                    # the file...
                    tmp_file.seek(0)
                    # ...and finally extract the contents of the
                    # archive to the auxiliary folder
                    with tarfile.TarFile(fileobj=tmp_file) as tar_file:
                        tar_file.extractall(auxiliary_path)
        except (*ftplib.all_errors, tarfile.ExtractError):
            # in case of error, we just remove everything
            shutil.rmtree(auxiliary_path)
            raise

download_auxiliary_files(AUXILIARY_PATH)


class IDFConversionManager(ConversionManager):
    """Manager for IDF conversion"""


class IDFConverter(Converter):
    """Base class for IDF converters. Uses the idf_converter package
    from ODL for the actual conversion. The child classes deal with
    the configuration files and gathering the results
    """
    PARAMETERS_DIR = os.path.join(os.path.dirname(__file__), 'parameters')
    PARAMETER_SELECTORS = (ParameterSelector(matches=lambda d: False, parameter_files=tuple()),)

    def __init__(self, parameter_files):
        self.parameter_paths = [
            os.path.join(self.PARAMETERS_DIR, parameter_file) for parameter_file in parameter_files
        ]

    def run(self, in_file, out_dir):
        """Run the IDF converter"""
        input_cli_args = ['-i', 'path', '=', in_file]

        results = []
        for parameter_path in self.parameter_paths:
            logger.debug(
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
                results.extend(self.move_results(tmp_dir, out_dir))
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
            raise ConversionError(f"No files to convert were found for {in_file}")

        results = []
        for dataset_file in subdatasets:
            for result in super().run(dataset_file, out_dir):
                results.append(result)
        return results


@IDFConversionManager.register()
class Sentinel1IDFConverter(MultiFilesIDFConverter):
    """IDF converter for Sentinel-1 datasets"""

    PARAMETER_SELECTORS = (
        ParameterSelector(matches=lambda d: re.match('^S1[AB]_[A-Z0-9]{2}_OCN.*$', d.entry_id),
                          parameter_files=('sentinel1_l2_rvl',)),
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

    PARAMETER_SELECTORS = (
        ParameterSelector(matches=lambda d: re.match('^S3[AB]_SL_2_WST.*$', d.entry_id),
                          parameter_files=('sentinel3_slstr_l2_wst',)),
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
class SingleResultIDFConverter(IDFConverter):
    """IDF converter for readers which produce a single output folder
    """
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_allsat_phy_l4_'),
            parameter_files=('cmems_008_046',)),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('GL_TS_DC_'),
            parameter_files=('cmems_013_048_drifter_0m', 'cmems_013_048_drifter_15m')),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('GL_TS_DB_'),
            parameter_files=('cmems_013_030_drifter_0m', 'cmems_013_030_drifter_15m')),
        ParameterSelector(
            matches=lambda d: re.match(
                '^D[0-9]{3}-ESACCI-L4_GHRSST-SSTdepth-OSTIA-GLOB_CDR2\.1-v02\.0-fv01\.0$',
                d.entry_id),
            parameter_files=('esa_cci_sst',)),
        ParameterSelector(
            matches=lambda d: d.entry_id.endswith('-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0'),
            parameter_files=('ghrsst_l2p_modis_a_day',)),
        ParameterSelector(
            matches=lambda d: d.entry_id.endswith('-JPL-L2P_GHRSST-SSTskin-MODIS_A-N-v02.0-fv01.0'),
            parameter_files=('ghrsst_l2p_modis_a_night',)),
        ParameterSelector(
            matches=lambda d: '-JPL-L2P_GHRSST-SSTskin-VIIRS' in d.entry_id,
            parameter_files=('ghrsst_l2p_viirs_jpl_sst',)),
        ParameterSelector(
            matches=lambda d: '-NAVO-L2P_GHRSST-SST1m-VIIRS' in d.entry_id,
            parameter_files=('ghrsst_l2p_viirs_navo_sst',)),
        ParameterSelector(
            matches=lambda d: 'OSPO-L2P_GHRSST-SSTsubskin-VIIRS' in d.entry_id,
            parameter_files=('ghrsst_l2p_viirs_ospo_sst',)),
        ParameterSelector(
            matches=lambda d: '-OSISAF-L3C_GHRSST-SSTsubskin-AVHRR_SST_METOP_B_GLB-' in d.entry_id,
            parameter_files=('ghrsst_l3c_avhrr_metop_b_sst',)),
        ParameterSelector(
            matches=lambda d: '-STAR-L3C_GHRSST-SSTsubskin-ABI_G16-' in d.entry_id,
            parameter_files=('ghrsst_l3c_goes16_sst',)),
        ParameterSelector(
            matches=lambda d: '-STAR-L3C_GHRSST-SSTsubskin-ABI_G17-' in d.entry_id,
            parameter_files=('ghrsst_l3c_goes17_sst',)),
        ParameterSelector(
            matches=lambda d: '-OSISAF-L3C_GHRSST-SSTsubskin-SEVIRI_SST-' in d.entry_id,
            parameter_files=('ghrsst_l3c_seviri_atlantic_sst',)),
        ParameterSelector(
            matches=lambda d: '-OSISAF-L3C_GHRSST-SSTsubskin-SEVIRI_IO_SST-' in d.entry_id,
            parameter_files=('ghrsst_l3c_seviri_indian_sst',)),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('hycom_glb_sfc_u_'),
            parameter_files=('hycom_osu',)),
        ParameterSelector(
            matches=lambda d: '/rtofs_glo_2ds_' in d.entry_id and d.entry_id.endswith('_diag'),
            parameter_files=('rtofs_diagnostic',)),
        ParameterSelector(
            matches=lambda d: '/rtofs_glo_2ds_' in d.entry_id and d.entry_id.endswith('_prog'),
            parameter_files=('rtofs_prognostic',)),
        ParameterSelector(
            matches=lambda d: re.match('^S3[AB]_OL_1_EFR.*$', d.entry_id),
            parameter_files=('sentinel3_olci_l1_efr',)),
        ParameterSelector(
            matches=lambda d: re.match('^S3[AB]_OL_2_WFR.*$', d.entry_id),
            parameter_files=('sentinel3_olci_chl',)),
        ParameterSelector(
            matches=lambda d: re.match('^S3[AB]_SL_1_RBT.*$', d.entry_id),
            parameter_files=('sentinel3_slstr_sst',)),
    )


@IDFConversionManager.register()
class MultiResultFoldersIDFConverter(IDFConverter):
    """IDF converter for CMEMS readers which produce multiple result
    folders from one dataset file
    """

    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('mercatorpsy4v3r1_gl12_hrly'),
            parameter_files=('cmems_001_024_hourly_mean_surface',)),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('SMOC_'),
            parameter_files=('cmems_001_024_hourly_smoc',)),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('dataset-uv-nrt-hourly_'),
            parameter_files=('cmems_015_003_0m', 'cmems_015_003_15m')),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('GL_TV_HF_'),
            parameter_files=('cmems_013_048_radar_total',)),
        ParameterSelector(
            matches=lambda d: '-REMSS-L3U_GHRSST-SSTsubskin-AMSR2-' in d.entry_id,
            parameter_files=('ghrsst_l3u_amsr2_sst',)),
        ParameterSelector(
            matches=lambda d: '-REMSS-L3U_GHRSST-SSTsubskin-GMI-' in d.entry_id,
            parameter_files=('ghrsst_l3u_gmi_sst',)),
        ParameterSelector(
            matches=lambda d: 'CMEMS_v5r1_IBI_PHY_NRT_PdE_01hav_' in d.entry_id,
            parameter_files=('ibi_hourly_mean_surface',)),
        ParameterSelector(
            matches=lambda d: '_hts-CMCC--RFVL-MFSeas6-MEDATL-' in d.entry_id,
            parameter_files=('mfs_med-cmcc-cur',)),
        ParameterSelector(
            matches=lambda d: '_hts-CMCC--ASLV-MFSeas6-MEDATL-' in d.entry_id,
            parameter_files=('mfs_med-cmcc-ssh',)),
        ParameterSelector(
            matches=lambda d: '_hts-CMCC--TEMP-MFSeas6-MEDATL-' in d.entry_id,
            parameter_files=('mfs_med-cmcc-temp',)),
    )
