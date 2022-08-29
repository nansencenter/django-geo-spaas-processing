"""Tools for converting dataset into a format displayable by Syntool"""
import logging
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from ..base import ConversionError, ConversionManager, Converter, \
                   ParameterSelector, NoMatch
from ...ops import crop

logger = logging.getLogger(__name__)


class SyntoolConversionManager(ConversionManager):
    """Manager for Syntoolconversion"""


class SyntoolConverter(Converter):
    """Base class for Syntool converters. Deals with the most common case.
    """

    PARAMETERS_DIR = Path(__file__).parent / 'parameters'

    def convert(self, in_file, out_dir, options):
        """Convert to GeoTIFF using syntool_converter"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            command = ['syntool-converter', *options, '-i', in_file, '-o', tmp_dir]
            try:
                logger.info("Running %s", command)
                process = subprocess.run(
                    command,
                    cwd=Path(__file__).parent, check=True, capture_output=True
                )
            except subprocess.CalledProcessError as error:
                raise ConversionError(
                    f"Conversion failed with the following message: {error.stderr}") from error
            results = self.move_results(tmp_dir, out_dir)
        if not results:
            raise ConversionError((
                "syntool-converter did not produce any file. "
                f"stdout: {process.stdout}"
                f"stderr: {process.stderr}"))
        return results

    def ingest(self, in_file, out_dir, options):
        """Use syntool-ingestor on a converted file (output of
        syntool-converter)
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            command = ['syntool-ingestor', *options, '--output-dir', tmp_dir, in_file]
            try:
                logger.info("Running %s", command)
                process = subprocess.run(
                    command,
                    cwd=Path(__file__).parent, check=True, capture_output=True
                )
            except subprocess.CalledProcessError as error:
                raise ConversionError(
                    f"Ingestion failed with the following message: {error.stderr}") from error
            results = self.move_results(tmp_dir, out_dir)
        if not results:
            raise ConversionError((
                "syntool-ingestor did not produce any file. "
                f"stdout: {process.stdout}"
                f"stderr: {process.stderr}"))
        return results

    def run(self, in_file, out_dir):
        """Runs the whole conversion process"""
        raise NotImplementedError()


@SyntoolConversionManager.register()
class PresetSyntoolConverter(SyntoolConverter):
    """Syntool converter using pre-set configuration files"""

    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: re.match(r'^S3[AB]_OL_2_WFR.*$', d.entry_id),
            convert_parameter_file='convert_sentinel3_olci_l2',
            ingest_parameter_files='ingest_geotiff_4326_tiles'),
        ParameterSelector(
            matches=lambda d: re.match(r'^S3[AB]_SL_1_RBT.*$', d.entry_id),
            convert_parameter_file='convert_sentinel3_slstr_bt_l1',
            ingest_parameter_files='ingest_geotiff_4326_tiles'),
        ParameterSelector(
            matches=lambda d: re.match(r'^.*nersc-MODEL-nextsimf.*$', d.entry_id),
            convert_parameter_file='convert_nextsim',
            ingest_parameter_files=(
                ParameterSelector(
                    matches=lambda p: ('sea_ice_concentration' in str(p) or
                                       'sea_ice_thickness' in str(p) or
                                       'snow_thickness' in str(p)),
                    ingest_file='ingest_geotiff_3413_raster'),
                ParameterSelector(
                    matches=lambda p: 'sea_ice_drift_velocity' in str(p),
                    ingest_file='ingest_geotiff_3413_vectorfield'),
        )),
    )

    def __init__(self, convert_parameter_file, ingest_parameter_files, **kwargs):
        self.convert_parameter_file = convert_parameter_file
        # Should be a string or list of ParameterSelectors
        self.ingest_parameter_files = ingest_parameter_files

    def find_ingest_config(self, converted_file):
        """Find the right ingestion config for a converted file"""
        if isinstance(self.ingest_parameter_files, str):
            return self.ingest_parameter_files
        for selector in self.ingest_parameter_files:
            if selector.matches(converted_file):
                return selector.parameters['ingest_file']
        raise ConversionError("Ingestor not found")

    def run(self, in_file, out_dir):
        """Transforms a file into a Syntool-displayable format using
        the syntool-converter and syntool-ingestor tools
        """
        # syntool-converter
        converted_files = self.convert(
            in_file, out_dir,
            ['--options-file', self.PARAMETERS_DIR / self.convert_parameter_file])

        # syntool-ingestor
        results = []
        for converted_file in converted_files:
            converted_path = Path(out_dir, converted_file)
            results.append(self.ingest(
                converted_path, out_dir, [
                    '--config', Path('parameters', '3413.ini'),
                    '--options-file',
                    self.PARAMETERS_DIR / self.find_ingest_config(converted_path)]))
        return results


@SyntoolConversionManager.register()
class CMEMSL4CurrentSyntoolConverter(PresetSyntoolConverter):
    """Syntool converter for current from CMEMS L4 products"""
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_allsat_phy_l4_'),
            convert_parameter_file='convert_cmems_l4_current',
            ingest_parameter_files='ingest_geotiff_4326_vectorfield'),)

    def __init__(self, convert_parameter_file, ingest_parameter_files, **kwargs):
        self.bounding_box = kwargs.pop('bounding_box', ('-180', '90', '180', '50'))
        super().__init__(convert_parameter_file, ingest_parameter_files)

    def convert(self, in_file, out_dir, options):
        converted_files = super().convert(in_file, out_dir, options)
        for converted_file in converted_files:
            converted_file_path = Path(out_dir, converted_file)
            _, tmp_file = tempfile.mkstemp()
            logger.info("Cropping %s with bounding box %s", converted_file, self.bounding_box)
            crop(converted_file_path, tmp_file, self.bounding_box)
            shutil.move(tmp_file, converted_file_path)
        return converted_files
