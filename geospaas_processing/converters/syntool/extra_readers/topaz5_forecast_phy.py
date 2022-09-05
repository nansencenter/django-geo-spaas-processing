"""Reader for TOPAZ 5 PHY forecast data"""
import topaz_utils


def convert(in_file, out_dir):
    """Conversion function for TOPAZ5 PHY forecast"""
    vector_parameters = {
        'current': (('vxo', 'vyo'), 'Sea water velocity', (-2, 2)),
        'sea_ice_velocity': (('vxsi', 'vysi'), 'Sea ice velocity', (-1, 1))
    }
    scalar_parameters = {
        'salinity': ('so', 'Sea water salinity', (0, 40), 'matplotlib_gnuplot'),
        'swt': ('thetao', 'Sea water temperature', (-5, 15), 'matplotlib_gist_rainbow_r'),
    }
    topaz_utils.convert(
        input_path=in_file,
        output_path=out_dir,
        product_base_name='TOPAZ5_forecast_phy',
        resolution=6250,
        vector_parameters=vector_parameters,
        scalar_parameters=scalar_parameters,
    )
