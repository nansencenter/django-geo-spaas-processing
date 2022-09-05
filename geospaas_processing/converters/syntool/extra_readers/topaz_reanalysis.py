"""Reader for TOPAZ 4 reanalysis data"""
import topaz_utils


def convert(in_file, out_dir):
    """Conversion function for TOPAZ4 reanalysis"""
    vector_parameters = {
        'current': (('vxo', 'vyo'), 'Sea water velocity', (-2, 2)),
        # 'sea_ice_velocity': (('vxsi', 'vysi'), 'Sea ice velocity', (-1, 1))
    }
    scalar_parameters = {
        # 'sea_water_potential_temperature_at_sea_floor': 'bottomT',
        # 'ocean_mixed_layer_thickness': 'mlotst',
        # 'sea_ice_concentration': 'siconc',
        # 'snow_thickness': 'sisnthick',
        # 'sea_ice_thickness': 'sithick',
        # 'salinity': ('so', 'Sea water salinity', (0, 40), 'matplotlib_gnuplot'),
        'swt': ('thetao', 'Sea water temperature', (-5, 25), 'matplotlib_gist_rainbow_r'),
        # 'sea_surface_height': 'zos',
    }
    topaz_utils.convert_data(
        input_path=in_file,
        output_path=out_dir,
        product_base_name='TOPAZ_reanalysis',
        resolution=12500,
        vector_parameters=vector_parameters,
        scalar_parameters=scalar_parameters,
    )
