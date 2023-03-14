"""Reader for TOPAZ 5 BGC forecast data"""
import topaz_utils


def convert(in_file, out_dir):
    """Conversion function for TOPAZ5 BGC forecast"""
    vector_parameters = {}
    scalar_parameters = {
        'chlorophyll': ('chl', 'Mass concentration of chlorophyll-a', (0, 10), 'chla_jet'),
        'oxygen': (
            'o2', 'Mole concentration of dissolved oxygen', (250, 475), 'matplotlib_gnuplot'),
    }
    topaz_utils.convert(
        input_path=in_file,
        output_path=out_dir,
        product_base_name='TOPAZ5_forecast_bgc',
        resolution=6250,
        vector_parameters=vector_parameters,
        scalar_parameters=scalar_parameters,
    )
