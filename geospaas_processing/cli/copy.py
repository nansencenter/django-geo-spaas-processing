"""
Copy files that are selected from the database using input criteria from copy config file.
"""
import geospaas_processing.cli.util as util
import geospaas_processing.copiers as copiers


def main():
    """
    Copy the files based on the addressed stored in database to the destination folder.
    The destination folder as well as criteria for finding the desired files are input of this
    function.
    """
    arg = cli_parse_args()
    cumulative_query = util.create_cumulative_query(arg)
    current_copy_action = copiers.Copier(type_in_flag_file=arg.type,
                                        flag_file_request=arg.flag_file,
                                        link_request=arg.link,
                                        destination_path=arg.destination_path,
                                        obsoleteness=int(arg.obsoleteness),
                                        **cumulative_query)
    current_copy_action.copy()
    if not arg.keeping_permanently:
        current_copy_action.delete()


def cli_parse_args():
    """Augment the common parser with additional specific arguments for copying purposes."""
    parser = util.parse_common_args()
    parser.add_argument(
        '-f', '--flag_file', required=False, action='store_true',
        help="The flag that distinguishes between the two cases of 1.writing the flag alongside the"
        + " copying or 2.just copy without writing it based on its ABSENCE or PRESENCE of this flag"
        + " in the arguments.")
    parser.add_argument(
        '-l', '--link', required=False, action='store_true',
        help="The flag that distinguishes between the two cases of 1.copying the file itself or 2."
        + "just copy a symboliclink of it at destination based on its ABSENCE or PRESENCE of this "
        + "flag in the arguments.")
    parser.add_argument(
        '-t', '--type', required=False, type=str,
        help="The type of dataset (as a str) which is written in flag file for further processing.")
    parser.add_argument(
        '-k', '--keeping_permanently', required=False, action='store_true',
        help="The flag that distinguishes between the two cases of 1.deletion of the previously "
        + "copied file(s) after a certain period of time or 2.do nothing regarding deletion based "
        + "on its ABSENCE or PRESENCE of this flag in the arguments.")
    parser.add_argument(
        '-o', '--obsoleteness', required=False, type=str, default="90",
        help="The upper limit of days of file existence that are being copied."
        + " If the file is older than this limit, it will be deleted.")
    return parser.parse_args()


if __name__ == "__main__":
    main()
