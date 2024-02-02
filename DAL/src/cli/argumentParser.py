import argparse
import sys


def argument_parser():
    parser = argparse.ArgumentParser(description='Database Query CLI')
    parser.add_argument('-jf', '--json_file', type=str, help='Path to the JSON configuration file. Ex - /path/to/file.json. Example: python main.py -jf "<path of JSON configuration file>"')
    # if (len(sys.argv)<3) or (len(sys.argv)!=3) or (sys.argv[1] not in ['-jf', '--json_file']):
    #     parser.print_help(sys.stderr)
    #     sys.exit(0)
    args = parser.parse_args()
    return args
