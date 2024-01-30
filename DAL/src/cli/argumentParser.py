import argparse


def argument_parser():
    parser = argparse.ArgumentParser(description='Database Query CLI')
    parser.add_argument('-jf', '--json_file', type=str, help='Path to the JSON configuration file. Ex - /path/to/file.json')
    args = parser.parse_args()
    return args
