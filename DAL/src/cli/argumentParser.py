import argparse


def argument_parser():
    parser = argparse.ArgumentParser(description='Database Query CLI')
    parser.add_argument('--json_file', type=str, help='Path to the JSON configuration file')
    args = parser.parse_args()
    return args
