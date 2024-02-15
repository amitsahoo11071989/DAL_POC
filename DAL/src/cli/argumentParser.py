import argparse
import sys


class InputParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Database Query CLI")
        self.parser.add_argument(
            "-jf",
            "--json_file",
            type=str,
            help='''Path to the JSON configuration file. 
                                 Ex - /path/to/file.json. 
                                 Example: python main.py -jf "<path of JSON configuration file>"''',
        )

    def argument_parser(self):
        """
        Parses command-line arguments

        Returns:
            argparse.Namespace: An object containing parsed command-line arguments.
        """
        # TODO uncomment these lines before demo
        # if (len(sys.argv)<3) or (len(sys.argv)!=3) or (sys.argv[1] not in ['-jf', '--json_file']):
        #     self.parser._print_message("\nCommand entered is incorrent. Refer the help section \n")
        #     self.parser.print_help(sys.stderr)
        #     sys.exit(0)
        # else:
        return self.parser.parse_args()
