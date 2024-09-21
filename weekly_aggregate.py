from argparse import ArgumentParser

parser = ArgumentParser(description="date of start | input path| output path| daily path")

parser.add_argument("--execution_date", help="Date of start weekly aggregate")
parser.add_argument("--input_path", help="Folder with .csv files")
parser.add_argument("--output_path", help="Folder to save .csv files that contain weekly aggregate")
parser.add_argument("--daily_path", help="Folder to save .csv files that contain daily aggregate")

args = parser.parse_args()