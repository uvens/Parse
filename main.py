import argparse
from Nokia.cm.parse_cm import Nokia_CM
from Erikson.cm.parse_cm import *
from Huawei.pm.parse_pm import Huawei_PM
from Nokia.pm.parse_pm import Nokia_PM
from Erikson.pm.parse_pm import Ericsson_PM


def main():
    parser = argparse.ArgumentParser(description="Example script with command line arguments")

    parser.add_argument("--vendor", required=True, help="Vendor (Ericsson, Huawei, or Nokia)")
    parser.add_argument("--type", required=True, choices=["cm", "pm"], help="Type (cm or pm)")
    parser.add_argument("--path_to_file", required=True, help="The path to the file")
    parser.add_argument("--path_to_directory", required=True, help="Path to directory")

    args = parser.parse_args()

    if args.vendor == "ericsson":
        if args.type == 'cm':
            Ericsson_CM().run(args.path_to_file,args.path_to_directory)
        elif args.type == 'pm':
            Ericsson_PM().run(args.path_to_file,args.path_to_directory)
    elif args.vendor == "nokia":
        if args.type == 'cm':
            Nokia_CM().run(args.path_to_file,args.path_to_directory)
        elif args.type == 'pm':
            Nokia_PM().run(args.path_to_file,args.path_to_directory)
    elif args.vendor == "huawei":
        if args.type == 'pm':
            Huawei_PM().run(args.path_to_file,args.path_to_directory)
    else:
        print("Invalid supplier. Valid values: Ericsson, Huawei, Nokia")

if __name__ == '__main__':
    main()