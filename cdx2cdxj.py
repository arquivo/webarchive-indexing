import argparse
import json
from collections import defaultdict

from pywb.utils.canonicalize import canonicalize


# TODO generate a ordered dictionary (python 3.6 feature)
# TODO adapt field extracting with the number of fields available (diferente formats of cdx)
def transform_cdx(cdx_path):
    with open(cdx_path, mode="r") as cdxfile:
        for line in cdxfile:
            record_dict = defaultdict()
            record_list = line.split(' ')

            # build dict
            record_dict['url'] = record_list[2]
            record_dict['mime'] = record_list[3]
            record_dict['status'] = record_list[4]
            record_dict['digest'] = record_list[5]
            record_dict['length'] = '0'
            record_dict['offset'] = record_list[7]
            record_dict['filename'] = record_list[8].replace('\n', '')
            try:
                print "{} {} {}".format(canonicalize(record_list[0], surt_ordered=True), record_list[1],
                                        json.dumps(record_dict))
            except ValueError as e:
                print "Header"


def main():
    parser = argparse.ArgumentParser(description='Transform CDX to CDXJ format')
    parser.add_argument('cdx', help='the location of the cdx file')

    args = parser.parse_args()
    transform_cdx(args.cdx)


if __name__ == "__main__":
    main()
