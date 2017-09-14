#!/usr/bin/env python

'''
   Create the table for the BasicDB exercise.

   You only execute this ONCE.
'''

# Standard libraries
import argparse

# Installed libraries
import boto.dynamodb2

# Import unqualified names
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.types import NUMBER

# Defaults
DEF_REGION = "us-west-2"
DEF_READ_CAPACITY = 1
DEF_WRITE_CAPACITY = 1

def get_args():
    argp = argparse.ArgumentParser(
        description="Create table for Assignment 3 of CMPT 474, Summer 2016")
    argp.add_argument("name", help="Name of table to create")
    argp.add_argument("--reads",
                      type=int,
                      default=DEF_READ_CAPACITY,
                      help="Read capacity (default {0})".format(DEF_READ_CAPACITY))
    argp.add_argument("--writes",
                      type=int,
                      default=DEF_WRITE_CAPACITY,
                      help="write capacity (default {0})".format(DEF_WRITE_CAPACITY))
    argp.add_argument("--region",
                      default=DEF_REGION,
                      help="Region (default {0})".format(DEF_REGION))
    return argp.parse_args()

if __name__ == "__main__":
    args = get_args()
    acts = Table.create (
        args.name,
        schema=[
            HashKey('id', data_type=NUMBER)
            ],
        throughput = {
                'read': args.reads,
                'write': args.writes
                },
        connection=boto.dynamodb2.connect_to_region(args.region)
)
