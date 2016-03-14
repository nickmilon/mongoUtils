#!/usr/bin/env python
"""
a command line tools
    - tail simulates tail -f  command to tail documents coming in a mongoDB collection
    - import imports worksheet o workbook into a db


:Example:
    >>> python -m mongoUtils.tools.command import --connection "mongodb://localhost:27017/test"
        --filepath "xxxxx/foo.xlsx" --db test
    >>> python -m mongoUtils.tools.command tail --connection "mongodb://localhost:27017/admin"
         --db test --collection 'foo'  --values_only
    >>> python -m  mongoUtils.tools.command tail --connection "mongodb://localhost:27017/foo"
        --db local --collection 'oplog.rs' --track_field 'ts' (op log track)

"""

import argparse
from mongoUtils.pubsub import Sub
from mongoUtils.client import muClient


def parse_args():
    con_arg = {'type': str, 'default': "mongodb://localhost/test",
               'help': 'a db connection string\
                as defined in http://docs.mongodb.org/manual/reference/connection-string/\
                that includes a database name for authentication'
               }
    parser = argparse.ArgumentParser(prog='tools', description="online mongodb tools")
    subp = parser.add_subparsers(help='command', dest='command')
    subp_tail = subp.add_parser('tail', help='tail or poll a mongoDB collection')
    subp_tail.add_argument('--connection', **con_arg)
    subp_tail.add_argument('--db', type=str, help='database name', required=True)
    subp_tail.add_argument('--collection', type=str, help='collection name', required=True)
    subp_tail.add_argument('--track_field', type=str, default=None,
                           help='name of field to be used (its values must be monotonically increasing)\
                           it defaults to _id or ts for capped collections'
                           )
    subp_tail.add_argument('--projection', type=str, default=None,
                           help='space delimited of fields to return')
    subp_tail.add_argument('--values_only', default=False, action="store_true", help='print values only')
    subp_impr = subp.add_parser('import', help='import a workbook to MongoDB')
    subp_impr.add_argument('--connection', **con_arg)
    subp_impr.add_argument('--db', type=str, help='database name', required=True)
    subp_impr.add_argument('--filepath', type=str, required=True, help='full file path to workbook')
    return parser.parse_args()


def main():
    args = parse_args()
    print ("command arguments")
    print(vars(args))
 
    client = muClient(args.connection)
    assert client.db is not None
    if args.command == "tail":
        projection = args.projection.split(' ') if args.projection is not None else None
        sub = Sub(client[args.db][args.collection], track_field=args.track_field)
        q = {}
        gen = sub.sub(query=q, projection=projection, start_from_last=True, sleep_secs=2) 
        for doc in gen:
            if args.values_only is True:
                print (doc.values())
            else:
                print (doc)
    elif args.command == "import":
        from mongoUtils.importsExports import import_workbook
        res = import_workbook(args.filepath, client[args.db], fields=True,
                              ws_options={'dt_python': True}, stats_every=100)


if __name__ == "__main__":
    main()