#!/usr/bin/env python
"""
a command line tool that simulates tail -f  command to tail documents coming in a mongoDB collection


:Example:
    >>> python -m mongoUtils.tools.tail -connection "mongodb://localhost:27017/classy" -collection statuses -track_field _id
"""

import argparse
from mongoUtils.pubsub import Sub
from mongoUtils.client import muClient


def parse_args():
    parser = argparse.ArgumentParser(description="tail a mongodb collection")
    parser.add_argument('-connection', type=str, required=True,
                        help='a db connection string\
                        as defined in http://docs.mongodb.org/manual/reference/connection-string/\
                        that included a database name'
                        )
    parser.add_argument('-collection', type=str, help='collection name', required=True)
    parser.add_argument('-track_field', type=str, default=None,
                        help='name of field to be used (its values must be monotonically increasing)\
                        it defaults to _id or ts for capped collections'
                        )
    parser.add_argument('-projection', type=str, default=None,
                        help='space delimited of fields to return')
    parser.add_argument('-values_only', default=False, action="store_true", help='print values only')
    return parser.parse_args()


def main():
    args = parse_args()
    print ("starting tailing", vars(args))
    client = muClient(args.connection)
    assert client.db is not None

    projection = args.projection.split(' ') if args.projection is not None else None
    sub = Sub(client.db[args.collection], track_field=args.track_field)
    q = {}
    gen = sub.poll(query=q, projection=projection, start_after='last', delay_secs=1)

    for doc in gen:
        if args.values_only is True:
            print (doc.values())
        else:
            print (doc)
if __name__ == "__main__":
    main()