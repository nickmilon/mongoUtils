'''
Created on Feb 3, 2016

@author: @nickmilon

migrates an es indexes to mongoDB

:Example:
>>> python -m xxx.yyy.es -mongo mongodb://localhost:27017 -es localhost:9200 -prefix es_

'''


import argparse
from Hellas.Thebes import Progress
from Hellas.Sparta import DotDot
from mongoUtils.client import muClient
import re
try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    print ("elasticsearch library is not installed \n >>> pip install elasticsearch")
    raise


class ESMONGO(object):
    """
    A class used to migrate elastic search indexes to mongoDB
    .. Warning:: if a db with same name exists already in mongoDB it will be overridden


    :Parameters:
        - client_es: an elasticsearch client client (i.e: client_es = Elasticsearch("localhost:92000")
        - client_mongo: a mongoDB client instance (i.e: client_mongo = MongoClient('mongodb://localhost:27017')

    :Example:
        >>> espmongo = ESMONGO(Elasticsearch("localhost:92000") , MongoClient('mongodb://localhost:27017')
        >>>
        son2Bar
    """
    def __init__(self, client_es, client_mongo):
        self.es = client_es
        self.mongo = client_mongo
        self.es_indexes = client_es.indices.get('*')

    def _normalize_db_name(self, es_name, replace_with="X_"):
        return re.sub("[\.\\\/\$\<\>:|?\s]", replace_with, es_name)[:64]

    def _normalize_collection_name(self, es_name, replace_with="_"):
        return re.sub("\$\s]", replace_with, es_name)[:64]  # no hard limit on size but limit it so we don't exceed namspave

    def get_es_indices(self):
        return self.es_indexes.keys()

    def es_hit_insert_to_db(self, hit, db):
        doc = hit['_source']
        doc['_id'] = hit['_id']
        db[self._normalize_collection_name(hit['_type'])].insert_one(doc)

    def import_all(self, db_prefix='', verbose=1):
        """ imports all indexes from es client

         :Parameters:
        - db_prefix:  (str) prefix the mongo db name with this string

        """
        res = {}
        for idx in self.get_es_indices():
            res[idx] = self.import_index(idx, db_prefix, verbose)[-1]
        return res

    def import_index(self, index_name, db_prefix='', verbose=1, es_query=None, es_size=100, **kargs):
        """
        imports an es index documents to a mongo db, mongo db name is same as index_name
        output collection name(s) are derived from es _type

        .. Warning:: any db with same name as index_name will be dropped (use db_prefix to change db's name)

        :Parameters:
        - index_name: (str) name of index to import
        - db_prefix:  (str) prefix the mongo db name with this string
        - verbose:    (int) if >0 print results and progress stats
         `for es_ arguments see  <https://elasticsearch-py.readthedocs.io/en/master/helpers.html#bulk-helpers>`_

        :Returns:
            tuple (mongoDB, docs migrated, total_docs in es index)

        :Example:
        >>> r=esmongo.import_index(index_name='foo', db_prefix='', verbose=1, es_query=None, size=100)
        """
        db_name = self._normalize_db_name(db_prefix + index_name)
        db = self.mongo[db_name]
        self.mongo.drop_database(db.name)
        es_query = {'query': {'match_all': {}}} if es_query is None else es_query
        docs_total = self.es.count(index=index_name)['count']
        if verbose:
            head_line = "importing {:12,d} from es index {} to {}".format(docs_total, index_name, db_name)
            progress = Progress(max_count=docs_total, head_line=head_line, every_seconds=30, every_mod=None)
        doc_cnt = 0
        r = helpers.scan(self.es, index=index_name, query=es_query, size=es_size, **kargs)
        for hit in r:
            doc_cnt += 1
            self.es_hit_insert_to_db(hit, db)
            if doc_cnt % 10 == 0:
                progress.progress(10)
        assert sum([db[c].count() for c in db.collection_names()]) == doc_cnt
        return db, doc_cnt, docs_total


def parse_args():
    parser = argparse.ArgumentParser(description="set up connections")
    parser.add_argument('-mongo',    default='mongodb://localhost:27017', type=str,
                        help='mongoDB connection string (see: https://docs.mongodb.com/manual/reference/connection-string/) defaults to:mongodb://localhost:27017')
    parser.add_argument("-es",    default="localhost:9200", type=str,
                        help='es connection defaults to localhost:9200')
    parser.add_argument("-prefix", default="", type=str,
                        help='db name prefix to use when creating mongoDB database (database_name = dbPrefix + es index name defaults to""')
    return parser.parse_args()


def main():
        args = parse_args()
        client_mongo = muClient(args.mongo, document_class=DotDot, w="1")
        client_es = Elasticsearch(args.es, sniff_on_start=True)
        espmongo = ESMONGO(client_es, client_mongo)
        dbs_to_drop = ", ".join([args.prefix + espmongo._normalize_db_name(i) for i in espmongo.es_indexes])
        print ("this operation will PERMANTLY DELETE mongoDB databases\n {}\n if exist !!! \n".format(dbs_to_drop))
        if raw_input("do U want to proceed (Y/N) ?") == 'Y':
            return espmongo.import_all(db_prefix=args.prefix, verbose=1)

if __name__ == "__main__":
    main()