'''
Created on Mar 16, 2013

@author: milon
'''

from pymongo import MongoClient
from pymongo.errors import ConfigurationError


class MdbClient(object):
    '''
    a mongo client
    use it in a 'with' statement or else call close() when instance is not needed
    see: http://api.mongodb.org/python/current/api/pymongo/mongo_client.html
    creates a MongoClient instance
    '''
    def __init__(self, *args, **kwargs):
        '''arguments: see MongoClient
        '''
        self.set_client(*args, **kwargs)
        try:
            self.db_set(self.get_default_database().name)
        except ConfigurationError:
            self.db = None
        print "after"
        self.setup_collections()

    # @auto_retry((ConnectionFailure, ConfigurationError ))  pymongo v > 3 doesn't support those exception any more
    def set_client(self, *args, **kwargs):
        if self.client:
            self.client.close()
        self.client = MongoClient(*args, **kwargs)
        return self.client

    def db_set(self, name, codec_options=None, read_preference=None, write_concern=None):
        self.db = self.client.get_database(name,
                                           codec_options=codec_options,
                                           read_preference=read_preference,
                                           write_concern=write_concern)
        return self.db

    def setup_collections(self):
        """ override in subclasses to initialize collections i.e:
                self.users=self.db['users']
                self.users.ensure_index ("foo")
        """
        return NotImplementedError

    def is_replicated(self):
        return self.client.primary(self) is not None

    def dropCollections(self, db=None, startingwith=['tmp.mr', 'tmp_', 'del_']):
        if db is None:
            db = self.db
        if db:
            dbcols = db.collection_names()
            colsToRemove = [c for c in dbcols if any([c.startswith(i) for i in startingwith])]
            for c in colsToRemove:
                (db.drop_collection(c))
            return colsToRemove

    def collStats(self, collectionNameStr):
        return self.db.command("collstats", collectionNameStr)

    def close(self):
        if hasattr(self, 'client'):
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, tp, value, traceback):
        self.close()
        return False  # @info False so we raise error see:http://docs.python.org/release/2.5/whatsnew/pep-343.html

    def __del__(self):
        if self:
            self.close()
