""" MongoDB client"""
from pymongo import MongoClient
from mongoUtils.helpers import (muDatabase, pp_doc, client_schema)
from Hellas.Sparta import DotDot


class muClient(MongoClient):
    """An enhanced mongoDB client with some extra features
    use it within a with statement or else call close() when instance is not needed any more see
    `mongo_client <http://api.mongodb.org/python/current/api/pymongo/mongo_client.html>`__

    :Parameters: see here http://api.mongodb.org/python/current/api/pymongo/mongo_client.html
    """
    def __init__(self, *args, **kwargs):
        super(muClient, self).__init__(*args, **kwargs)
        self.mu_init_complete()

    def colstats(self, details=2, verbose=True):
        rt = DotDot([[d, self[d].collstats(details, False)] for d in self.database_names()])
        pp_doc(rt, sort_keys=False, verbose=verbose)
 
    def _get_MongoClient(self, *args, **kwargs):
        return MongoClient(*args, **kwargs)

    def schema(self, details=1, verbose=True):
        return client_schema(self, details, verbose)

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, db_name):
        """sets a defualt db"""
        if self.client:
            self._db = self.client[db_name]

    def use(self, db_name):
        """mimics use console command"""
        self.db = db_name

    def db_set(self, name, codec_options=None, read_preference=None, write_concern=None):
        """returns a database with specified options"""
        self.db = self.get_database(name,
                                    codec_options=codec_options,
                                    read_preference=read_preference,
                                    write_concern=write_concern)
        return self.db

    def mu_init_complete(self):
        """override in subclasses to initialize things i.e:

        >>> self.users=self.db['users']
        >>> self.users.ensure_index ("foo")
        """
        return NotImplementedError

    def is_replicated(self):
        return self.primary(self) is not None

    def _db_command(self, command_str, *args):
        return self._db.command(command_str, *args)

    def __getitem__(self, name):
        return muDatabase(self, name)

#     def __repr__(self): 
#         return super(muClient, self).__repr__().replace('MongoClient', 'MongoUtiles.Client')

    def __enter__(self):
        return self

    def __exit__(self, tp, value, traceback):
        self.close()
        return False  # @info False so we raise error see:http://docs.python.org/release/2.5/whatsnew/pep-343.html

    def __del__(self):
        if self:
            self.close()
    