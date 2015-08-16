""" MongoDB client"""
from pymongo import MongoClient
from pymongo.errors import ConfigurationError
from mongoUtils.helpers import (muDatabase, pp_doc, client_schema)
from Hellas.Sparta import DotDot


class muClient(MongoClient):
    """An enhanced mongoDB client with some extra features
    use it within a with statement or else call close() when instance is not needed any more see
    `mongo_client <http://api.mongodb.org/python/current/api/pymongo/mongo_client.html>`__

    :Property: db instances are initialized with a default property self.db pointing to the database
               if one is included in connection string otherwise it's value is None
               appls can set it by calling the :func:`use` method

    ..Warning:: for efficiency this class descents from MongoClient and shares the way it uses
                class name space. So be very careful when defining methods names in descendant classes to
                avoid masking potential database names to be addressed by pymongo's dot notation
                so client.nodes can't refer to a database coz it is the name of a method in pymongo
                if you have to refer to nodes as database use the client[nodes] form

    :Parameters: `see here <http://api.mongodb.org/python/current/api/pymongo/mongo_client.html>`__

    """
    def __init__(self, *args, **kwargs):
        super(muClient, self).__init__(*args, **kwargs)
        try:
            self.db = self.get_default_database()
        except ConfigurationError as e:
            if e.message == 'No default database defined':
                self.db = None
            else:
                raise
        self.cl_init_complete()

    def cl_colstats(self, details=2, verbose=True):
        rt = DotDot([[d, self[d].collstats(details, False)] for d in self.database_names()])
        pp_doc(rt, sort_keys=False, verbose=verbose)

    def cl_schema(self, details=1, verbose=True):
        return client_schema(self, details, verbose)

    def use(self, db_name):
        """mimics use console command"""
        self.db = self[db_name]

    def cl_db_set(self, name, codec_options=None, read_preference=None, write_concern=None):
        """returns a database with specified options"""
        self.db = self.get_database(name,
                                    codec_options=codec_options,
                                    read_preference=read_preference,
                                    write_concern=write_concern)
        return self.db

    def cl_init_complete(self):
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
    