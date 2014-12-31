'''
Created on Dec 23, 2010

@author: milon
'''

from pymongo.read_preferences import ReadPreference


def capped_set_or_get(client, dbName, colName, sizeBytes=10000000, maxDocs=None, autoIndexId=True):
    """
        http://docs.mongodb.org/manual/tutorial/use-capped-collections-for-fast-writes-and-reads/
        autoIndexId must be True for replication so must be True except on a stand alone mongodb or
        when collection belongs to 'local' db
    """
    if colName not in client[dbName].collection_names():
        return client[dbName].create_collection(colName, capped=True, size=sizeBytes,
                                                max=maxDocs, autoIndexId=autoIndexId)
    else:
        cappedCol = client[dbName][colName]
        if not cappedCol.options().get('capped'):
            client[dbName].command({"convertToCapped": colName, 'size': sizeBytes})
        return cappedCol


class AuxTools(object):
    ''' based on https://github.com/rick446/MongoTools/blob/master/mongotools/sequence/sequence.py
    '''
    def __init__(self, client, db_name='AuxTools', col_name='AuxCol'):
        self.client = client
        self.db = client[db_name]
        self.coll = self.db[col_name]
        self.coll.read_preference = ReadPreference.PRIMARY
        # @note make sure sequence_current gets correct value

    def sequence_reset(self, name):
        self.coll.remove({'_id': name})

    def sequence_current(self, seqName):
        doc = self.coll.find_one({'_id': seqName})
        return 0 if doc is None else doc['val']

    def sequence_next(self, seqName, collName=None, inc=1):
        return self.coll.find_and_modify({'_id': seqName}, {'$inc': {'val': inc}},
                                         upsert=True, new=True)['val']
