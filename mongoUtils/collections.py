'''
Created on Dec 23, 2010

@author: milon
'''

from Hellas.Sparta import DotDot
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


def coll_stats(db, coll_name_str):
    "collection statistics"
    return db.command("collstats", coll_name_str)


def coll_index_names(a_collection):
    return [i['key'][0][0] for i in a_collection.index_information().values()]


def coll_validate(db, coll_name_str, scandata=False, full=False):
    ''' see: http://docs.mongodb.org/manual/reference/command/validate/#dbcmd.validate
    '''
    return db.validate_collection(coll_name_str, scandata=scandata, full=full)


def coll_range(aCollection, field_name="_id"):
    "returns minimum, maximum value of a field"
    idMin = aCollection.find_one(sort=[(field_name, 1)], fields=[field_name], as_class=DotDot)
    if idMin:
        idMin = eval("idMin." + field_name)  # @note:  a little tricky coz field_name can be sub fields
        idMax = aCollection.find_one(sort=[(field_name, -1)], fields=[field_name], as_class=DotDot)
        idMax = eval("idMax." + field_name)
        return idMin, idMax
    else:
        return None, None


def update_id(collection, doc_old, new_id):
    """very dangerous if you don't know what you are doing, use it at your own risk
       be careful on swallow copies if modifying IdNew from old document
    """
    docNew = doc_old.copy()
    docNew['_id'] = new_id
    try:
        rt = collection.insert(docNew)
    except Exception as e:
        raise
        return False, Exception, e  # @note:return before removing original !!!!
    else:
        return True, collection.remove({"_id": doc_old['_id']},  multi=False), rt


def coll_chunks(collection, chunkSize=50000, field_name="_id"):
    """ similar to not documented splitVector command
        db.runCommand({splitVector: "test.uniques", keyPattern: {dim0: 1},
        maxChunkSizeBytes: 32000000})
        Provides a range query arguments for scanning a collection in batches equals to chunkSize
        for optimization reasons first chunk size is chunksize +1
        if field_name is not specified defaults to _id if specified it must exist for all documents
        in collection and been indexed will make things faster
        usage:chk=collectionChunks(a_colection, chunkSize=500000, field_name="AUX.Foo");
              for i in chk:print i
    """
    idMin, idMax = coll_range(collection, field_name)
    curMin = idMin
    curMax = idMax
    cntChunk = 0
    while cntChunk == 0 or curMax < idMax:
        nextChunk = collection.find_one({field_name: {"$gte": curMin}}, sort=[(field_name, 1)],
                                        skip=chunkSize, as_class=DotDot)
        curMax = eval('nextChunk.' + field_name) if nextChunk else idMax
        query = {field_name: {"$gte"if cntChunk == 0 else "$gt": curMin, "$lte": curMax}}
        yield cntChunk, query
        cntChunk += 1
        curMin = curMax


def coll_copy(collObjFrom, collObjTarget, filterDict={},
              create_indexes=False, dropTarget=False, verboseLevel=1):
    ''' # @note: we can also copy the collection using Map Reduce which is probably faster:
        http://stackoverflow.com/questions/3581058/mongodb-map-reduce-minus-the-reduce
    '''
    if verboseLevel > 0:
        print "copy_collection:%s to %s" % (collObjFrom.name, collObjTarget.name)
    if dropTarget:
        collObjTarget.drop()
    docs = collObjFrom.find(filterDict)
    totalRecords = coll_stats(collObjFrom.database, collObjFrom.name)['count'] \
        if filterDict == {} else docs.count()
    for cnt, doc in enumerate(docs):
        percDone = round((cnt + 1.0) / totalRecords, 3) * 100
        if verboseLevel > 0 and percDone % 10 == 0:
            print "%% done=:%3d %% :%10d of %10d" % (percDone, cnt + 1, totalRecords)
        collObjTarget.save(doc, secure=False)
    if create_indexes:
        for k, v in collObjFrom.index_information().iteritems():
            if k != "_id_":
                idx = (v['key'][0])
                if verboseLevel > 0:
                    print "creating index:%s" % (str(idx))
                collObjTarget.create_index(idx[0], idx[1])
    return collObjTarget


class AuxTools(object):
    '''
    Auxiliary tools
    based on https://github.com/rick446/MongoTools/blob/master/mongotools/sequence/sequence.py
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
