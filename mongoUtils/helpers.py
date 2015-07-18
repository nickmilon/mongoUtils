"""some helper functions and classes"""

from Hellas.Sparta import DotDot
from bson import json_util
from mongoUtils import _PATH_TO_JS
from pymongo.read_preferences import ReadPreference
from pymongo.collection import Collection
from pymongo.database import Database


class MongoUtilsError(Exception):
    """Base class for all MongoUtils exceptions."""


class CollectionExists(MongoUtilsError):
    def __init__(self, collection_name=''):
        super(MongoUtilsError, self).__init__("Collection Exists " + collection_name)


"""operations on a collection object"""


def col_stats(collection_obj, indexDetails=True, scale=2 ** 10):
    """collection statistics
       scale default in MegaBytes, give it 2 ** 30 for GigaBytes
    """
    return DotDot(collection_obj.database.command("collstats", collection_obj.name,
                                                  indexDetails=indexDetails, scale=scale))


def coll_index_names(coll_obj):
    return [i['key'][0][0] for i in coll_obj.index_information().values()]


def coll_validate(coll_obj, scandata=False, full=False):
    """ see here: http://docs.mongodb.org/manual/reference/command/validate/#dbcmd.validate
    """
    return coll_obj.database.validate_collection(coll_obj.name, scandata=scandata, full=full)


def coll_range(coll_obj, field_name="_id"):
    """returns (minimum, maximum) value of a field

    .. Warning:: it uses 'eval' use it carefully
    """
    idMin = coll_obj.find_one(sort=[(field_name, 1)], fields=[field_name])
    if idMin:
        idMin = DotDot(idMin)
        idMin = eval("idMin." + field_name)  # @note:  a little tricky coz field_name can be sub fields
        idMax = DotDot(coll_obj.find_one(sort=[(field_name, -1)], fields=[field_name]))
        idMax = eval("idMax." + field_name)
        return idMin, idMax
    else:
        return None, None


def coll_update_id(coll_obj, doc, new_id):
    """updates a document's id by inserting a new doc then removing old one

    .. Warning:: very dangerous if you don't know what you are doing, use it at your own risk.
       Never use it in production
       also be careful on swallow copies
    """
    docNew = doc.copy()
    docNew['_id'] = new_id
    try:
        rt = coll_obj.insert(docNew)
    except Exception as e:
        raise
        return False, Exception, e  # @note: on error return before removing original !!!!
    else:
        return True, coll_obj.remove({"_id": doc['_id']},  multi=False), rt


def coll_chunks(collection, chunk_size=100000, field_name="_id"):
    """Provides a range query arguments for scanning a collection in batches equals to chunk_size
    for optimization reasons first chunk size is chunk_size +1
    similar to undocumented splitVector command

    >>> db.runCommand({splitVector: "test.uniques", keyPattern: {dim0: 1}, maxChunkSizeBytes: 32000000})

    :Parameters:
        - collection: (obj) a pymongo collection instance
        - chunk_size: (int) requested  number of documents in each chunk, defaults to 100000
        - field_name: (str) the collection field to use, defaults to _id, all documents in
          collection must have a value for this field

    :Returns:

    - a list with a query specification dictionary for each chunk

    :Usage:

    >>> rt = coll_chunks(a_collection, 100)
    >>> for i in rt: print i
    """
    idMin, idMax = coll_range(collection, field_name)
    curMin = idMin
    curMax = idMax
    cntChunk = 0
    while cntChunk == 0 or curMax < idMax:
        nextChunk = collection.find_one({field_name: {"$gte": curMin}}, sort=[(field_name, 1)],
                                        skip=chunk_size, as_class=DotDot)
        curMax = eval('nextChunk.' + field_name) if nextChunk else idMax
        query = {field_name: {"$gte" if cntChunk == 0 else "$gt": curMin, "$lte": curMax}}
        yield cntChunk, query
        cntChunk += 1
        curMin = curMax


def coll_copy(collObjFrom, collObjTarget, filterDict={},
              create_indexes=False, dropTarget=False, verboseLevel=1):
    """ # @note: we can also copy the collection using Map Reduce which is probably faster:
        http://stackoverflow.com/questions/3581058/mongodb-map-reduce-minus-the-reduce
    """
    if verboseLevel > 0:
        print "copy_collection:%s to %s" % (collObjFrom.name, collObjTarget.name)
    if dropTarget:
        collObjTarget.drop()
    docs = collObjFrom.find(filterDict)
    totalRecords = collObjFrom.count() if filterDict == {} else docs.count()
    print "totalRecords", totalRecords
    for cnt, doc in enumerate(docs):
        percDone = round((cnt + 1.0) / totalRecords, 3) * 100
        if verboseLevel > 0 and percDone % 10 == 0:
            print ("%% done=:%3d %% :%10d of %10d" % (percDone, cnt + 1, totalRecords))
        collObjTarget.save(doc)
    if create_indexes:
        for k, v in collObjFrom.index_information().iteritems():
            if k != "_id_":
                idx = (v['key'][0])
                if verboseLevel > 0:
                    print "creating index:%s" % (str(idx))
                collObjTarget.create_index([idx])
    return collObjTarget


def db_capped_create(db, colName, sizeBytes=10000000, maxDocs=None, autoIndexId=True):
    if colName not in db.collection_names():
        return db.create_collection(colName, capped=True, size=sizeBytes,
                                    max=maxDocs, autoIndexId=autoIndexId)
    else:
        raise CollectionExists(colName)


def db_convert_to_capped(db, colName, sizeBytes=2 ** 30):
    if colName in db.collection_names():
        return db.command({"convertToCapped": colName, 'size': sizeBytes})


def db_capped_set_or_get(db, colName, sizeBytes=2 ** 30, maxDocs=None, autoIndexId=True):
    """
    see more here: http://docs.mongodb.org/manual/tutorial/use-capped-collections-for-fast-writes-and-reads
    autoIndexId must be True for replication so must be True except on a stand alone mongodb or
    when collection belongs to 'local' db
    """
    if colName not in db.collection_names():
        return db.create_collection(colName, capped=True, size=sizeBytes,
                                    max=maxDocs, autoIndexId=autoIndexId)
    else:
        cappedCol = self[colName]
        if not cappedCol.options().get('capped'):
            db.command({"convertToCapped": colName, 'size': sizeBytes})
        return cappedCol


def client_schema(client, details=1, verbose=True):
    def col_details(col):
        res = col.name if details == 0 else {'name': col.name}
        if details > 1:
            res['stats'] = col_stats(col)
        return res
    rt = [[d, [col_details(client[d][c])
               for c in client[d].collection_names()]] for d in client.database_names()]
    rt = dict(rt)
    if verbose:
        pp_doc(rt)
    return rt


class muCollection(Collection):
    """just a plain pymongo collection with some extra features
    it is safe to cast an existing pymongo collection to this by:

    >>> a_pymongo_collection_instance.__class__ = muCollection
    """
    def stats(self, indexDetails=True, scale=2 ** 10):
        """collection statistics (see :func:`col_stats`).
        """
        return col_stats(self, indexDetails, scale)

    def index_names(self):
        """see :func:`coll_index_names`"""
        return coll_index_names(self)

    def validate(self, scandata=False, full=False):
        """see :func:`coll_validate`"""
        return coll_validate(self, scandata=scandata, full=full)


class muDatabase(Database):
    """just a plain pymongo Database with some extra features
    it is safe to cast an existing pymongo database to this by:

    >>> a_pymongo_database_instance.__class__ = muDatabase

    """
    def dbstats(self, scale=10 ** 30):
        return DotDot(self.command("dbStats", scale=scale))

    def collstats(self, details=2, verbose=True):
        def coll_details(col):
            res = col.name if details == 0 else {'namespace': self.name + '.' + col.name}
            if details > 1:
                res['stats'] = col.collstats()
            return res
        rt = DotDot([[c, coll_details(self[c])] for c in self.collection_names()])
        pp_doc(rt, sort_keys=True, verbose=verbose)
        return rt

    def server_status(self, verbose=True):
        rt = DotDot(self.command("serverStatus"))
        pp_doc(rt, sort_keys=True, verbose=verbose)
        return rt

    def capped_create(self, colName, sizeBytes=10000000, maxDocs=None, autoIndexId=True):
        return db_capped_create(self, colName, sizeBytes, maxDocs, autoIndexId)

    def convert_to_capped(self, colName, sizeBytes=2 ** 30):
        return db_convert_to_capped(self, colName, sizeBytes)

    def capped_set_or_get(self, colName, sizeBytes=2 ** 30, maxDocs=None, autoIndexId=True):
        """see :func:`db_capped_set_or_get`. """
        return db_capped_set_or_get(self, colName, sizeBytes, maxDocs, autoIndexId)

    def colections_startingwith(self, startingwith=[]):
        """ returns names of all collections starting with specified strings
        :param list startingwith: starting with prefixes i.e.  ["tmp\_", "del\_"]
        """
        return [c for c in self.collection_names() if any([c.startswith(i) for i in startingwith])]

    def drop_collections_startingwith(self, startingwith=[]):
        colsToRemove = self.colections_startingwith(startingwith)
        for c in colsToRemove:
            self.drop_collection(c)
        return colsToRemove

    def js_fun_add(self, fun_name, fun_str):
        """ to use the function from mongo shell you have to execute db.loadServerScripts(); first
        """
        self.system_js[fun_name] = fun_str
        return fun_name

    def js_fun_add_default(self, file_name, fun_name):
        return self.js_fun_add(fun_name, parse_js_default(file_name, fun_name))

    def js_list(self):
        return self.system_js.list()

    def __getitem__(self, name):
        return muCollection(self, name)


def pp_doc(doc, indent=4, sort_keys=False, verbose=True):
    """ pretty print a boson document"""
    rt = json_util.dumps(doc, sort_keys=sort_keys, indent=indent, separators=(',', ': '))
    if verbose:
        print (rt)
    return rt


#  'Copeland's snippet <https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py>`_ 
# 'counters collection <http://docs.mongodb.org/manual/tutorial/create-an-auto-incrementing-field/#auto-increment-counters-collection>`_
class AuxTools(object):
    """**a collection to support generation of sequence numbers using counter's collection technique**

    .. Note:: counter's collection guarantees a unique incremental id value even in a multiprocess/mutithreading environment
              but if this id is used for insertions. Insertion order is not 100% guaranteed to correspond to this id.
              If insertion order is critical use the Optimistic Loop technique

    .. Seealso:: `counters collection <http://docs.mongodb.org/manual/tutorial/
        create-an-auto-incrementing-field/#auto-increment-counters-collection>`__
        and `Copeland's snippet <https://github.com/rick446/
        MongoTools/blob/master/mongotools/pubsub/channel.py>`__

    :Args:

        - collection (obj optional) a pymongo collection object
        - db (obj optional) a pymongo database object
        - client:  (obj optional) a pymongo MongoClient instance

        all parameters are optional but exactly one must be provided
        if collection is None a collection db[AuxCol]  will be used
        if collection is None a collection on db ['AuxTools']['AuxCol'] will be used
    """
    def __init__(self, collection=None, db=None, client=None):
        if collection is None:
            collection = db['AuxCol'] if db is not None else client['AuxTools']['AuxCol']
        self.collection = collection.with_options(read_preference=ReadPreference.PRIMARY)
        # @note make sure sequence_current gets correct value

    def sequence_reset(self, seq_name):
        """resets sequence"""
        self.collection.remove({'_id': seq_name})

    def sequence_set(self, seq_name, val=1):
        """sets sequence's current value to val if doesn't exist it is created"""
        return self.collection.find_and_modify({'_id': seq_name}, {'$set': {'val': val}},
                                               upsert=True, new=True)['val']

    def sequence_current(self, seq_name):
        """returns sequence's current value for particular name"""
        doc = self.collection.find_one({'_id': seq_name})
        return 0 if doc is None else doc['val']

    def sequence_next(self, seq_name, inc=1):
        """increments sequence's current value by incr, if doesn't exist sets initial value to incr"""
        return self.collection.find_and_modify({'_id': seq_name}, {'$inc': {'val': inc}},
                                               upsert=True, new=True)['val']


def parse_js(file_path, function_name, replace_vars=None):
    """helper function to get a js function string from a file containing js functions.
    useful if we want to call js functions from python as in mongoDB map reduce
    Function must be named starting in first column and end with '}' in first column

    :Args:

    - file_path: (str): full path_name
    - function name (str): name of function
    - replace_vars (optional) a tuple to replace %s variables in functions

    :Returns:

    a js function as string
    """
    rt = ''
    start = 'function {}'.format(function_name)
    with open(file_path, 'r') as fin:
        ln = fin.readline()
        while ln:
            if ln.startswith(start) or rt:
                rt += ln
            if rt and ln.startswith('}'):
                break
            ln = fin.readline()
    if rt and replace_vars:
        return rt % replace_vars
    else:
        return rt


def parse_js_default(file_name, function_name, replace_vars=None):
    """fetch a js function od default directory from file_name
    see parse_js
    """
    return parse_js(_PATH_TO_JS+file_name, function_name, replace_vars)
