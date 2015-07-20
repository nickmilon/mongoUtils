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
    """collection statistics scale default in MegaBytes, give it 2 ** 30 for GigaBytes
    """
    return DotDot(collection_obj.database.command("collstats", collection_obj.name,
                                                  indexDetails=indexDetails, scale=scale))


def coll_index_names(coll_obj):
    return [i['key'][0][0] for i in list(coll_obj.index_information().values())]


def coll_validate(coll_obj, scandata=False, full=False):
    """`see validate <http://docs.mongodb.org/manual/reference/command/validate/#dbcmd.validate>`_"""
    return coll_obj.database.validate_collection(coll_obj.name, scandata=scandata, full=full)


def coll_range(coll_obj, field_name="_id"):
    """returns (minimum, maximum) value of a field

    :Parameters:
        - coll_obj: a pymongo collection object
        - field_name: (str) name of field (defaults to _id)
    :Example:
        >>> coll_range(db.muTest_tweets_users, 'id_str')
        (u'1004509039', u'999314042')
    """
    projection = {} if field_name == '_id' else {'_id': 0, field_name: 1}  # make sure we get just ONE field
    idMin = coll_obj.find_one(sort=[(field_name, 1)], projection=projection)
    if idMin:
        idMin = list(idMin.values())[0]
        idMax = coll_obj.find_one(sort=[(field_name, -1)], projection=projection)
        idMax = list(idMax.values())[0]
        return idMin, idMax
    else:
        return None, None


def coll_chunks(collection, field_name="_id", chunk_size=100000):
    """Provides an iterator with range query arguments for scanning a collection in batches equals to chunk_size
    for optimization reasons first chunk size is chunk_size +1
    similar to undocumented mongoDB splitVector command try it in mongo console:
    ``db.runCommand({splitVector: "mongoUtilsTests.muTest_tweets", keyPattern: {_id: 1}, maxChunkSizeBytes: 1000000})``
    seems our implementation is faster than splitVector when tried on a collection of ~300 million documents

    :Parameters:
        - collection: (obj) a pymongo collection instance
        - field_name: (str) the collection field to use, defaults to _id, all documents in
           this field must be indexed otherwise operation will be slow,
           also collection must have a value for this field 
        -  chunk_size: (int or float)  (defaults to 100000)
            - if int requested  number of documents in each chunk
            - if float (< 1.0) percent of total documents in collection i.e if 0.2 means 20%
    :Returns:
        - an iterator with a tuple (chunk number, query specification dictionary for each chunk)

    :Usage:
        >>> coll_chunks(db.muTest_tweets, 'id_str', 400)
        >>> for i in rt: print i
        (0, {'id_str': {'$lte': u'523829721985851392', '$gte': u'523829696790663168'}})
        (1, {'id_str': {'$lte': u'523829751329611777', '$gt': u'523829721985851392'}})
        (2, {'id_str': {'$lte': u'523829763937681408', '$gt': u'523829751329611777'}})

    """
    projection = {} if field_name == '_id' else {'_id': 0, field_name: 1}  # make sure we get just ONE field
    if isinstance(chunk_size, float) and chunk_size < 1:
        chunk_size = int(collection.count() * chunk_size)
    idMin, idMax = coll_range(collection, field_name)
    curMin = idMin
    curMax = idMax
    cntChunk = 0
    while cntChunk == 0 or curMax < idMax:
        nextChunk = collection.find_one({field_name: {"$gte": curMin}}, sort=[(field_name, 1)],
                                        skip=chunk_size, projection=projection)
        curMax = list(nextChunk.values())[0] if nextChunk else idMax
        query = {field_name: {"$gte" if cntChunk == 0 else "$gt": curMin, "$lte": curMax}}
        yield cntChunk, query
        cntChunk += 1
        curMin = curMax


def coll_update_id(coll_obj, doc, new_id):
    """updates a document's id by inserting a new doc then removing old one

    .. Warning:: | Very dangerous if you don't know what you are doing, use it at your own risk.
                 | Never use it in production
                 | Also be careful on swallow copies

    :Parameters:
        - coll_obj: a pymongo collection 
        - doc: document_to rewrite with a new_id
        - new_id: value of new id
    :Returns:  a tuple
        - tuple[0]: True if operation was successful otherwise False
        - tuple[1]: Exception if unsuccessful or delete results if success
        - tuple[2]: Insert results if successful
    """
    docNew = doc.copy()
    docNew['_id'] = new_id
    try:
        rt = coll_obj.insert_one(docNew)
    except Exception as e:
        return False, Exception, e  # @note: on error return before removing original !!!!
    else:
        if rt.inserted_id == new_id:
            return True, coll_obj.find_one_and_delete({"_id": doc['_id']}), rt
        else:
            return False, False, False


def coll_copy(collObjFrom, collObjTarget, filter_dict={},
              create_indexes=False, dropTarget=False, write_options={}, verbose=10):
    """copies a collection using unordered bulk inserts
    similar to `copyTo <http://docs.mongodb.org/manual/reference/method/db.collection.copyTo/>`_ that is now deprecated

    :Parameters:
        - collObjFrom:
        - collObjTarget: destination collection
        - filter_dict: a pymongo query dictionary to specify which documents to copy (defaults to {})
        - create_indexes: creates same indexes on destination collection if True
        - dropTarget: drop target collection before copy if True (other wise appends to it)
        - write_options: operation options (use {'w': 0} for none critical copies
        - verbose: if > 0 prints progress statistics at verbose percent intervals
    """
    frmt_stats = "copying {:6.2f}% done  documents={:20,d} of {:20,d}"
    if verbose > 0:
        print("copy_collection:{} to {}".format(collObjFrom.name, collObjTarget.name))
    if dropTarget:
        collObjTarget.drop()
    docs = collObjFrom.find(filter_dict)
    totalRecords = collObjFrom.count() if filter_dict == {} else docs.count()
    if verbose > 0:
        print("totalRecords", totalRecords)
    perc_done_last = -1
    bulk = collObjTarget.initialize_unordered_bulk_op()
    cnt = 0
    for doc in docs:
        cnt += 1
        if verbose > 0:
            perc_done = round((cnt + 1.0) / totalRecords, 3) * 100
            if perc_done != perc_done_last and perc_done % verbose == 0:
                print(frmt_stats.format(perc_done, cnt, totalRecords))
                perc_done_last = perc_done
        bulk.insert(doc)
        if cnt % 20000 == 0 or cnt == totalRecords:
            bulk.execute(write_options)
            bulk = collObjTarget.initialize_unordered_bulk_op()
    if create_indexes:
        for k, v in list(collObjFrom.index_information().items()):
            if k != "_id_":
                idx = (v['key'][0])
                if verbose > 0:
                    print("creating index {:s}".format(k))
                collObjTarget.create_index([idx], backgound=True)
    return collObjTarget


def db_capped_create(db, coll_name, sizeBytes=10000000, maxDocs=None, autoIndexId=True):
    """create a capped collection

    :Parameters:
        - `see here <http://api.mongodb.org/python/current/api/pymongo/database.html>`_ 
        - `and here <http://docs.mongodb.org/manual/reference/method/db.createCollection/>`_
    """
    if coll_name not in db.collection_names():
        return db.create_collection(coll_name, capped=True, size=sizeBytes,
                                    max=maxDocs, autoIndexId=autoIndexId)
    else:
        raise CollectionExists(coll_name)


def db_convert_to_capped(db, coll_name, sizeBytes=2 ** 30):
    """converts a collection to capped"""
    if coll_name in db.collection_names():
        return db.command({"convertToCapped": coll_name, 'size': sizeBytes})


def db_capped_set_or_get(db, coll_name, sizeBytes=2 ** 30, maxDocs=None, autoIndexId=True):
    """sets or converts a collection to capped
    `see more here <http://docs.mongodb.org/manual/tutorial/use-capped-collections-for-fast-writes-and-reads>`_
    autoIndexId must be True for replication so must be True except on a stand alone mongodb or
    when collection belongs to local db
    """
    if coll_name not in db.collection_names():
        return db.create_collection(coll_name, capped=True, size=sizeBytes,
                                    max=maxDocs, autoIndexId=autoIndexId)
    else:
        capped_coll = self[coll_name]
        if not capped_coll.options().get('capped'):
            db.command({"convertToCapped": coll_name, 'size': sizeBytes})
        return capped_coll


def client_schema(client, details=1, verbose=True):
    """returns and optionally prints a mongo schema containing databases and collections in use

    :Parameters:
        - client: a pymongo client instance
        - details: (int) level of details to print/return
        - verbose: (bool) if True prints results
    """
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
        """collection statistics (see :func:`col_stats`)"""
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
        """:Returns: database statistics"""
        return DotDot(self.command("dbStats", scale=scale))

    def collstats(self, details=2, verbose=True):
        """:Returns: database collections statistics"""
        def coll_details(col):
            res = col.name if details == 0 else {'namespace': self.name + '.' + col.name}
            if details > 1:
                res['stats'] = col.collstats()
            return res
        rt = DotDot([[c, coll_details(self[c])] for c in self.collection_names()])
        pp_doc(rt, sort_keys=True, verbose=verbose)
        return rt

    def server_status(self, verbose=True):
        """:Returns: server status"""
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
        """returns names of all collections starting with specified strings

        :parameter: list startingwith: starting with prefixes i.e.  ["tmp\_", "del\_"]
        """
        return [c for c in self.collection_names() if any([c.startswith(i) for i in startingwith])]

    def drop_collections_startingwith(self, startingwith=[]):
        """drops all collections names starting with specified strings

        :parameter: list startingwith: starting with prefixes i.e.  ["tmp\_", "del\_"]
        """
        colsToRemove = self.colections_startingwith(startingwith)
        for c in colsToRemove:
            self.drop_collection(c)
        return colsToRemove

    def js_fun_add(self, fun_name, fun_str):
        """adds a js function to database 
        to use the function from mongo shell you have to execute db.loadServerScripts(); first

        :Parameters:
            - fun_name (string): a name for this function
            - fun_str  (string): js function string
        """
        self.system_js[fun_name] = fun_str
        return fun_name

    def js_fun_add_default(self, file_name, fun_name):
        return self.js_fun_add(fun_name, parse_js_default(file_name, fun_name))

    def js_list(self):
        """:Returns: all user js functions installed on server"""
        return self.system_js.list()

    def __getitem__(self, name):
        return muCollection(self, name)


def pp_doc(doc, indent=4, sort_keys=False, verbose=True):
    """pretty print a boson document"""
    rt = json_util.dumps(doc, sort_keys=sort_keys, indent=indent, separators=(',', ': '))
    if verbose:
        print(rt)
    return rt


class AuxTools(object):
    """**a collection to support generation of sequence numbers using counter's collection technique**

    .. Note:: counter's collection guarantees a unique incremental id value even in a multiprocess/mutithreading environment
              but if this id is used for insertions. Insertion order is not 100% guaranteed to correspond to this id.
              If insertion order is critical use the Optimistic Loop technique

    .. Seealso:: `counters collection <http://docs.mongodb.org/manual/tutorial/
        create-an-auto-incrementing-field/#auto-increment-counters-collection>`__
        and `Copeland's snippet <https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py>`__

    :Parameters:
        - collection: (obj optional) a pymongo collection object
        - db: (obj optional) a pymongo database object
        - client:  (obj optional) a pymongo MongoClient instance
            - all parameters are optional but exactly one must be provided
            - if collection is None a collection db[AuxCol] will be used
            - if db is None a collection on db ['AuxTools']['AuxCol'] will be used
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
    """
    | helper function to get a js function string from a file containing js functions
    | useful if we want to call js functions from python as in mongoDB map reduce.
    | Function must be named starting in first column and end with '}' in first column 
      (see relevant functions in js directory)

    :Parameters:
        - file_path: (str) full path_name
        - function name: (str) name of function
        - replace_vars: (optional) a tuple to replace %s variables in functions
    :Returns: a js function as string
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
    """fetch a js function on default directory from file_name (see :func:`parse_js`)"""
    return parse_js(_PATH_TO_JS+file_name, function_name, replace_vars)
