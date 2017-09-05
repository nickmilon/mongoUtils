"""some helper functions and classes"""

import logging
from datetime import datetime, date
from Hellas.Sparta import DotDot, seconds_to_DHMS
from Hellas.Thebes import Progress
from bson import json_util, SON
from bson.objectid import ObjectId
from mongoUtils import _PATH_TO_JS
from pymongo.read_preferences import ReadPreference
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo import ReturnDocument
from pymongo.bulk import BulkOperationBuilder

LOG = logging.getLogger(__name__)
LOG.debug("loading module: " + __name__)


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


def coll_copy(collObjFrom, collObjTarget, filter_dict=None,
              create_indexes=False, dropTarget=False, write_options={'w': "majority"}, verbose=10):
    """copies a collection using unordered bulk inserts
    similar to `copyTo <http://docs.mongodb.org/manual/reference/method/db.collection.copyTo/>`_ that is now deprecated

    :Parameters:
        - collObjFrom: original collection
        - collObjTarget: destination collection
        - filter_dict: a pymongo query dictionary to specify which documents to copy (defaults to {})
        - create_indexes: creates same indexes on destination collection if True
        - dropTarget: drop target collection before copy if True (other wise appends to it)
        - write_options: operation options (use {'w': 0} for none critical copies
        - verbose: if > 0 prints progress statistics at verbose percent intervals
    """
    frmt_stats = "copying {:6.2f}% done  documents={:22,d} of {:22,d}"
    if verbose > 0:
        print("copy_collection:{}.{} to ==> {}.{}".format(collObjFrom.database.name, collObjFrom.name, collObjTarget.database.name, collObjTarget.name))
    if dropTarget:
        collObjTarget.drop()
    docs = collObjFrom.find(filter_dict)
    totalRecords = collObjFrom.count() if filter_dict is None else docs.count()
    if verbose > 0:
        print("totalRecords", totalRecords)
    perc_done_last = -1
    bulk = muBulkOps(collObjTarget, ordered=False, ae_n=1000, dwc=write_options)
    cnt = 0
    for doc in docs:
        cnt += 1
        if verbose > 0:
            perc_done = round((cnt + 1.0) / totalRecords, 3) * 100
            if perc_done != perc_done_last and perc_done % verbose == 0:
                print(frmt_stats.format(perc_done, cnt, totalRecords))
                perc_done_last = perc_done
        bulk.insert(doc)
    bulk.execute_if_pending()
    if create_indexes:
        for k, v in list(collObjFrom.index_information().items()):
            if k != "_id_":
                idx = (v['key'][0])
                if verbose > 0:
                    print("creating index {:s}".format(k))
                collObjTarget.create_index([idx], backgound=True)
    return collObjTarget


def coll_transform(coll, query={}, func=lambda x, y: x, verbose=True, **kwargs):
    """ transforms collection's documents by applying function func to (doc, collection) func should either return a document or None
    """
    finds = coll.find(query)

    max_count = coll.count() if query == {} else None
    counter = DotDot({'total': 0, 'transformed': 0})
    if verbose:
        head_line = "transforming db:'{}' collection:'{}'".format(coll.database.name, coll.name)
        progress = Progress(max_count=max_count, head_line=head_line,
                            extra_frmt='{total:12,d}|{transformed:12,d}|', extra_dict=counter, every_seconds=30, every_mod=None)
    for doc in finds:
        counter.total += 1
        new_doc = func(doc, coll)
        if new_doc:
            counter.transformed += 1
            coll.replace_one({'_id': doc['_id']}, doc, **kwargs)
        if verbose and counter.total % 100 == 0:
            progress.progress(100, counter)
    if verbose:
        progress.print_end(counter)
    return counter


def coll_validation_set(db, collection_name, validator=None, validationLevel="moderate", validationAction="error"):
    """ sets validation to a collection

    `see here <https://docs.mongodb.com/manual/core/document-validation/>`_

    :Parameters:
        - validationLevel (str): one of "moderate" or "strict"
        - validationAction (str): one of 'error' or 'warn'
        - validationAction (str): one of 'error' or 'warn'

    :Returns:
         collection (object)
    """

    if collection_name in db.collection_names():
        db.command('collMod', collection_name, validator=validator, validationLevel="moderate")
    else:
        db.create_collection(collection_name, validator=validator, validationLevel=validationLevel)
    return db[collection_name]


def db_transform(db, query={}, func=lambda x, y: x, **kwargs):
    res = DotDot()
    for col_name in db.collection_names():
        res[col_name] = coll_transform(db[col_name], query={}, func=func, **kwargs)
    return res


def client_transform(mongo_client, query={}, func=lambda x, y: x, exclude_dbs=['test', 'local', 'admin'], **kwargs):
    res = DotDot()
    for db_name in mongo_client.database_names():
        if db_name not in exclude_dbs:
            res[db_name] = db_transform(mongo_client[db_name], query={}, func=func, **kwargs)
    return res


def db_copy(dbObjFrom, dbObjTarget, col_name_prefix='',
            create_indexes=True, dropTarget_collections=False, write_options={'w': "majority"}, verbose=10):
    """copies a db by calling coll_copy for all its collections
    useful becouse shell command doesn't work properly for protected dbs

    :Parameters:
        - dbObjFrom: original db
        - dbObjTarget: destination db
        - col_name_prefix: a string to prefix target collection names so will not overwrite those defaults to ''
        - create_indexes: creates same indexes on destination collections if True (default)
        - dropTarget_collections: drop target collections before copy if True (other wise appends to it)
        - write_options: operation options (use {'w': 0} for none critical copies
        - verbose: if > 0 prints progress statistics at verbose percent intervals
    """
    for col_name in dbObjFrom.collection_names():
        if col_name != "system":
            coll_copy(dbObjFrom[col_name], dbObjTarget[col_name_prefix + col_name], filter_dict=None,
                      create_indexes=create_indexes, dropTarget=dropTarget_collections, write_options=write_options, verbose=verbose)
    return dbObjTarget


def db_capped_create(db, coll_name, sizeBytes=1024 * 1000 * 100, maxDocs=None, autoIndexId=True, **kwargs):
    """create a capped collection

    :Parameters:
        - `see here <http://api.mongodb.org/python/current/api/pymongo/database.html>`_
        - `and here <http://docs.mongodb.org/manual/reference/method/db.createCollection/>`_
    """
    if coll_name not in db.collection_names():
        return db.create_collection(coll_name, capped=True, size=sizeBytes,
                                    max=maxDocs, autoIndexId=autoIndexId, **kwargs)
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
    :warning:
        - warming existiong indexes will be lost if converted to capped  
    """
    if coll_name not in db.collection_names():
        return db.create_collection(coll_name, capped=True, size=sizeBytes,
                                    max=maxDocs, autoIndexId=autoIndexId)
    else:
        capped_coll = db[coll_name]
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


class muBulkOps(object):
    """ a wrapper around BulkOperationBuilder provides for some automation

    .. versionadded:: 1.0.6

    :parameters:
        - ae_n: (int) auto execute every n operations (defaults to 0 to refrain from auto execution)
        - ae_s: (int) auto execute seconds since start or last execute before a new execute is automatically initiated
          useful when we want to ensure that collection data are relative fresh
          set it to 0 (default to disable auto execute b
        - dwc: (dict) or None default write concern to use in case of autoexecute_every
          DO NOT pass a WriteConcern object just a plain dict i.e {'w':1}

    .. Warning:: | caller should NOT in any way modify documents that are in pipeline pending execute
                 | if u are not sure use doc.copy() and be careful on swallow copies
    """
    frmt_stats = "{:s}db:{:s} collection:{:s} cnt_operations_executed:{:16,d} cnt_operations_pending:{:6,d}"

    def __init__(self, collection, ordered=True, ae_n=0, ae_s=0, dwc=None):
        """Initialize a new BulkOperationBuilder instance."""
        self.collection = collection
        self.ordered = ordered
        self.ae_n = ae_n
        self.dwc = dwc
        self.cnt_operations_pending = 0
        self.cnt_operations_executed = 0
        self.ae_n = ae_n
        self.ae_s = ae_s
        if ae_s != 0:
            self.dt_last = datetime.now()
        self._init_builder()

    def _init_builder(self):
        self._bob = BulkOperationBuilder(collection=self.collection, ordered=self.ordered)

    def find(self, selector):
        return self._bob.find(selector)

#     def append(self, document):
#         return self.insert(document)
    def stats(self, message=''):
        return self.frmt_stats.format(message, self.collection.database.name, self.collection.name,
                                      self.cnt_operations_executed, self.cnt_operations_pending)

    def stats_print(self):
        print(self.stats())

    def insert(self, document):
        rt = self._bob.insert(document)
        self.cnt_operations_pending += 1
        # LOG.critical(self.stats("inserts"))
        if self.ae_s != 0:
            current_dt = datetime.now()
            if self.cnt_operations_pending == self.ae_n or ((current_dt - self.dt_last).seconds > self.ae_s):
                self.dt_last = current_dt
                rt = self.execute(write_concern=self.dwc, recreate=True)
        elif self.cnt_operations_pending == self.ae_n:
            rt = self.execute(write_concern=self.dwc, recreate=True)
        return rt

    def execute(self, write_concern=None, recreate=True):
        rt = self._bob.execute(write_concern=write_concern)
        self.cnt_operations_executed += self.cnt_operations_pending
        self.cnt_operations_pending = 0
        if recreate:
            self._init_builder()
        return rt

    def execute_if_pending(self, write_concern=None):
        """executes if any pending operations still exist call it on error or something"""
        if write_concern is None:
            write_concern = self.dwc
        if self.cnt_operations_pending > 0:
            rt = self.execute(write_concern=self.dwc, recreate=True)
            return rt
# Collection.parallel_scan(self, num_cursors)


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

    .. todo::
        - use a range of ids for eficiency as per last source below

    .. Seealso:: `counters collection <http://docs.mongodb.org/manual/tutorial/
        create-an-auto-incrementing-field/#auto-increment-counters-collection>`__
        and `Copeland's snippet <https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py>`__
        `generating globally_unique Ids <https://www.mongodb.com/blog/post/generating-globally-unique-identifiers-for-use-with-mongodb?jmp=twt&utm_content=buffere7369&utm_medium=social&utm_source=twitter.com&utm_campaign=buffer>`__

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
        return self.collection.find_one_and_update({'_id': seq_name}, {'$set': {'val': val}},
                                                   upsert=True, return_document=ReturnDocument.AFTER)['val']

    def sequence_current(self, seq_name):
        """returns sequence's current value for particular name"""
        doc = self.collection.find_one({'_id': seq_name})
        return 0 if doc is None else doc['val']

    def sequence_next(self, seq_name, inc=1):
        """increments sequence's current value by incr, if doesn't exist sets initial value to incr"""
        return self.collection.find_one_and_update({'_id': seq_name}, {'$inc': {'val': inc}},
                                                   upsert=True, return_document=ReturnDocument.AFTER)['val']


class SONDot(SON):
    """
    A SON class that can handle dot notation to access its members (useful when parsing JSON content)

    :Example:
        >>> son = SONDot([('foo', 'bar'), ('son2', SON([('son2foo', 'son2Bar')]))])
        >>> son.son2.son2foo
        son2Bar

    .. Warning:: don't use dot notation for write operations  i.e son.foo = 'bar' **(it will fail silently !)**
    """
    def __getattr__(self, attr):
        try:
            item = self[attr]
        except KeyError as e:
            raise AttributeError(e)    # expected Error by pickle on __getstate__ etc
        if isinstance(item, dict) and not isinstance(item, DotDot):
            item = SONDot(item)
        return item


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


def geo_near_point_q(geo_field, Long_Lat, query={}, minDistance=None, maxDistance=None):
    """geo near point query constructor

    :Parameters:
        - geo_field: (str) name of geo indexed field (i.e. location)
        - Long_Lat: (tuple or list) [longitude, latitude]
        - query: (dict) an other query specifications to be combined with geo query (defaults to {})
        - minDistance: minimum distance in meters (defaults to None)
        - maxDistance: up to distance in meters (defaults to None)

    :Returns: query dictionary updated with geo specs
    """
    gq = {geo_field: {'$near': {'$geometry': {'type': 'Point', 'coordinates': Long_Lat}}}}
    if minDistance is not None:
        gq[geo_field]['$near']['$minDistance'] = minDistance
    if maxDistance is not None:
        gq[geo_field]['$near']['$maxDistance'] = maxDistance
    query.update(gq)
    return query


def field_counts(collection, field, sort=True, incl_perc=True):
    """a group counts function with functionality simillar to aggregation $group
    it is more performant than aggregation or a map reduce equivelant when distinct values of field
    are a small number < ~30 and field is indexed.

    :Parameters:
        - collection: a pymongo collection object
        - field: (str) field name
        - sort: (True) sort results by value if True
        - incl_perc: (bool) include percentages if True
    """
    rt = [[v, collection.find({field: v}).count()] for v in collection.distinct(field)]
    if incl_perc:
        total = float(sum([i[1] for i in rt]))
        for i in rt:
            i.append(100.0 * (i[1]/total))
    if sort:
        rt = sorted(rt, key=lambda x: x[1])
    return rt


def collection_insert_dict(a_collection, a_dict, use_key_as_id=True):
    """
    inserts dictionary items into a collection using unordered bulk operation
    :Parameters:
        - a_collection: a pymongo collection object
        - a_dict: a dict
        - use_key_as_id: (bool) uses dictionaries keyw as _id
    """
    bulkops = muBulkOps(a_collection, ordered=False, ae_n=1000)
    for k, v in a_dict.items():
        bulkops.insert(dict(v, **{'_id': k}) if a_dict else v)
    bulkops.execute_if_pending()
    return a_collection


def oid_date_range_filter(dt_from=None, dt_upto=None, field_name='_id'):
    """
    constructs a range query usefull to query an ObjectId field by date
    :Parameters:
        - dt_from (datetime or tuple): starting date_time if tuple a datetime is constucted from tuple
        - dt_upto (datetime or tuple): end date_time if tuple a datetime is constucted from tuple
        - field_name: (str): optional default to '_id' field to query or None if None returns range only else returns full query
    :Returns:
        - range query (due to objectId structure $gt includes dt_from) while $lt dt_upto (not included)
    """
    def dt(dt_or_tuple):
        if isinstance(dt_or_tuple, datetime):
            return dt_or_tuple
        elif isinstance(dt_or_tuple, tuple):
            return datetime(*dt_or_tuple)
        else:
            raise TypeError('dt must be a date or tuple')
    q = SON()
    if dt_from is not None:
        q.update(SON([('$gte', ObjectId.from_datetime(dt(dt_from)))]))
    if dt_upto is not None:
        q.update(SON([('$lte', ObjectId.from_datetime(dt(dt_upto)))]))
    return q if field_name is None else SON([(field_name, q)])


def OID_gaps(col=None, ObjectId_field="_id", threshold_secs=60):
        """ finds gaps in collections Object_id)
        """

        frmt = "|docs:{:12,d}|of {:12,d}| %: {:4.4f}|"
        frmtgap = "|{}|{}|from:{}|to:{}|gap ddd-hh-mm:ss: {}|"
        res = DotDot({'count': float(col.count()), 'cnt': 0, 'gaps': []})
        step = int(res.count/100)
        curs = col.find(sort=[(ObjectId_field, 1)], projection={ObjectId_field: 1})
        last_doc_gt = curs[0][ObjectId_field].generation_time
        first_doc_gt = curs[0][ObjectId_field].generation_time
        res.dt_start = first_doc_gt
        for doc in curs:
            res.cnt += 1
            if res.cnt == 1 or res.cnt % 100000 == step:
                print(frmt.format(res.cnt, int(res.count), 100 * (res.cnt/res.count)))
            doc_time = doc[ObjectId_field].generation_time
            timedelta_secs = abs((doc_time - last_doc_gt).total_seconds())
            if timedelta_secs > threshold_secs:
                r = (ObjectId_field, str(doc[ObjectId_field]), last_doc_gt, doc_time, seconds_to_DHMS(timedelta_secs))
                print (frmtgap.format(*r))
                res.gaps.append(r)
            last_doc_gt = doc_time
        res.seconds_total = timedelta_secs = abs((doc_time - first_doc_gt).total_seconds())
        res.docsPerSecond = res.count / res.seconds_total
        res.dt_end = last_doc_gt
        for g in res.gaps:
            print (frmtgap.format(*g))
        return res


def db_counts(mongo_client, verbose=True):
    """ returns document counts for each collection in client's db
    """
    res = {}
    for db in mongo_client.database_names():
        res[db] = {}
        for col in mongo_client[db].collection_names():
            res[db][col] = mongo_client[db][col].count()
    return res