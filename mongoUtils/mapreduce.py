"""map reduce operations

.. Note:: | `see mongodb manual <http://docs.mongodb.org/manual/core/map-reduce/>`_
          | optimize by sorting on emit field
            `see notes <http://edgystuff.tumblr.com/post/7624019777/optimizing-map-reduce-with-mongodb>`_
          | but also see `my ticket <https://jira.mongodb.org/browse/SERVER-16544>`_ (probably solved after v3)
          | when mr output is {'inline':1 } MAX output size is 16MB (Max BOSON doc size)
          | when map reduce runs on a replica secondary only output option is 'inline'

"""

from bson.code import Code
from bson import SON

from mongoUtils.helpers import parse_js_default
from mongoUtils.helpers import coll_index_names


def mr(
    coll,                       # a pymongo collection instance
    fun_map,                    # js function used for map
    fun_reduce=None,            # js function used for reduce defaults to one counting values
    query={},                   # a pymongo query dictionary to query coll defaults to {}
    out={"replace": 'mr_tmp'},  # output dict {'replace'|'merge'|'reduce'|'inline':collection_name|'database':db_name}
    fun_finalize=None,          # js function to run on finalize
    scope={},                   # vars available during map-reduce
    sort=None,                  # i.e: sort= { "_id":1 } short dict to sort before map
    jsMode=False,               # True|False (don't convert to Bson between map & reduce if True)
    verbose=1                   # if 1 includes timing info on output if 2,3  more details
        ):
    """simplified generic Map Reduce
    `see MongoDB Map Reduce <http://docs.mongodb.org/manual/reference/command/mapReduce/>`_

    :Parameters:
      - coll (object) a pymongo collection instance
      - fun_map js function used for map
      - fun_reduce js function used for reduce defaults to a function that increments value count
      - query a pymongo query dictionary to query collection, defaults to {}
      - out  a dictionary for output specification  {replace|merge|reduce|:collection_name|db:db_name}
        also can specify {'inline':1} for in memory operation (with some limitations)
        defaults to {"replace": 'mr_tmp'}
      - scope vars available during map-reduce-finalize
      - sort dictionary to sort before map  i.e: sort= { "_id":1 }
      - jsMode True|False (don't convert to Bson between map & reduce if True)
        should be False if we expect more than 500K distinct results
      - db' (optional): database_name
        if no db is specified output collection will be in same db as input coll

    :Returns: tuple (results collection or results list if out={"inline" :1}, MR response statistics)
    :Example: see :func:`group_counts` function
    """
    def mr_cmd():
        """returns the actual command from output parameter"""
        return [i for i in ['replace', 'merge', 'reduce', 'inline']
                if i in list(out.keys())][0]
    command = mr_cmd()
    out_db = out.get('db', None)
    out = SON([(command, out[command]), ('nonAtomic', out.get('nonAtomic', False))])
    if out_db:
        out.update(SON([('db', out_db)]))
#     out = SON([(command, out[command]), ('db', out.get('db')),
#                    ('nonAtomic', out.get('nonAtomic', False))])
        # nonAtomic not allowed on replace
    fun_map = Code(fun_map, {})
    if fun_reduce is None:
        fun_reduce = parse_js_default('MapReduce.js', 'GroupCountsReduce')
    fun_reduce = Code(fun_reduce, {})

    if sort:
        sort = SON(sort)
    if verbose > 1:
        frmt = "Map Reduce {}\n\
                collection={coll!s:}\n\
                query=     {query!s:}\n\
                sort=      {sort!s:}\n\
                scope=     {scope!s}\n\
                out=       {out!s:}\n\
                jsMode=    {jsMode!s:}\n\
                map=       {fun_map!s:}\n\
                reduce=    {fun_reduce!s:}\n\
                finalize=  {fun_finalize!s:}\n"
        print(frmt.format('Starting...', **locals()))
    r = coll.map_reduce(fun_map, fun_reduce, out=out, query=query,
                        finalize=fun_finalize, scope=scope, sort=sort,
                        full_response=True, jsMode=jsMode)
    if verbose > 0:
        frmt = "Map Reduce {}\n\
                ok=        {ok:}\n\
                millisecs = {timeMillis:,d}\n\
                counts=    {counts!s:}\n\
                out=       {!s:}\n"
        print(frmt.format('End', out, **r))

    if command == 'inline':
        # if 'results' in list(r.keys()):              # @note:  results is a list if inline else a coll
        results = r['results']
        del r['results']
    else:
        db_out = out.get('db', coll.database.name)     # if db not specified it is the collection_db
        results = coll.database.client[db_out][out[command]]
    return results, r


def group_counts(
        collection,
        field_name="_id",
        query={},
        out={"replace": 'mr_tmp'},
        sort=None,
        jsMode=False,
        verbose=1):
    """group values of field using map reduce

    :Parameters: see :func:`mr` function

    :Example:
        >>> from pymongo import MongoClient;from mongoUtils.configuration import testDbConStr      # import MongoClient
        >>> db = MongoClient(testDbConStr).get_default_database()                                  # get test database
        >>> col, res = group_counts(db.muTest_tweets_users, 'lang', out={"replace": "muTest_mr"})  # execute MR
        >>> res                                                                                    # check MR statistics
        {u'counts': {u'input': 997, u'reduce': 72, u'emit': 997, u'output': 21},
        u'timeMillis': 256, u'ok': 1.0, u'result': u'del_1'}
        >>> for i in col.find(sort=[('value',-1)]): print i                                        # print MR results
        {'_id': 'en', 'value': 352.0}
        {'_id': 'ja', 'value': 283.0}
        {'_id': 'es', 'value': 100.0}
        >>>
    """
    FunMap = parse_js_default('MapReduce.js', 'GroupCountsMap', field_name)
    return mr(collection, FunMap, query=query, out=out, sort=sort, verbose=verbose, jsMode=jsMode)


def mr2(
        operation,          # one of 'Orphans' or 'Join'
        col_a,
        col_a_key,
        col_b,
        col_b_key,
        col_a_query=None,
        col_b_query=None,
        db=None,
        out=None,
        sort_on_key_fields=False,
        jsMode=False,
        verbose=3):
    """A kind of sets operation on 2 collections
    Map Reduce two collection objects (col_a, col_b) on a common field (col_a_key, col_b_key)
    allowing queries (col_a_query, col_b_query)

    :Parameters:
        - col_a, col_b: pymongo collection objects
        - col_a_key col_b_key: name of fields to run MR for col_a & col_b
        - col_a_query, col_b_query optional queries to run on respective collections
        - db optional db name to use for results (use 'local' to avoid replication on results)
        - out: optional output collection name defaults to mr_operation
        - sort_on_key_fields: (bool) tries to sort on key if key has an index this is supposed to speed up MR
        - jsMode (True or False) (see mongo documentation)

    :Returns:
        a tuple(results_collection collection, MapReduce1 statistics, MapReduce2 statistics)
    :results_collection:
        - if operation == 'Orphans':
            - {'_id': 'XXXXX', 'value': {'A': 2.0, 'sum': 3.0, 'B': 1.0}}
            - value.a = count of documents in a
            - value.b count of documents in b,
            - sum = count of documents in both A+B
            - to get documents non existing in col_a:
            - >>> resultCollection.find({'value.a':0})
            - to get documents non existing in col_b:
            - >>> resultCollection.find({'value.b':0})
            - to get documents existing in both col_a and col_b
            - >>> resultCollection.find({'value.a':{'$gt':0}, 'value.a':{'$gt': 0}})
            - to check for unique in both collections
            - >>> resultCollection.find({'value.sum':2})
        - if operation = 'Join':
            - performs a join between 2 collections
            - {'_id': 'XXXXX', 'value': {'a': document from col_a,'b': document from col_b}}
            - if a document is missing from a collection its corresponding value is None
            - .. Warning:: document's value will also be None if key exists but is None
                so res[0].find({'value.b': None}} means either didn't exist in collection b
                or its value is None

    :Example:
        >>> from pymongo import MongoClient;from mongoUtils.configuration import testDbConStr      # import MongoClient
        >>> db = MongoClient(testDbConStr).get_default_database()                                  # get test database
        >>> res , stats1, stats2 = mr2('Orphans', db.muTest_tweets, 'user.screen_name',            # execute
                col_b = db.muTest_tweets_users, col_b_key='screen_name',
                col_b_query= {'screen_name': {'$ne':'Albert000G'}}, verbose=0)
        >>> res.find({'value.b':0}).count()                                                        # not found in b
        1
        >>> res.find({'value.b':0})[0]
        {u'_id': u'Albert000G', u'value': {u'a': 1.0, u'sum': 1.0, u'b': 0.0}}
        >>> res = mr.mr2('Join', db.muTest_tweets, 'user.screen_name',                             # execute a Join
                  col_b = db.muTest_tweets_users, col_b_key='screen_name', col_a_query= {},
                  col_b_query= {'screen_name':{'$ne':'Albert000G'}},   verbose=3)
        >>> f = res[0].find({'value.b': None})                                                     # check missing in b
        >>> for i in f: print i['value']['a']['user']['screen_name']
        Albert000G
    """
    if out is None:
        out = 'mr2_' + operation
    map_js = parse_js_default('MapReduce.js', operation + 'Map')
    reduce_js = parse_js_default('MapReduce.js', operation + 'Reduce')
    mr_a = mr(
        col_a,
        map_js % (col_a_key),
        fun_reduce=reduce_js,
        query=col_a_query,
        out={'db': db, 'replace': out} if db else {'replace': out},
        scope={'phase': 1},
        sort={col_a_key: 1} if sort_on_key_fields and col_a_key in coll_index_names(col_a) else None,
        jsMode=jsMode,
        verbose=verbose
        )

    mr_b = mr(
        col_b,
        map_js % (col_b_key),
        fun_reduce=reduce_js,
        query=col_b_query,
        out={'db': db, 'reduce': out} if db else {'reduce': out},
        scope={'phase': 2},
        sort={col_b_key: 1} if sort_on_key_fields and col_b_key in coll_index_names(col_b) else None,
        jsMode=jsMode,
        verbose=verbose
        )
    return mr_b[0], mr_a[1], mr_b[1]
