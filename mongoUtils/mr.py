'''
Created on Jan 12, 20`0

@author: nickmilon
'''

from bson.code import Code
from bson import SON

from Hellas.Thiva import format_header
from mongoUtils.utils import parse_js_default
from mongoUtils.collections import coll_index_names

""" map reduce slow with sort see: https://jira.mongodb.org/browse/SERVER-16544
"""


def mr(
    coll,                       # a pymongo coll instance
    fun_map,                    # js function used for map
    fun_reduce=None,            # js function used for reduce defaults to one counting values
    query={},                   # a pymongo query dictionary to query coll defaults to {}
    out={"replace": 'mr_tmp'},  # output dict {'replace'|'merge'|'reduce'|'inline':collection_name}
    fun_finalize=None,          # js function to run on finalize
    scope={},                   # vars available during map-reduce-finalize
    sort=None,                  # i.e: sort= { "_id":1 } short dict to sort before map
    jsMode=False,               # True|False (don't convert to Bson between map & reduce if True)
    verbose=1                   # if 1 includes timing info on output if 2,3  more details
        ):
    """ simplified generic Map Reduce
        see: http://docs.mongodb.org/manual/reference/method/db.coll.mapReduce/
        * when output is {'inline':1} MAX output size is 16MB (Max BOSON doc size)
        * if map reduce is on a replica secondary only output option is 'inline'
        Args:
            out a dictionary of following format:
            {'replace'|'merge'|'reduce'|'inline': output coll name or 1 for inline,
             db' (optional): database_name
            }
            if no db is specified output collection will be in same db as input coll
            tip: use {'db':'local'} to speed up things on replicated collections
        returns tuple (results collection or results list if out={"inline":1}, MR response object)
        Reduce function defaults to one that increments value count
        optimize by sorting on emit field
        see: http://edgystuff.tumblr.com/post/7624019777/optimizing-map-reduce-with-mongodb
        but also see my ticket on:https://jira.mongodb.org/browse/SERVER-16544
        docs.mongodb.org/manual/reference/method/db.coll.mapReduce/#db.coll.mapReduce
        sort      i.e: sort= { "_id":1 }
        jsMode    should be False if we expect more than 500K distinct results
    """
    def mr_cmd():
        '''returns the actual command from output parameter'''
        return [i for i in ['replace', 'merge', 'reduce', 'inline']
                if i in list(out.keys())][0]
    if len(out.keys()) > 1:
        command = mr_cmd()
        # print ("command", command)
        out = SON([(command, out[command]), ('db', out.get('db')),
                   ('nonAtomic', out.get('nonAtomic', False))])
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
    if 'db' in list(out.keys()):
        # @note:  can be dict or SON, either way it has property keys
        results = coll.database.connection[r['result']['db']][r['result']['coll']]
    else:
        results = r['results'] if list(out.keys())[0] == 'inline' else coll.database[r['result']]
        # @note:  results is a list if inline else a coll
    return results, r


def group_counts(
        collection,
        field_name="_id",
        query={},
        out={"replace": 'mr_tmp'},
        sort=None,
        jsMode=False,
        verbose=1):
    """ group values of field using map reduce (see: 'mr')
        examples:
            r,col=mr_group(a_collection, 'lang', out={"replace": "del_1"}, verbose=3)
            r,col=mr_group(a_collection, 'lang', out={"replace": "del_1", 'db':'local'})
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
    ''' A kind of set operation on 2 collections
        Map Reduce two collection objects (col_a, col_b) on a common field (col_a_key, col_b_key)
        allowing queries (col_a_query, col_b_query)
        Args:
            col_a, col_b  :pymongo collection objects
            col_a_key col_b_key: name of fields to run MR for col_a & col_b
            col_a_query, col_b_query optional queries to run on respective collections
            db optional db name to use for results (use 'local' to avoid replication on results)
            out: optional output collection name defaults to 'mr_'+operation
            sort_on_key_fields(True or False) tries to sort on key if key has an index
                            this is supposed to speed up MR
            jsMode (True or False) (see mongo documentation)
        Returns:a tuple(result_collection collection, MapReduce1 statistics, MapReduce2 statistics)
        result_collection if operation = 'Orphans':
            item form : {u'_id': u'105719173', u'value': {u'A': 2.0, u'sum': 3.0, u'B': 1.0}}
            _id = key
            value.a = count of documents in a,
            value.b count of documents in b,
            sum = count of documents in both A+B
            to get documents non existing in col_a: resultCollection.find({'value.a':0})
            to get documents non existing in col_a: resultCollection.find({'value.b':0})
            to get documents existing in both col_a and col_b
                resultCollection.find({'value.a':{'$gt':0}, 'value.a':{'$gt':0}})
            to check for unique in both collections resultCollection.find({'value.sum':2})
        result_collection if operation = 'Join':
            {'_id': '100001818', u'value': {'a': document from col_a,'b':document from col_b}}
            if a document is missing from a collection its value will be None
        call example:
               mr2("Orphans',bof.TstatusesSrc, '_id', {}, col_b=ag13, col_b_key='_id.vl',
               col_b_query={'_id.kt': 'src'}, out='mr_join', jsMode=False, verbose=3)
    '''
    if out is None:
        out = 'mr2_'+operation
    map_js = parse_js_default('MapReduce.js', operation+'Map')
    reduce_js = parse_js_default('MapReduce.js', operation+'Reduce')
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


def schema(collection,
           query={},
           out={"replace": 'tmpMrFields'},  # does not work with 'inline'
           verbose=2,
           meta=False,
           scope={'parms': {'levelMax': -1, 'inclHeaderKeys': False}}):
    """
        A kind of Schema Analyzer for mongo collections
        A utility which finds all field's names used by documents of a collection
        xxx.floatApprox xxx.bottom', xxx.top = an internal mongoDB field for storing long integers
        for a different approach see: https://github.com/variety/variety
    """
    map_js = parse_js_default('MapReduce.js', 'KeysMap')
    reduce_js = parse_js_default('MapReduce.js', 'KeysReduce')
    rt = mr(
        collection,
        map_js,
        reduce_js,
        query=query,
        out=out,
        scope=scope,
        verbose=verbose)
    results_col, mr_stats = rt
    totalRecords = float(mr_stats['counts']['input'])
    totalCnt = 0
    if verbose > 0:
        print ("calculating percentages ...")
    for doc in results_col.find():
        cnt = doc['value']['cnt']
        percent = (cnt / totalRecords) * 100
        doc['value']['percent'] = percent
        results_col.update(
            {'_id': doc['_id']},
            {"$set": {"value.percent": percent}}, safe=True, multi=False
            )
        # @warning:  don't use {_id:id} does not work possibly coz different subfields order
        # print results_col.find_one({'_id':doc['_id']}, safe=True)
        totalCnt += cnt
    if verbose > 0:
        print ("creating indexes")
    rt[0].ensure_index('_id.type', background=True)
    if meta:
        rtMeta = schema_meta(rt, verbose=verbose)
        return rt, rtMeta
    else:
        return rt


def schema_meta(mr_keys_results, verbose=2):
    """ given the results returned by schema calculates and returns statistics for
        schema fields also pretty prints stats if verbose > 0
        Be aware of hidden mongoDB fields that mongoDB uses internally
        Args: mr_keys_results results tuples returned be schema
              verbose 0 | 1
        Returns:list of containing stats for each field
    """
    map_js = parse_js_default('MapReduce.js', 'KeysMetaMap')
    reduce_js = parse_js_default('MapReduce.js', 'KeysMetaReduce')
    hidden_fields = ['floatApprox', 'top', 'bottom']    # internal mongo fields
    res = mr(
        mr_keys_results[0],
        map_js,
        reduce_js,
        out={'inline': 1},
        verbose=verbose)
    info = {}
    for i in res[0]:
        i['value']['cnt'] = int(i['value']['cnt'])
        i['value']['depth'] = int(i['value']['depth'])
        i['value']['notes'] = ""
        i['value']['field'] = i['_id']
        if i['value']['depth'] > 1:
            if i['_id'].split('.')[-1] in hidden_fields or i['_id'] == '_id.str':
                i['value']['notes'] += "-hidden mongo field"
    l = sorted(res[0], key=lambda x: x['value']['depth'])
    info['max depth'] = l[-1]['value']['depth']
    info['total fields'] = len(l)
    l = sorted(res[0], key=lambda x: x['_id'])
    if verbose > 0:
        frmt = "|{field:^70s}|{cnt:16,d}|{percent:7.2f}|{depth:5d}|{notes:^30s}|"
        header = format_header(frmt)
        print(header)
        for i in l:
            print(frmt.format(**i['value']))
        print(header.split('\n')[0])
        print (info)
    return l
