"""**Schema Analyzer Utilities for mongoDB collection based on map reduce**"""


from mongoUtils.helpers import parse_js_default
from mongoUtils.mapreduce import mr
from Hellas.Thebes import format_header


def schema(collection,
           query={},
           out = {'replace': 'tmp_mrFields'},
           meta=False,
           scope={'parms': {'levelMax': -1, 'inclHeaderKeys': False}},
           verbose=2
           ):
    """discovers all field's names used by a  a collection's documents
    for a different approach `see here <https://github.com/variety/variety>`_
    also mongoDB will introduce a similar tool
    fields of the form xxx.floatApprox xxx.bottom', xxx.top are internal mongoDB field for storing long integers
    outputs to local db so results don't get replicated

    .. todo::
        - convert it to aggregation with sample
        - include one document fore each field

    :Parameters:
        - collection: a mongoDB collection
        - query: a pymongo query dictionary to filter documents that will be searched
          to a subset of a collection (useful for large collections)
        - out: map reduce output specificatins dictionary (see see :func:`~.mr` function
          except for it can't be inline
        - meta: if True results are passed to :func:`schema_meta` function for analysis
        - scope: a dictionary {'parms': {'levelMax': -1, 'inclHeaderKeys': False}}
            - levelMax: (int)  max level for keys if -1 any level (defaults to -1)
            - inclHeaderKeys: (bool) if True includeds top level keys
        - verbose: (int) if > 0 prints progress and output

    :Example:
        >>> from pymongo import MongoClient;from mongoUtils.configuration import testDbConStr      # import MongoClient
        >>> db = MongoClient(testDbConStr).get_default_database()                                  # get test database
        r = schema(db.muTest_tweets, meta=True, verbose=1)                                         # check fields
        ........................................................................................................
        |                      field                       |      cnt       |percent|depth|       notes        |
        ........................................................................................................
        |                       _id                        |           1,000| 100.00|    1|                    |
        |                   contributors                   |           1,000| 100.00|    1|                    |
        |                   coordinates                    |           1,000| 100.00|    1|                    |
        |             coordinates.coordinates              |              18|   1.80|    2|                    |
        |                 coordinates.type                 |              18|   1.80|    2|                    |
        |                    created_at                    |           1,000| 100.00|    1|                    |
        |                     entities                     |           1,000| 100.00|    1|                    |
        |                entities.hashtags                 |           1,000| 100.00|    2|                    |
        |                  entities.media                  |             196|  19.60|    2|                    |
        |                 entities.symbols                 |           1,000| 100.00|    2|                    |
        |                 entities.trends                  |           1,000| 100.00|    2|                    |
        |                  entities.urls                   |           1,000| 100.00|    2|                    |
        |              entities.user_mentions              |           1,000| 100.00|    2|                    |
        |                extended_entities                 |             196|  19.60|    1|                    |
        |             extended_entities.media              |             196|  19.60|    2|                    |
        |                  favorite_count                  |           1,000| 100.00|    1|                    |
        |                    favorited                     |           1,000| 100.00|    1|                    |
        |                   filter_level                   |           1,000| 100.00|    1|                    |
        |                       geo                        |           1,000| 100.00|    1|                    |
        |                 geo.coordinates                  |              18|   1.80|    2|                    |
        |                     geo.type                     |              18|   1.80|    2|                    |
        |                        id                        |           1,000| 100.00|    1|                    |
        |                    id.bottom                     |           1,000| 100.00|    2|-hidden mongo field |
        |                  id.floatApprox                  |           1,000| 100.00|    2|-hidden mongo field |
        |                  ......etc.......                |           .....| ......|    .|                    |
        ........................................................................................................
        >>>  for i in r[1]: print i:                                                               # print results
        {u'_id': u'', u'value': {'notes': '', 'field': u'', u'cnt': 31, u'percent': 3.1000000000000005, u'depth': 1}}
        etc. etc...
    """
    # out = {'db': 'local', 'replace': output_coll_name}

    map_js = parse_js_default('MapReduce.js', 'KeysMap')
    reduce_js = parse_js_default('MapReduce.js', 'KeysReduce')
    if verbose > 0:
        print ("discovering fields ...")
    rt = mr(
        collection,
        map_js,
        reduce_js,
        query=query,
        out=out,
        scope=scope,
        verbose=0)
    results_col, mr_stats = rt
    totalRecords = float(mr_stats['counts']['input'])
    totalCnt = 0
    if verbose > 0:
        print ("calculating percentages ...")
    for doc in results_col.find():
        cnt = doc['value']['cnt']
        percent = (cnt / totalRecords) * 100
        doc['value']['percent'] = percent
        results_col.update_one(
            {'_id': doc['_id']},
            {"$set": {"value.percent": percent}}
            )
        # @warning:  don't use {_id:id} does not work possibly coz different subfields order
        # print results_col.find_one({'_id':doc['_id']}, safe=True)
        totalCnt += cnt
    if verbose > 0:
        print ("creating indexes")
    rt[0].create_index('_id.type', background=True)
    if meta:
        rtMeta = schema_meta(rt, verbose=verbose)
        return rt, rtMeta
    else:
        return rt


def schema_meta(mr_keys_results, verbose=2):
    """given the results returned by :func:`schema` function  calculates and returns statistics for
    schema fields also pretty prints stats if verbose > 0
    Be aware of hidden mongoDB fields that mongoDB uses internally

    :Parameters:
        - mr_keys_results: results tuples returned be schema
        - verbose 0 | 1
    :Returns: list of statistics for each field
    """
    map_js = parse_js_default('MapReduce.js', 'KeysMetaMap')
    reduce_js = parse_js_default('MapReduce.js', 'KeysMetaReduce')
    hidden_fields = ['floatApprox', 'top', 'bottom']    # internal mongo fields
    res = mr(
        mr_keys_results[0],
        map_js,
        reduce_js,
        out={'inline': 1},
        verbose=0)
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
        frmt = "|{field:70s}|{cnt:16,d}|{percent:7.2f}|{depth:5d}|{notes:^20s}|"
        header = format_header(frmt)
        print(header)
        for i in l:
            print(frmt.format(**i['value']))
        print(header.split('\n')[0])
        print (info)
    return l


def schema_exclude_parents(fields_list, as_string=True):
    """useful for producing fields parameter for mongoexport

    :Parameters:
        - fields_list: a fields_list as produced by :func:`schema` function
        - as_string: True or False, converts output to string if True (default)
    :Returns:
      - last level elements of fields_list
    :Example:
        >>> res, stats = sch.schema(db.muTest_tweets_users,verbose=0)
        >>> res['value']['fields']
        ['_id', '_id.str', 'contributors_enabled', 'created_at', 'default_profile' .... ]
        >>> schema_exclude_parents(res['value']['fields'])
        '_id.str,contributors_enabled,created_at,default_profile,default_profile_image ...
    """
    def is_parent(item):
        return len([i for i in fields_list if i.startswith(item+'.')]) > 0
    rt = [i for i in fields_list if not is_parent(i)]
    return ",".join(rt) if as_string is True else rt


def schema_client(mongo_client, exclude_dbs=['test', 'local', 'admin']):
    for db in mongo_client.database_names():
        if db not in exclude_dbs:
            for col in mongo_client[db].collection_names():
                print ("fields in db {} collection {}".format(db, col))
                schema(mongo_client[db][col], meta=True,  scope={'parms': {'levelMax': -1, 'inclHeaderKeys': False}}, verbose=2)


def mongoexport_fields(file_path, collection, query={}, excl_fields_lst=[]):
    """exports all field names except excl_fields_lst to a file

    :Parameters:
        - file_path: (str) path to output file
        - collection: a pymongo collection object
        - query: a pymongo query dictionary (optional) to restrict fields discovery
          to a subset of a collection (useful for large collections)
        - excl_fields_lst: (list) field names to exclude from output
    :Example:
        >>> mongoexport_fields("/path_to_file", db.muTest_tweets_users,  excl_fields_lst=['_id'])
    """
    r = schema(collection, query=query,  verbose=0)
    r = schema_exclude_parents(r[0].find()[0]['value']['fields'], as_string=False)
    with open(file_path, "w") as fout:
        for i in r:
            fout.write(i + "\n")
    return r
