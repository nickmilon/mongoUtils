'''
Created on Feb 13, 2016

@author: @nickmilon
some trivial benchmarks 
nothing fancy just basic testing of server's speed 
'''

import random
from mongoUtils.client import muClient
from mongoUtils.aggregation import Aggregation
from mongoUtils.helpers import muDatabase, muCollection
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
from datetime import datetime
from Hellas.Sparta import DotDot 
from pprint import pprint


class muBMClient(muClient): 
    def cl_init_complete(self):
        pass

    def bm_inserts_01(self, db_name='test', count=1000000, fields=10, w=0, drop_collection=False, verbose=True):
        collection_name = 'bm_iserts_01'
        doc = {'f_' + str(i): i for i in range(0, fields)}
        if drop_collection:
            self[db_name].drop_collection(collection_name)
        coll = muCollection(database=self[db_name], name=collection_name,
                            read_preference=ReadPreference.PRIMARY_PREFERRED, write_concern=WriteConcern(w=w)) 
        calcs = DotDot()
        calcs.coll_docs_on_start = coll.count()
        calcs.dt_start = datetime.utcnow()
        cnt = 0
        while cnt < count:
            doc['_id'] = calcs.coll_docs_on_start + cnt
            rt = coll.insert_one(doc)
            cnt += 1
        calcs.dt_end = datetime.utcnow()
        calcs.seconds = (calcs.dt_end-calcs.dt_start).total_seconds()
        calcs.inserts_per_sec = int(count/calcs.seconds)
        calcs.coll_docs_on_end = coll.count()
        calcs.docs_inserted = calcs.coll_docs_on_end - calcs.coll_docs_on_start
        calcs.docs_missed = count - calcs.docs_inserted  
        if verbose:
            pprint(calcs)
        return calcs, coll

    def bm_reads_01(self, db_name='test', count=1000000, id_only=False,  verbose=True):
        collection_name = 'bm_iserts_01' 
        coll = muCollection(database=self[db_name], name=collection_name,
                            read_preference=ReadPreference.PRIMARY_PREFERRED)
        calcs = DotDot()
        calcs.coll_docs_on_start = coll.count()
        calcs.docs_missed = 0
        calcs.dt_start = datetime.utcnow()
        projection = {'_id': 1} if id_only else None
        cnt = 0
        while cnt < count: 
            rand_id = random.randint(0, calcs.coll_docs_on_start)
            doc = coll.find_one({'_id': rand_id}, projection=projection)
            if doc is None:
                calcs.docs_missed += 1
            cnt += 1
        calcs.dt_end = datetime.utcnow()
        calcs.seconds = (calcs.dt_end-calcs.dt_start).total_seconds()
        calcs.reads_per_sec = int(count/calcs.seconds)
        calcs.coll_docs_on_end = coll.count()
        if verbose:
            pprint(calcs)
        return calcs, coll
 
 
 