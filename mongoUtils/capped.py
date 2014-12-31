'''
Created on Dec 22, 2010

@author: nickmilon
'''

from datetime import datetime
from pymongo.errors import AutoReconnect
from pymongo.cursor import _QUERY_OPTIONS
from gevent import sleep, Greenlet, GreenletExit
from mongoUtils.collections import AuxTools, capped_set_or_get
from Hellas.Delphi import auto_retry
from Hellas.Pella import dict_copy


class SubToCapped(object):
    '''
        generic class for Subscribing to a capped collection
        runs as Greenlet so it won't block
        clients should treat all properties as strictly read_only
        set autoJoin to True to start it immediately or .join() the instance it creates
        set func = a function to be executed for each document received or/and override onDoc
        set retryOnDeadCursor to retry if cursor dies as is the case on an empty collection
        until it has at least one document stored
        i.e. to view your local  see: example test_SubToCappedOptLog
        startFrom A ts field value to start after
                  or True to start after next Doc
                  or None to start from beginning (replays all docs)
                  * applies only to collections with ts field
        query a query i.e. {'topic': 'to', 'machine': 'ms'} or None
        retryOnDeadCursor must be True if the collection is empty when we start
        def test_monitorOpLog():
            def log_docs(doc):print  " . . . "* 20, "\n%s" %(str (doc))
            mon = SubToCapped(dbcl.client.local.oplog.rs , func=log_docs)
        @note check here for creating capped collection with ts field
        http://blog.pythonisito.com/2013/04/mongodb-pubsub-with-capped-collections.html
        https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py
    '''
    def __init__(self, cappedCol, fields=None, startFrom=False, func=None,
                 autoJoin=False, retryOnDeadCursor=True, query=None):
        if not cappedCol.options().get('capped'):
            raise ValueError("collection_is_not_capped")
            return
        if func:
            self.onDoc = func
        self.collection = cappedCol
        self.isOpLog = cappedCol.full_name == 'local.oplog.rs'
        self.query = query
        # self.query = None if (isOpLog and startFrom) else query #@note: not compatible with
        # _QUERY_OPTIONS['oplog_replay']  see:database error: no ts field in query
        self.fields = fields
        self.retryOnDeadCursor = retryOnDeadCursor
        self.startFrom = startFrom
        self.id_start = None
        self.id_last = None
        self.t_start = datetime.utcnow()
        self.dt_last = None
        self.docs_fetched = 0
        self.docs_skipped = 0
        self.__stop = False
        # self.start()
        self.glet = Greenlet.spawn(self._run)
        if autoJoin:
            self.join()
        # if autoJoin:self.join()

    def join(self):
        self.glet.join()

    def del_oplogCursorSkipToDoc(self, Doc=None):
        if Doc is None:
            Doc = self.collection.find_one(sort=[("$natural", -1)]).hint([('$natural', -1)])
        currentTs = Doc.get('ts')
        if currentTs:
            queryTs = {'ts': {'$gt': currentTs}} if Doc else None
            cursor = self.collection.find(queryTs, tailable=True, await_data=True)
            cursor.hint([('$natural', -1)])
            cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
            return cursor
        else:
            return None

    def getCursor(self):
        lastDoc = None
        if self.startFrom is True:
            lastDoc = self.collection.find_one(sort=[("$natural", -1)], hint=([('$natural', -1)]))
        elif self.startFrom is None:
            lastDoc = self.collection.find_one(sort=[("$natural", 1)], hint=([('$natural', 1)]))
        elif self.startFrom is not None:
            lastDoc = self.collection.find_one({'ts': self.startFrom})
        if lastDoc:
            currentTs = lastDoc.get('ts')
            if currentTs:
                query = {'ts': {'$gte' if self.startFrom is None else '$gt': currentTs}}
                if self.query is not None:
                    query.update(self.query)
                cursor = self.collection.find(query, fields=self.fields,
                                              tailable=True, await_data=True)
                cursor.hint([('$natural', -1)])
                cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
                return cursor
        cursor = self.collection.find(self.query, fields=self.fields,
                                      tailable=True, await_data=True)
        if self.startFrom:
            self.docs_skipped = cursor.count()
            cursor.skip(self.docs_skipped)
        return cursor

    def timeSinceStart(self):
        return datetime.utcnow() - self.t_start

    def timeSinseLastDoc(self):
        if self.dt_last:
            return datetime.utcnow() - self.dt_last
        else:
            return None

    def docsPerMinuteAvg(self):
        return (self.docs_fetched / self.timeSinceStart().total_seconds()) * 60

    @auto_retry(AutoReconnect, 6, 0.5, 1)
    def _run(self):
        retry = True
        while retry and (not self.__stop):
            cursor = self.getCursor()
            # print ("cursor {} {}".format(cursor.alive, self.__stop))
            while cursor.alive and (not self.__stop):
                try:
                    doc = cursor.next()
                    if not self.id_start:
                        self.id_start = doc.get('_id', True)
                    self.id_last = doc.get('_id', None)
                    self.dt_last = datetime.utcnow()
                    if self.onDoc(doc):
                        self.docs_fetched += 1
                except StopIteration:
                    # print ("StopIteration")
                    sleep()
                except Exception:
                    raise
            self.startFrom = False
            # since we got here, skip is meaningless
            if self.retryOnDeadCursor:
                sleep(1)
            else:
                retry = False
        self.onExit(cursor)

    def onDoc(self, doc):
        '''overide this if you wish or pass a function on init and
           return True if U want to increment docs_fetched
        '''
        return NotImplemented

    def onExit(self, cursor):
        '''overide if you wish, you can check if cursor.alive'''
        print ("onExit")
        pass
        # raise NotImplementedError 

    def stop(self):
        self.__stop = True
        self.glet.kill(exception=GreenletExit, block=True, timeout=None)
        print ("stopped " * 4)


class Del_PubSub(object):
    def __init__(self, client, db_name, col_name, name, size=10000000, maxdocs=None):
        self._client = client
        self._db_name = db_name
        self._name = name
        self._col_name = col_name
        self._col_size_bytes = size
        self._coll_maxdocs = maxdocs
        self.AuxToolsObj = AuxTools(client, dbName=db_name, colName=col_name + '_PB')
        self._capped_set_or_get()

    def _capped_set_or_get(self):
        self.PubSubCol = capped_set_or_get(self._client, self._db_name, self._col_name,
                                           self._col_size_bytes, self._coll_maxdocs)

    def col_reset(self):
        self.col_drop()
        self.self._capped_set_or_get()

    def col_drop(self):
        return self._client[self._db_name].drop_collection(self._col_name)

    @auto_retry(AutoReconnect, 6, 1, 1)
    def _pub(self, topic, verb, payload, target, state, **kwargs):  # channel ?
        doc = dict_copy(locals(), ['self', 'kwargs'])
        ts = self.AuxToolsObj.sequence_next(self._col_name)
        dt = datetime.utcnow()
        doc.update({'_id': ts, 'origin': self._name, 'ts': ts,
                    'RCVby': " " * 10,  # just reserve space
                    'dtCr': dt, 'dtUp': dt})
        rt = self.PubSubCol.insert(doc, **kwargs)
        # self.PubSubCol.update({'_id': rt}, {"$set": {'ts': rt}})
        return rt

    def pub(self, topic, verb, payload, target=None, state=0, async=False, **kwargs):
        if async:
            return Greenlet.spawn(self._pub, topic, verb, payload, target, state, **kwargs)
        else:
            return self._pub(topic, verb, payload, target, state, **kwargs)

    def query(self, topic=None, verb=None, target=True, state=0):
        if target is True:
            target = self._name
        return dict_copy(locals(), ['self'], [None])

    def sub(self, topic=None, verb=None, target=True, state=0, replay=False):
        query = self.query(topic, verb, target, state)
        self.sub_coll = SubToCapped(self.PubSubCol,
                                    func=self._sub,
                                    query=query,
                                    startFrom=None if replay else True
                                    )
        return self.sub_coll

    def _sub(self, doc):
        rt = self._sub_on_doc(doc)
        if rt:
            yield doc

    def poll(self, topic=None, verb=None, target=True, state=0, replay=False):
        """ in cases of low traffic and response time is not critical,
            instead of a tailing cursor we better poll on certain intervals
        """
        def next_batch():
            docs = self.PubSubCol.find(query, sort=[('ts', 1)])
            # print "query ", query
            if docs:
                for doc in docs:
                    rt = self._sub_on_doc(doc)
                    if rt is not None:
                        yield doc
                    query.update({'ts': {'$gt': doc['ts']}})
        query = self.query(topic, verb, target, state)
        if replay is False:
            last_doc = self.PubSubCol.find_one(sort=[("$natural", -1)], hint=([('$natural', -1)]))
            if last_doc:
                query.update({'ts': {'$gt': last_doc['ts']}})
        while True:
            yield next_batch()

    def _sub_on_doc(self, doc):
        """ we check state to make sure than it was not picked by another client meanwhile
        """
        rt = self.PubSubCol.find_and_modify({'_id': doc['_id'], 'state': doc['state']},
                                            {'$inc': {'state': 1},
                                             "$set": {'dtUp': datetime.utcnow(),
                                                      'RCVby': self._name[:10]},
                                             },
                                            upsert=False, new=True, full_response=False
                                            )

        return rt

    def sub_on_doc(self, doc):
        print ("doc=", doc)


class PubSub(object):
    """
        generic class for Subscribing to a capped collection
        can be used with non capped collections except we have to use self.poll(...)
        instead of self.tail(....)
        pubsub_col MUST include a 'ts' field for compatibility with oplog collections
        until it has at least one document stored
        i.e. to view your local  see: example test_SubToCappedOptLog
        to replay oplog collection: set database to 'local' and collection 'oplog.rs'
                                    warning: DO NOT write to oplog
        @note check here for creating capped collection with ts field
        http://blog.pythonisito.com/2013/04/mongodb-pubsub-with-capped-collections.html
        https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py

        Parameters:
            client:  (obj) a pymongo dbclient instance
            db_name: (str) database name
            col_name:(str) collection name
            name:    (str) this instance name, defaults to collection name
            reset:   (Bool) drops & recreates collection and resets id counters if True
            capped:  (Bool)set to True for capped collections
            size:    (int) capped collection size in bytes
            max_docs:(int) capped collection max documents count

    """
    def __init__(self, client, db_name, col_name, name=None,
                 capped=True, reset=False,
                 size=2 ** 30,  # ~1 GB
                 max_docs=None):
        self._name = col_name if name is None else name
        self.client = client
        self._col_name = col_name
        self._capped = capped
        self.db = self.client[db_name]
        self.aux_tools = AuxTools(client, db_name=db_name)
        if reset:
            self.reset()
        if capped is True:
            self.pubsub_col = capped_set_or_get(client, db_name, self._col_name, size, max_docs)
        else:
            self.pubsub_col = self.db[col_name]
        self._iscupped = self.pubsub_col.options().get('capped')
        self.pubsub_col.ensure_index("ts", background=True)
        self._continue = True
        self.counters = {'id_start': None, 'id_cur': None, 'msg_count': 0, 'dt': None}

    def reset(self):
        self.db.drop_collection(self._col_name)
        self.aux_tools.sequence_reset(self._col_name)

    def counters_set(self, id_value):
        if self.counters['id_start'] is None:
            self.counters['id_start'] = id_value
        self.counters['id_cur'] = id_value
        self.counters['msg_count'] += 1
        self.counters['dt'] = datetime.utcnow()

    def _init_query(self, start_after='last'):
        """ oplog_replay query option needs {'$gte' or '$gt' ts}
            Parameters:
                start_from ['last'|'start'|value]
                        on next inserted document if 'last'
                        from 1st document if 'start'  i.e kind of replay
                        on next document after ts=number if number
        """
        doc = None
        # print "locals 111", locals()
        if start_after == 'last':
            doc = self.pubsub_col.find_one(sort=[("$natural", -1)], hint=([('$natural', -1)]))
        elif start_after == 'start':
            doc = self.pubsub_col.find_one(sort=[("$natural", 1)], hint=([('$natural', 1)]))
        else:  # then must be a value
            doc = self.pubsub_col.find_one({'ts': start_after})
        ts = doc['ts'] if doc is not None else 0
        return {'ts': {'$gte' if start_after == 'start' else '$gt': ts}}

    def _get_cursor(self, query={}, fields=None, start_after='last'):
        query.update(self._init_query(start_after=start_after))
        if self._iscupped:
            cursor = self.pubsub_col.find(query, fields=fields,
                                          tailable=True, await_data=True)
            cursor.hint([('$natural', -1)])
            cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
        else:
            cursor = self.pubsub_col.find(query, fields=fields, sort=[('$natural', -1)])
            cursor.hint([('$natural', -1)])
            if start_after != 'start':
                cursor.skip(cursor.count())
        return cursor

    def _id_next(self):
        return self.aux_tools.sequence_next(self._col_name)

    def _acknowledge(self, doc):
        """ marks doc as received
            we check state to make sure than it was not picked by another client meanwhile
        """

        rt = self.pubsub_col.find_and_modify({'_id': doc['_id'], 'state': doc['state']},
                                             {'$inc': {'state': 1},
                                              '$set': {'dtUp': datetime.utcnow(),
                                                       'RCVby': self._name[:10]},
                                              },
                                             upsert=False, new=True, full_response=False
                                             )
        return rt

    @auto_retry(AutoReconnect, 6, 1, 1)
    def _pub(self, topic, verb, payload, target, state=0, ackn=0, **kwargs):
        doc = dict_copy(locals(), ['self', 'kwargs'])
        ts = self._id_next()
        dt = datetime.utcnow()
        doc.update({'_id': ts, 'origin': self._name, 'ts': ts,
                    'RCVby': " " * 10,  # just reserve space
                    'dtCr': dt, 'dtUp': dt})
        return self.pubsub_col.insert(doc, **kwargs)

    def pub(self, topic, verb, payload, target=None, state=0, ackn=0, async=False, **kwargs):
        """ Parameters:
                ackn: (int) request acknowledgement (by incrementing state)
                      0=no acknowledge, > 0 acknowledge
        """
        if async:
            return Greenlet.spawn(self._pub, topic, verb, payload, target, state, ackn, **kwargs)
        else:
            return self._pub(topic, verb, payload, target, state, ackn, **kwargs)

    def query(self, topic=None, verb=None, target=True, state=0):
        if target is True:
            target = self._name
        return dict_copy(locals(), ['self'], [None])

    def sub_poll(self, topic=None, verb=None, target=True, state=0,
                 fields=None, start_after='last'):
        """ subscribe by poll
            in case collection is not capped or when response time is not critical,
            instead of a tailing cursor we can use poll
        """
        @auto_retry(AutoReconnect, 6, 0.5, 1)
        def next_batch():
            docs = self.pubsub_col.find(query, sort=[('ts', 1)], fields=fields)
            if docs:
                for doc in docs:
                    query.update({'ts': {'$gt': doc['ts']}})
                    if doc['ackn'] == 0 or self._acknowledge(doc):
                        self.counters_set(doc['_id'])
                        yield doc

        query = self.query(topic, verb, target, state)
        query.update(self._init_query(start_after))
        while self._continue:
            yield next_batch()

    def sub_tail(self, topic=None, verb=None, target=True, state=0,
                 fields=None, start_after='last'):
        """ subscribe by tail
        """
        query = self.query(topic, verb, target, state)
        return self.tail(query, fields, start_after)

    @auto_retry(AutoReconnect, 6, 0.5, 1)
    def tail(self, query, fields=None, start_after='last'):
        retryOnDeadCursor = True
        retry = True
        while retry and self._continue:
            cursor = self._get_cursor(query, fields, start_after)
            # print ("cursor {} {}".format(cursor.alive, self.__stop))
            while cursor.alive and self._continue:
                try:
                    doc = cursor.next()
                    if doc['ackn'] == 0 or self._acknowledge(doc):
                        self.counters_set(doc['_id'])
                        yield doc
                except StopIteration:
                    # print ("StopIteration")
                    sleep()
                except Exception:
                    raise
            # self.startFrom = False
            # since we got here, skip is meaningless
            if retryOnDeadCursor:
                sleep(1)
            else:
                retry = False
        self.tail_exit(cursor)

    def tail_exit(self, cursor):
        """ used for deluging only i.e. check cursor state etc
        """
        pass

    def stop(self):
        self._continue = False


# ===============================================================================
# def test_SubToCappedOptLog():
#     mconf = mongoConfToPy('mongonm01')
#     dbcl = MdbClient(**mconf)
#     def log_docs(doc):
#     print  ". . . . "* 20, "\n%s" %(str (doc))
#     SubToCapped(dbcl.client.local.oplog.rs , func=log_docs)
# if __name__ == "__main__":
#     test_SubToCappedOptLog()
# ===============================================================================