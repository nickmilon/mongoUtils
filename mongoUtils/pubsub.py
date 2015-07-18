"""publish and/or subscribe to a collection"""

from datetime import datetime
from pymongo.errors import AutoReconnect
from pymongo.cursor import CursorType
from pymongo import collection
from gevent import sleep, Greenlet
from mongoUtils.helpers import AuxTools, db_capped_set_or_get
# capped_set_or_get
from Hellas.Delphi import auto_retry
from Hellas.Pella import dict_copy


class PubSub(object):
    """**generic class for Publishing/Subscribing  to a capped collection
    useful for implementing task-message queues and oplog tailing**

    https://softwaremill.com/mqperf/

    it can also be used with non capped collections except it must use polling instead of a tailing cursor
    collection **must** include a **ts** field for compatibility with oplog collections
    until it has at least one document stored
    i.e. to view your local  see: example test_SubToCappedOptLog
    to replay oplog collection: set database to 'local' and collection to 'oplog.rs'

    .. Warning:: Make sure you DO NOT attempt writing to oplog collection
                also you understand potential side effects as described 
                `here <https://www.mongodb.com/blog/post/pitfalls-and-workarounds-for-tailing-the-oplog-on-a-mongodb-sharded-cluster>`_

    .. Seealso:: more info `here <http://blog.pythonisito.com/2013/04/mongodb-pubsub-with-capped-collections.html>`__
        and `here <https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py>`__

    :Args:

    - collection_or_name: (obj or str) a pymongo collection or a string
    - db:      (obj optional) a pymongo db instance only needed if collection_or_name is a string
    - name:    (str) collection name a name for this instance if not given defaults to db.name|collection.name
    - capped:  (bool optional) set to True for to get a capped collection
    - reset:   (bool) drops & recreates collection and resets id counters if True
    - size:    (int) capped collection size in bytes
    - max_docs:(int) capped collection max documents count
    """

    #===============================================================================
# def test_SubToCappedOptLog():
#     mconf = mongoConfToPy('mongonm01')
#     dbcl = MdbClient(**mconf)
#     def log_docs(doc):
#     print  ". . . . "* 20, "\n%s" %(str (doc))
#     SubToCapped(dbcl.client.local.oplog.rs , func=log_docs)
    def __init__(self, collection_or_name, db=None, name=None,
                 capped=True, reset=False,
                 size=2 ** 30,  # ~1 GB
                 max_docs=None):
        if isinstance(collection_or_name, collection.Collection):
            self._col_name = collection_or_name.name
            self.db = collection_or_name.database
        else:
            self._col_name = collection_or_name
            assert(db is not None)
            self.db = db
        self.aux_tools = AuxTools(db=self.db)
        if reset:
            self.reset()
        if capped is True:
            self.pubsub_col = db_capped_set_or_get(db, self._col_name, size, max_docs)
        else:
            self.pubsub_col = self.db[self._col_name]
        self._name = self.pubsub_col.db.name + '|' + self.pubsub_col.name if name is None else name
        self._capped = self.pubsub_col.options().get('capped')
        self.pubsub_col.ensure_index("ts", background=True)
        self._continue = True
        self.counters = {'id_start': None, 'id_cur': None, 'msg_count': 0, 'dt': None}

    @property
    def name(self):
        return self._name

    def reset(self):
        """drops collection and resets sequence generator"""
        self.db.drop_collection(self._col_name)
        self.aux_tools.sequence_reset(self._col_name)

    def _counters_set(self, id_value):
        if self.counters['id_start'] is None:
            self.counters['id_start'] = id_value
        self.counters['id_cur'] = id_value
        self.counters['msg_count'] += 1
        self.counters['dt'] = datetime.utcnow()

    def _init_query(self, start_after='last'):
        """oplog_replay query option needs {'$gte' or '$gt' ts}

        :Parameters:
            - start_from [last|start|value]
                - on next inserted document if 'last'
                - from 1st document if 'start'  i.e kind of replay
                - on next document after ts=number if number
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
        if self._capped:
            cursor = self.pubsub_col.find(query, fields=fields, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
            cursor.hint([('$natural', -1)])
        else:
            cursor = self.pubsub_col.find(query, fields=fields, sort=[('$natural', -1)])
            cursor.hint([('$natural', -1)])
            if start_after != 'start':
                cursor.skip(cursor.count())
        return cursor

    def _id_next(self):
        return self.aux_tools.sequence_next(self._col_name)

    def _acknowledge(self, doc):
        """marks doc as received
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
        """Parameters:
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
                        self._counters_set(doc['_id'])
                        yield doc

        query = self.query(topic, verb, target, state)
        query.update(self._init_query(start_after))
        while self._continue:
            yield next_batch()

    def sub_tail(self, topic=None, verb=None, target=True, state=0,
                 fields=None, start_after='last'):
        """subscribe by tail"""
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
                        self._counters_set(doc['_id'])
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
        """used for deluging only i.e. check cursor state etc"""
        pass

    def stop(self):
        """stops subscription"""
        self._continue = False
