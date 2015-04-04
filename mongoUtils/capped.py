'''
Created on Dec 22, 2010

@author: nickmilon
'''

from datetime import datetime
from pymongo.errors import AutoReconnect
from pymongo.cursor import _QUERY_OPTIONS
from gevent import sleep, Greenlet
from mongoUtils.collections import AuxTools, capped_set_or_get
from Hellas.Delphi import auto_retry
from Hellas.Pella import dict_copy


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
