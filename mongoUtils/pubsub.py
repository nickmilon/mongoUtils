"""publish and/or subscribe to a collection
"""

from datetime import datetime
from pymongo.errors import AutoReconnect
from pymongo.cursor import CursorType
from pymongo import collection, ReturnDocument
from bson.objectid import ObjectId
from bson import SON, CodecOptions
from time import sleep
from mongoUtils.helpers import AuxTools, db_capped_set_or_get, MongoUtilsError
from mongoUtils.aggregation import Aggregation
from Hellas.Delphi import auto_retry
from Hellas.Sparta import DotDot, EnumLabels


class MsgState(EnumLabels):
    """An enum used to reflect message state used by classes :class:`Sub` and :class:`PubSub`
    """
    UKNKOWN = 0
    """unknown state"""
    SENT = 1
    """message is unprocessed"""
    RECEIVED = 2
    """message has been picked up for Processing"""
    SUCCES = 3
    """message processed successfully"""
    FAIL = 4
    """message processed but failed or (any int > 10 < 100 is considered as error type)"""


class Acknowledge(EnumLabels):
    """An enumeration used while requesting acknowledgement to a message used by class :class:`PubSub`."""
    NO = 0
    """requires NO acknowledgement (kind of broadcast message)"""
    RECEIPT = 1
    """acknowledge by setting state after receiving the message"""
    RESULTS = 2
    """acknowledge by setting state and issuing a result message with original as parent"""


class SubTarget(EnumLabels):
    """An enumeration used when subscribing to messages it is also possible to specify a string expression
    used by class :class:`~pubsub.PubSub`.
    """
    ANY = 1
    """Listen to any targets"""
    NAME = 2
    """Listen only to Messages targeting (by name) this target """
    NAME_OR_ANY = 3
    """combination of 1 or 2 above"""
    NAME_RX = 4
    """Listen to messages targeting names starting with Name (Regex)"""


class MongoUtilsPubSubError(MongoUtilsError):
    """Base class for all MongoUtils exceptions."""


class Sub(object):
    """**generic class for subscribing to a collection
    useful for implementing task-message queues and oplog tailing**
    `see here <https://softwaremill.com/mqperf/>`_
    it can also be used with non capped collections except it must use polling instead of a tailing cursor
    until it has at least one document stored
    i.e. to view your local  see: example test_SubToCappedOptLog
    to replay the oplog: set database to 'local' collection to 'oplog.rs'and track_field to 'ts'

    :Parameters:
        - a_collection: (obj) a pymongo collection
        - track_field: (str) optional the name of field to base the query (defaults to None)
              - it must be a first level field i.e. contains no dots
              - it's values must be monotonic increasing
              - it must be an indexed field (especially for large collections)
              - if None the instance will try to get one by examining the documents in collection
        - name:    (str) a name for this instance if not given defaults to db.name|collection.name

    :Raises:
        - MongoUtilsPubSubError if a track_field is not provided and can't be obtained automatically
        - MongoUtilsPubSubError if track field contains dots
    """
    def __init__(self, a_collection, track_field=None, name=None):
        self._collection = a_collection
        self._capped = self._collection.options().get('capped')
        if track_field is None:
            track_field = self._suggest_track_field()
        else:
            if track_field.find('.') > -1:
                raise MongoUtilsPubSubError('track_field is not first level')
        self._track_field = track_field
        self._name = "{:7s}|{:8s}".format(self._collection.db.name, self._collection.name) if name is None else name
        if track_field is None:
            raise MongoUtilsPubSubError('no track_field')
        self._dt_utc_start = datetime.utcnow()
        self._continue = True

    def _suggest_track_field(self):
        doc_first = self._collection.find_one()
        if doc_first is not None and self._capped is True and 'ts' in doc_first.keys():
            return 'ts'
        if doc_first is not None and isinstance(doc_first['_id'], ObjectId):
            return '_id'
        return None

    @property
    def name(self):
        return self._name

    def _init_query(self, start_from_last=True):
        """oplog_replay query option needs {'$gte' or '$gt': ts}

        :Parameters:
            - start_from_last [True|False|value] (defaults to True
                - on next inserted document if True
                - from 1st document if False  i.e kind of replay
                - on next document after self._track_field=number if number
        """
        doc = None
        if start_from_last is True:
            doc = self._collection.find_one(sort=[("$natural", -1)])
        elif start_from_last is False:
            doc = self._collection.find_one(sort=[("$natural", 1)])
        else:  # then must be a value
            doc = self._collection.find_one({self._track_field: start_from_last})
        track_field_val = doc[self._track_field] if doc is not None else None
        if track_field_val:
            return {self._track_field: {'$gt' if start_from_last is True else '$gte': track_field_val}}
        else:
            return {}

    def _projection_validate(self, projection):
        if projection is None:
            return None
        if isinstance(projection, (list, tuple)):
            projection = {i: 1 for i in projection}
        projection.update({self._track_field: 1})
        return projection

    def _get_cursor(self,  query={}, projection=None, start_from_last=True):
        query.update(self._init_query(start_from_last=start_from_last))
        if self._capped:
            cursor = self._collection.find(query, projection=projection,        # No hint for this type of cursor
                                           cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=self._track_field == True)
        else:
            cursor = self._collection.find(query, projection=self.projection, sort=[('$natural', -1)])
            cursor.hint([('$natural', -1)])
            if start_from_last is True:
                cursor.skip(cursor.count())
        return cursor

    @auto_retry(AutoReconnect, 6, 0.5, 1)
    def tail(self, query, projection=None, start_from_last=True, sleep_secs=0.001, filter_func=lambda x: x):
        """
        subscribe to a capped collection via a tailing cursor. Method is thread safe.
        :Parameters:
            - query a pymongo filter dictionary to filter results
            - start_from_last [True|False|value] (defaults to True
                - True: on next inserted document
                - False: from 1st document  i.e kind of replay
                - value: on next document after self._track_field=value if any other value
            - sleep_secs: (int or float) seconds to wait on cursor StopIteration
                - a small number (0 - 0.001) makes it more responsive a bigger one (0.01 - 1) more efficient
            - filter_func: a function to filter/modify returned docs (defaults to lambda x: x)
                - if filter function returns None doc is skipped
        """
        projection = self._projection_validate(projection)
        retryOnDeadCursor = True
        retry = True
        if start_from_last is True and self._collection.count() == 0:  # last doen't apply
            start_from_last = False
        while retry and self._continue:
            cursor = self._get_cursor(query, projection, start_from_last)
            # print  ("cursor {} {}".format(cursor.alive, 'X1X1'))
            while cursor.alive and self._continue:
                try:
                    doc = next(cursor)
                    filtered_doc = filter_func(doc)
                    if filtered_doc is not None:
                        yield filtered_doc
                except StopIteration:
                    sleep(sleep_secs)
            # self.start_from_last = False  # @note: since we got here, skip it is meaningless
            if retryOnDeadCursor:
                sleep(1)  # collection is empty or something  print ("retryOnDeadCursor")
            else:
                retry = False
        self.tail_exit(cursor)

    def poll(self, query={}, projection=None, start_from_last=True, sleep_secs=1, filter_func=lambda x: x, limit=100):
        """
        subscribe by poll in case collection is not capped or when response time is not critical,
        instead of a tailing cursor we can use poll. Method is thread safe.

        :Parameters:
            - limit: (int) number of documents to return in each batch
                - set it to an appropriate number according to use case, a small number (1-10) makes it more responsive
                  a larger value (100 - 1000) makes it more efficient
            - see :meth:`tail` method for other parameters
        """
        projection = self._projection_validate(projection)

        @auto_retry(AutoReconnect, 6, 0.5, 1)
        def next_batch():
            docs = self._collection.find(query, sort=[(self._track_field, 1)],
                                         projection=projection, limit=limit)
            doc_last = None
            for doc in docs:
                doc_last = doc
                filtered_doc = filter_func(doc)
                if filtered_doc is not None:
                    yield filtered_doc
            if doc_last is not None:
                query[self._track_field] = {'$gt': doc_last[self._track_field]}

        query.update(self._init_query(start_from_last))
        # self._continue = True
        while self._continue:
            for d in next_batch():
                yield d
            sleep(sleep_secs)

    def sub(self, *args, **kwargs):
        """wrapper around poll and tail, it uses tail if collection is capped else poll"""
        return self.tail(*args, **kwargs) if self._capped else self.poll(*args, **kwargs)

    def tail_exit(self, cursor):
        """called when tail exits useful only for debugging i.e. check cursor state etc"""
        pass

    def stop(self):
        """stops subscription"""
        self._continue = False

    def restart(self):
        self._continue = True

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.name)


class PubSub(Sub):
    """**generic class for Publishing/Subscribing to a collection**

    `see here <https://softwaremill.com/mqperf/>`_
    it can also be used with non capped collections except it must use polling instead of a tailing cursor

    .. Warning:: In case you use this class to tail the oplog
                    Make sure you DO NOT attempt writing to oplog collection
                    also that you understand potential side effects as described
                    `here <https://www.mongodb.com/blog/post/\
                    pitfalls-and-workarounds-for-tailing-the-oplog-on-a-mongodb-sharded-cluster>`_


    .. Seealso:: more info `here <http://blog.pythonisito.com/2013/04/mongodb-pubsub-with-capped-collections.html>`__
                 and `here <https://github.com/rick446/MongoTools/blob/master/mongotools/pubsub/channel.py>`__

    :Parameters:
        - collection_or_name: (obj or str) a pymongo collection or a string
        - db:      (obj optional) a pymongo db instance only needed if collection_or_name is a string
        - name:    (str) a name for this instance if not given defaults to db.name|collection.name
        - capped:  (bool optional) set to True to get a capped collection
        - reset:   (bool) drops & recreates collection and resets id counters if True
        - size:    (int) capped collection size in bytes
        - max_docs:(int) capped collection max documents count
    """
    _max_name_len = 32
    _reserve_name = " " * _max_name_len  # reserved bytes in a document to ensure it will not grow
    _dt_frmt_info = "{} {:%Y-%m-%d %H:%M:%S %f}"

    def __init__(self, collection_or_name, db=None, name=None,
                 capped=True, reset=False,
                 size=2 ** 30,  # ~1 GB
                 max_docs=None):
        self._coll_init_specs = {'capped': capped, 'size': size, 'max_docs': max_docs}
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
        a_collection = self._create_collection()
        print "self._track_field" * 10
        super(PubSub, self).__init__(a_collection=a_collection, track_field='ts', name=name)
        if len(self._name) > self._max_name_len:
            raise MongoUtilsPubSubError("name can't be greater than {:2d} chars".format(self._max_name_len))
        a_collection.ensure_index("ts", background=True)
        self._ackn_delay = 0

    def reset(self):
        """drops collection and resets sequence generator"""
        self.db.drop_collection(self._col_name)
        self.aux_tools.sequence_reset(self._col_name)
        self._collection = self._create_collection()

    @property
    def ackn_delay(self):
        return self._ackn_delay

    @ackn_delay.setter
    def ackn_delay(self, millis=0):
        """used by :meth:`_acknowledge_received` method.
        Although subscriptions are somehow auto balancing,
        this can be used as a throttling mechanism when more than one clients can compete for same message
        setting it to a higher value when for example cpu activity is high increases the chances that the message will
        be picked by an other instance possibly running in an other machine or process which is not overloaded

        :Parameters:
            - millis (int or float  >=0) in milliseconds
        """
        self.self._ackn_delay = millis

    def _create_collection(self):
        specs = self._coll_init_specs
        if specs['capped'] is True:
            a_collection = db_capped_set_or_get(self.db, self._col_name, specs['size'], specs['max_docs'])
        else:
            a_collection = self.db[self._col_name]
        opts = CodecOptions(document_class=SON)
        a_collection = a_collection.with_options(codec_options=opts)
        return a_collection

    def _id_next(self):
        return self.aux_tools.sequence_next(self._col_name)

    def _acknowledge(self, fltr, up):
        return self._collection.find_one_and_update(fltr, up, upsert=False, return_document=ReturnDocument.AFTER)

    def _acknowledge_received(self, msg):
        """marks doc as received we check state to make sure than it was not picked by another client meanwhile
        """
        if self._ackn_delay > 0:
            sleep(self._ackn_delay)
        fltr = {'_id': msg['_id'], 'status.state': MsgState.SENT}
        up = {'$set': {'status.state': MsgState.RECEIVED, 'dt.received': datetime.utcnow(),
                       'status.receivedBy': self._name}}
        return self._acknowledge(fltr, up)

    def _yield_doc(self, msg):
        """descendants should check the doc and return None if don't want to yield it"""
        return msg if msg['ackn'] == Acknowledge.NO else self._acknowledge_received(msg)

    def acknowledge_done(self, msg, state=MsgState.SUCCES):
        fltr = {'_id': msg['_id'], 'status.state': MsgState.RECEIVED, 'status.receivedBy': self._name}
        up = {'$set': {'status.state': state, 'dt.completed': datetime.utcnow()}}
        rt = self._acknowledge(fltr, up)
        if rt is None:
            raise MongoUtilsPubSubError('message not found')
        return rt

    @auto_retry(AutoReconnect, 6, 1, 1)  # todo: check new pymongo errors
    def _insert_msg(self, payload, topic, verb, target, state, ackn, parent=0, sentBy=None):
        """we use SON to ensure order so we can index properly
        """
        if sentBy is None:
            sentBy = self.name
        ts = self._id_next()
        _id = SON([('id', ts), ('parent', parent)])
        address = SON([('topic', topic), ('verb', verb), ('target', target)])
        dt = SON([('sent', datetime.utcnow()), ('received', datetime(1900, 1, 1)), ('completed', datetime(1900, 1, 1))])
        status = SON([('state', state), ('sentBy', sentBy),
                      ('receivedBy', self._reserve_name)])  # reserve space so document will not grow on update
        msg = SON([('_id', _id), ('ts', ts),  ('ackn', ackn), ('address', address),
                   ('status', status), ('dt', dt), ('payload', payload)])
        return self._collection.insert_one(msg)

    def pub(self, payload, topic='', verb='', target=None, ackn=Acknowledge.RECEIPT, sentBy=None):
        """
        :Parameters:
            - payload: (dict) message body
            - topic: message topic
            - verb: message verb
            - target: str or None specifies target(s) name
            - ackn: Request acknowledge see: :class: Acknowledge class
            - sendBy: str or None identifies sender (if None defaults to instance name)
        """
        return self._insert_msg(payload, topic, verb, target, state=MsgState.SENT, ackn=ackn, sentBy=sentBy)

    def _query(self, topic=None, verb=None, target=True, state=MsgState.SENT):
        def update_son(key, val):
            if val is not None and val != '':
                qson.update({key: val})

        if target == SubTarget.ANY:
            target = None
        elif target == SubTarget.NAME:
            target = self._name
        elif target == SubTarget.NAME_OR_ANY:
            target = {'$or': [self._name, None, '']}
        elif target == SubTarget.NAME_RX:
            target = {'$regex': '^' + self._name + '.'}
        qson = SON()
        update_son('address.topic', topic)
        update_son('address.verb', verb)
        update_son('address.target', target)
        update_son('status.state', state)
        return qson

    def tail(self, topic=None, verb=None, target=SubTarget.NAME,
             projection=None, start_from_last=True, sleep_secs=0.01):
        """subscribe by tail

        :Parameters:
            - topic: any arbitrary value specifying a topic or None
            - topic: any arbitrary value specifying a verb or None
            - target: a value specifying target listener or None
              see  :class:`~pubsub.SubTarget` class
            - projection: a pymongo projection specifying which fields to return or None
            - start_from_last: see :meth:`Sub.tail` method
            - delay_secs  see :meth:`Sub.tail` method
        """
        query = self._query(topic, verb, target, None)
        return super(PubSub, self).tail(query, projection=projection, start_from_last=start_from_last,
                                        sleep_secs=sleep_secs, filter_func=self._yield_doc)

    def poll(self, topic=None, verb=None, target=SubTarget.NAME,
             projection=None, start_from_last=True, sleep_secs=1, limit=10):
        """subscribe by poll

        :Parameters: see methods :meth:`Sub.poll`  and :meth:`PubSub.tail`
        """

    def _tail_adhoc(self, *args, **kwargs):
        """bypass protocol and tails as defined by parent - used for testing"""
        return super(PubSub, self).tail(*args, **kwargs)

    def _poll_adhoc(self, *args, **kwargs):
        """bypass protocol and poll as defined by parent - used for testing"""
        return super(PubSub, self).poll(*args, **kwargs)

    @classmethod
    def msg_info(cls, msg):
        """returns dictionary with human readable info about a message"""
        res = DotDot(msg.copy())
        secs = DotDot()
        secs.receive = (res.dt.received - res.dt.sent).total_seconds() if res.status.state > MsgState.SENT else -1
        secs.done = (res.dt.completed - res.dt.received).total_seconds() if res.status.state > MsgState.RECEIVED else -1
        res['seconds'] = secs
        for k, v in list(msg['dt'].items()):
            res['dt'][k] = cls._dt_frmt_info.format(k, v)
        # print "check 123 ", (res.status.state, MsgState.RECEIVED)

        res['status']['receivedBy'] = MsgState.value_name(res.status.receivedBy.strip())
        res['status']['state'] = MsgState.value_name(res.status.state)

        res.ackn = Acknowledge.value_name(res.ackn)
        res['address']['target'] = SubTarget.value_name(res.address.target)
        return res


class PubSubStats(object):
    def __init__(self, collection):
        self.collection = collection
        self.cache = {}

    def _aggr(self):
        return Aggregation(self.collection)

    def job_status(self, name=None, match=None, fields_list=['address.topic', 'status.state']):
        if name is None:
            name = 'job_status'
        res = self.cache.get(name)
        if res is not None:
            return res
        aggr = self._aggr()
        if match is not None:
            aggr.match(match)
        aggr.group({'_id': aggr.construct_fields(fields_list), 'count': {'$sum': 1}})
        aggr.sort({'_id.status_state': 1})
        self.cache[name] = aggr
        return aggr

    def responce_stats(self, name=None, match={}, group=None):
        if name is None:
            name = 'responce_stats'
        res = self.cache.get(name)
        if res is not None:
            return res
        aggr = Aggregation(self.collection)
        match.update({'status.state': {'$gt': MsgState.SENT}})
        aggr.match(match)
        aggr.project({'_id': '$_id', 'state': '$status.state', 'rMillis': {'$subtract': ["$dt.received", "$dt.sent"]}})
        aggr.group(aggr.construct_stats(['rMillis']))
        self.cache[name] = aggr
        return aggr

    def mesgs_persec(self, query={'status.state': {'$gt': MsgState.SENT}}):
        res = DotDot({'msgs': -1, 'seconds': -1, 'msgsPerSec': -1})
        msg_first = self.collection.find_one(query, sort=[("$natural", 1)])
        if msg_first is not None:
            msgs = self.collection.find(query, sort=[("$natural", -1)])
            if msg_first['_id'] != msgs[0]['_id']:
                res.msgs = msgs.count()
                res.seconds = (msgs[0]['dt']['received'] - msg_first['dt']['received']).total_seconds()
                res.msgsPerSec = 0 if res.seconds == 0 else res.msgs / res.seconds
        return res
