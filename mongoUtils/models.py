'''
Created on Jul 16, 2016

@author: milon
'''

from datetime import datetime
from time import time
from Hellas.Sparta import DotDot
from pymongo.errors import WriteError, DuplicateKeyError
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import ReadPreference
from bson import CodecOptions, codec_options, SON, objectid


ObjectId = objectid.ObjectId

class MC(object):
    """ mongo constants"""
    id = '_id'
    _in = '$in'
    gt = '$gt'
    gte = '$gte'
    lt = '$lt'
    lte = '$lte'
    bitsAllSet = '$bitsAllSet'


class REST(object):
    statuses = DotDot({200: 'OK',
                       201: 'Created',
                       400: 'Bad Request',
                       404: 'Not Found',
                       409: 'Conflict',
                       412: 'Precondition Failed'
                       })

    def __init__(self):
        self.models_tree = []

    @classmethod
    def is_sublist(cls,  lst_small, lst_big):
        """ same elements same position"""  # inefficient but ok for small lists
        for i, v in enumerate(lst_small):
            if lst_big[i] != v:
                return False
        return True

    @classmethod
    def parse_path(cls, path, path_valid_keys_lst):  # more efficient if we use regex but difficult to understand the flow
        def split_comma(v):
            lst = v.split(',')
            return v if len(lst) == 1 else lst

        path = path.split(".")
        frmt = path[1] if len(path) > 1 else "json"
        path = path[0].split("/")
        if (len(path) % 2 != 0) or (not cls.is_sublist(path[::2], path_valid_keys_lst)):
            return False, False
        values = [split_comma(i) for i in path[1::2]]
        req = SON(zip(path[::2], values))
        return req, frmt

    @classmethod
    def status_get(cls, status_num, msg=''):
        return status_num, "{}. {}".format(cls.statuses.get(status_num, ''), msg)

    @classmethod
    def ops_results_default_start(cls):
        return {'header': {'ts_start': time()}}

    @classmethod
    def ops_results_default_end(cls, results, status_msg=(200, 'OK'), ops_count=1):
        header = results['header']
        duration = time() - header['ts_start']
        header['duration_ms'] = int(duration * 1000)
        header['ops_count'] = ops_count
        header['ops_per_sec'] = int(ops_count / duration)
        header['status'] = status_msg[0]
        header['message'] = status_msg[1] if len(status_msg) == 2 else cls.statuses.get(status_msg[0], '')
        header['dt_start'] = datetime.utcfromtimestamp(header['ts_start']).isoformat("T") + "Z"
        return results


class muModel(object):
    """ redefine following default values in descendants if needed"""
    _validator = None
    _write_concern = 1
    _name = "test_muModel"
    _validation_error_msg_ = 'Document failed validation'
    _rest_field = ('_id', True)  # (field_name, unique)
    _all = '_all'
    ##########################

    def __init__(self, db, parent_model=None, codec_options=None, write_concern=None, read_concern=None, read_preference=None,  drop=False):
        """
        """
        self._db = db
        self._col = None
        self._write_concern = WriteConcern(w=write_concern) if write_concern else None 
        self._read_concern = ReadConcern(level=read_concern) if read_concern else None
        self._codec_options = CodecOptions(document_class=codec_options) if codec_options else None
        self._read_preference = read_preference
        self._relations = {}
        self._parent_model = parent_model
        if drop:
            self._drop()
        self.collection_set()

    def _relation_add(self, rel_name, model, field, parent_field=None):
        """
        :parms:
            - field str:
            - parent_field: tuple: defaults to fields
        """
        if parent_field is None:
            parent_field = field
        self._relations[rel_name] = (model, field, parent_field)

    def _parent_find_one(self, rel_name, value, **kwargs):
        relation = self._relations.get(rel_name)
        if relation:
            return relation[0]._col.find_one({relation[2], value}, **kwargs)
        raise KeyError(rel_name)

    @classmethod
    def cursor_get_first(cls, cursor):
        """ emulates find_one"""
        try:
            return cursor[0]
        except IndexError:
            return None

    def collection_set(self):
        """ inheriting classes should call this on parent then run their own implementation
        """
        if self._name in self._db.collection_names():
            self._col = self._db[self._name].with_options(codec_options=self._codec_options, write_concern=self._write_concern,
                                                          read_concern=self._read_concern, read_preference=self._read_preference)

        else:
            if self._validator:
                self._col = self._db.create_collection(self._name, codec_options=self._codec_options,
                                                       write_concern=self._write_concern, read_concern=self._read_concern,
                                                       read_preference=self._read_preference, validator=self._validator)
            else:
                self._col = self._db.create_collection(self._name, codec_options=self._codec_options, write_concern=self._write_concern,
                                                       read_concern=self._read_concern,  read_preference=self._read_preference)  # don't pass {} as validator
            self._collection_created()

        self._collection_initialised()

    def _collection_created(self):
        """descendants implementing this method should call parent's method first"""
        if self._rest_field[0] != "_id":
            self._col.create_index([(self._rest_field[0], 1)], unique=self._rest_field[1], background=True, name='idx_' + self._rest_field[0])

    def _collection_initialised(self):
        """descendants can implement this method"""
        return NotImplemented

    @classmethod
    def _filter_clear_empty(cls, q_filter):
        for key in [k for k, v in q_filter.items() if v == cls.__all]:
            del q_filter[key]

    def to_Html_table(self, query=[]):
        pass

    def put(self, doc):
        def fld_to_str(fld):  # @todo move it to mongo utils and expand to full document
            return str(fld) if isinstance(fld, (ObjectId, datetime)) else fld
        try:
            rt = self._col.insert_one(doc)
        except DuplicateKeyError as e:
            # @todo parse e,message "[E11000 duplicate key error collection: test_cva.Venues index: _id_ dup key: { : 3145984 }]'}"
            msg = e.message.encode('utf-8')  # coz contains value that may be not encoded
            return REST.status_get(409, 'resource exists attempted _id: [{}] message:{}'.format(doc[MC.id], msg))
        except WriteError as e:
            if e.message == self._validation_error_msg_:
                return REST.status_get(409, "{} attempted _id: [{}]".format(self._validation_error_msg_, format(doc[MC.id])))
            else:
                raise
        return 201, fld_to_str(rt.inserted_id)


    def get(self, q_filter, **kwargs): 
        if isinstance(q_filter, basestring):
            q_filter = SON([(self._rest_field[0], q_filter)])
        self._filter_clear_empty(q_filter)
        rt = self._col.find(q_filter, **kwargs) 
        return rt


    def tree(self, lst=None):
        if lst is None:
            lst = []
        lst.insert(0, self)
        if self._parent_model is not None:
            self._parent_model.tree(lst)
        return lst

    @classmethod
    def parent_id(cls, _id):
        """ all descendants should implement this class method"""
        raise NotImplementedError

    @classmethod
    def children_range(cls, id_parent):
        """ all descendants except first in chain should implement this class method"""
        raise NotImplementedError

    def get_by_rest_field(self, value, find_one=True, **kwargs):
        q = SON([(self._rest_field[0], value)])
        if find_one:
            return self._col.find_one(SON([(self._rest_field[0], value)]), **kwargs)
        else:
            return self._col.find(SON([(self._rest_field[0], value)]), **kwargs)

    def get_children_query(self, id_parent=None, extra_query=None):
        """returns children query"""
        if id_parent is None:
            q = SON()
        else:
            id_min, id_max = self.children_range(id_parent) 
            q = SON([(MC.id, SON([(MC.gte, id_min), (MC.lte, id_max)]))])
        if extra_query is not None:
            q.update(extra_query)
        return q

    def get_children(self, id_parent=None, extra_query=None, **kwargs):
        """returns children by parent id or None"""
        return self._col.find(self.get_children_query(id_parent, extra_query=extra_query), **kwargs)

    def get_siblings(self, _id, extra_query=None, **kwargs):
        """returns siblings including self"""
        return self.get_children(self.parent_id(_id), extra_query, **kwargs)

    @classmethod
    def parent(cls, _id, **kwargs): 
        return None if cls._parent is None else cls._parent_model._col.find_one({MC.id: cls.parent_id(_id)}, **kwargs)

    def _drop(self):
        self._db.drop_collection(self._name)