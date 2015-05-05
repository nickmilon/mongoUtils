'''aggregation operations'''

from mongoUtils.helpers import pp_doc
from pymongo.command_cursor import CommandCursor


class Aggregation(object):
    '''**a helper for constructing aggregation pipelines** see:
    `aggregation framework  <http://docs.mongodb.org/manual/reference/aggregation/>`__ supports all
    `aggregation operators  <http://docs.mongodb.org/manual/reference/operator/aggregation/>`__

    :Args:

    - collection: (obj) a pymongo collection object
    - pipeline: (optional list) an initial pipeline list
    - kwargs: `any optional arguments  <http://docs.mongodb.org/manual/reference/operator/aggregation/>`__
        - allowDiskUse=True
    :Usage:
     >>> ppl = Aggregation(my_collection, allowDiskUse=True)
     >>> ppl.group({'_id': '$field_name, 'count': {'$sum': 1}})
     >>> ppl()
    '''
    _operators = 'project match redact limit skip sort unwind group out geoNear'.split(' ')
 
    def __init__(self, collection, pipeline=None, **kwargs):
        def _makefun(name):
            setattr(self, name, lambda value, position=None: self.add('$' + name, value, position))
        self._collection = collection
        self._kwargs = kwargs
        self._pll = pipeline or []      # pipeline list
        for item in self._operators:    # auto build functions for operators
            _makefun(item)

    @property
    def pipeline(self):
        ''' returns the pipeline (a list)
        '''
        return self._pll

    @classmethod
    def help(cls, what='operators'):
        '''returns list of available operators'''
        print (cls._operators)

    def add(self, operator, value, position=None):
        if position is None:
            position = len(self._pll)
        self._pll.insert(position, {operator: value})

    def search(self, operator, count=1):
        ''' returns (position, operator
        '''
        cnt = 0
        for i, item in enumerate(self.pipeline):
            if item.keys()[0] == operator:
                cnt += 1
                if cnt == count:
                    return (i, item)

    def save(self, file_pathname):
        '''save pipeline list to file'''
        with open(file_pathname, 'w') as f:
            return f.write(self.code(verbose=False))

    def remove(self, position):
        '''remove an element from pipeline list given its position'''
        return self._ppl.remove(position)

    def code(self, verbose=True):
        return pp_doc(self.pipeline, 4, sort_keys=False, verbose=verbose)

    def __call__(self, verbose=False, **kwargs):
        '''perform the aggregation when called
        >>> Aggregation_object()

        `for kwargs see: <http://api.mongodb.org/python/current/api/pymongo/collection.html?highlight=aggregate/>`__
        :Args:
          - `verbose`: (bool) if True will print results but return None because there is no rewind() for CommandCursor
          - `kwargs` (optional): if any of kwargs are specified override any kwargs provided on instance initialization.
        '''
        tmp_kw = self._kwargs.copy()
        tmp_kw.update(kwargs)
        rt = self._collection.aggregate(self.pipeline, **tmp_kw)
        if verbose:
            if isinstance(rt, CommandCursor):
                for doc in rt:
                    print (doc)
                return None
            else:
                print (rt)
        return rt


class AggrCounts(Aggregation):
    '''constructs a group count aggregation pipeline based on :class:`~Aggregation` class
    :Args:
    - collection:` a pymongo collection object
    - field: (str) field name
    - match: a query match expression
    - sort: a sort expression defaults to {'count': -1}
    '''
    def __init__(self, collection, field,  match=None, sort={'count': -1}, **kwargs):
        super(AggrCounts, self).__init__(collection, **kwargs)
        if match is not None:
            self.match(match)
        self.group({'_id': '$'+field, 'count': {'$sum': 1}})
        if sort is not None:
            self.sort(sort)
