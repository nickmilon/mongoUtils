"""aggregation operations"""

from mongoUtils.helpers import pp_doc
from pymongo.command_cursor import CommandCursor
from bson.son import SON


class Aggregation(object):
    """**a helper for constructing aggregation pipelines** see:
    `aggregation framework  <http://docs.mongodb.org/manual/reference/aggregation/>`_ supports all
    `aggregation operators  <http://docs.mongodb.org/manual/reference/operator/aggregation/>`_

    :param obj collection: a pymongo collection object
    :param list pipeline: (optional) an initial pipeline list
    :param dict kwargs: (optional) `any arguments  <http://docs.mongodb.org/manual/reference/operator/aggregation/>`_

    :returns: an aggregation object

    :Example:
        >>> from pymongo import MongoClient;from mongoUtils.configuration import testDbConStr  # import MongoClient
        >>> db = MongoClient(testDbConStr).get_default_database()                              # get test database
        >>> aggr_obj = Aggregation(db.muTest_tweets_users, allowDiskUse=True)                  # select users collection
        >>> aggr_obj.help()                                                                    # ask for help
        ['project', 'match', 'redact', 'limit', .... ]                                         # available operators
        >>> aggr_obj.match({'lang': 'en'})                                                     # match English speaking
        >>> aggr_obj.group({'_id': None, "avg_followers": {"$avg": "$followers_count"}})       # get average followers
        >>> print(aggr_obj.code(False))                                                        # print pipeline
        [{"$match": {"lang": "en"}},{"$group": {"avg_followers":
        {"$avg": "$followers_count"},"_id": null}}]
        >>> next(aggr_obj())                                                                   # execute and get results
        {u'avg_followers': 2943.8210227272725, u'_id': None})                                  # results
     """                                             # executes aggregation
    _operators = 'project match redact limit skip sort unwind group out geoNear'.split(' ')
    _frmt_str = "{}\nstage#= {:2d}, operation={}"

    def __init__(self, collection, pipeline=None, **kwargs):
        def _makefun(name):
            setattr(self, name, lambda value, position=None: self.add('$' + name, value, position))
        self._collection = collection
        self._kwargs = kwargs
        self._pll = pipeline or []      # pipeline list
        for item in self._operators:    # auto build functions for operators
            _makefun(item)

    @classmethod
    def construct_fields(cls, fields_list=[]):
        """a constructor for fields
        """
        return SON([(i.replace('.', '_'), '$'+i) for i in fields_list])

    @classmethod
    def construct_stats(cls, fields_lst, _id=None, stats=['avg', 'max', 'min'], incl_count=True):
        """a constructor helper for group statistics

        :Parameters:
            - fields_lst: (list) list of field names
            - stats: (list) list of statistics
            - incl_count: (Bool) includes a count if True
        :Example:
            >>> specs_stats(['foo'])
            {'max_foo': {'$max': '$foo'}, '_id': None, 'avg_foo': {'$avg': '$foo'}, 'min_foo': {'$min': '$foo'}}
        """
        frmt_field_stats = "{}_{}"
        res = {}
        for field in fields_lst:
            res.update({frmt_field_stats.format(i, field): {'$'+i: '$'+field} for i in stats})
        if incl_count:
            res.update({'count': {'$sum': 1}})
        res.update({'_id': _id})
        return res

    @property
    def pipeline(self):
        """returns the pipeline (a list)"""
        return self._pll

    @classmethod
    def help(cls, what='operators'):
        """returns list of available operators"""
        print(cls._operators)

    def add(self, operator, value, position=None):
        """adds an operation at specified position in pipeline"""
        if position is None:
            position = len(self._pll)
        self._pll.insert(position, {operator: value})

    def search(self, operator, count=1):
        """returns (position, operator"""
        cnt = 0
        for i, item in enumerate(self.pipeline):
            if list(item.keys())[0] == operator:
                cnt += 1
                if cnt == count:
                    return (i, item)

    def save(self, file_pathname):
        """save pipeline list to file"""
        with open(file_pathname, 'w') as f:
            return f.write(self.code(verbose=False))

    def remove(self, position):
        """remove an element from pipeline list given its position"""
        del self._pll[position]

    def code(self, verbose=True):
        return pp_doc(self.pipeline, 4, sort_keys=False, verbose=verbose)

    def clear(self):
        self._pll = []

    def __call__(self, print_n=None, **kwargs):
        """perform the aggregation when called
        >>> Aggregation_object()

        for kwargs see: `aggregate <http://api.mongodb.org/python/current/api/pymongo/collection.html>`_

        :Parameters:
            - print_n:
                - True: will print results and will return None
                - None: will cancel result printing
                - int: will print top n documents
            -  kwargs: if any of kwargs are specified override any arguments provided on instance initialization.
        """
        tmp_kw = self._kwargs.copy()
        tmp_kw.update(kwargs)
        rt = self._collection.aggregate(self.pipeline, **tmp_kw)
        if print_n is not None:
            print (self._frmt_str.format("--" * 40, len(self.pipeline), str(self.pipeline[-1])))
            if isinstance(rt, CommandCursor):
                for cnt, doc in enumerate(rt):
                    print (doc)
                    if print_n is not True and cnt+2 > print_n:
                        break
                return None
            else:
                print (rt)
        return rt


class AggrCounts(Aggregation):
    """
    constructs a group count aggregation pipeline based on :class:`~Aggregation` class

    :param obj collection: a pymongo collection object
    :param str field: field name
    :param dict match: a query match expression, defaults to None
    :param dict sort: a sort expression defaults to {'count': -1}
    :param dict kwargs: optional arguments to pass to parent :class:`~Aggregation`

    :Example:
        >>> from pymongo import MongoClient;from mongoUtils.configuration import testDbConStr  # import MongoClient
        >>> db = MongoClient(testDbConStr).get_default_database()                              # get test database
        >>> AggrCounts(db.muTest_tweets_users, "lang",  sort={'count': -1})(verbose=True)      # counts by language
        {u'count': 352, u'_id': u'en'}
        {u'count': 283, u'_id': u'ja'}
        {u'count': 100, u'_id': u'es'}  ...
    """
    def __init__(self, collection, field,  match=None, sort={'count': -1}, **kwargs):
        super(AggrCounts, self).__init__(collection, **kwargs)
        if match is not None:
            self.match(match)
        self.group({'_id': '$'+field, 'count': {'$sum': 1}})
        if sort is not None:
            self.sort(sort)
