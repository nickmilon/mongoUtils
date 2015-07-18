"""aggregation operations"""

from mongoUtils.helpers import pp_doc
from pymongo.command_cursor import CommandCursor


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
        >>> aggr_obj().next()                                                                  # execute and get results
        {u'avg_followers': 2943.8210227272725, u'_id': None})                                  # results
     """                                             # executes aggregation
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
        """returns the pipeline (a list)"""
        return self._pll

    @classmethod
    def help(cls, what='operators'):
        """returns list of available operators"""
        print (cls._operators)

    def add(self, operator, value, position=None):
        """adds an operation at specified position in pipeline"""
        if position is None:
            position = len(self._pll)
        self._pll.insert(position, {operator: value})

    def search(self, operator, count=1):
        """returns (position, operator"""
        cnt = 0
        for i, item in enumerate(self.pipeline):
            if item.keys()[0] == operator:
                cnt += 1
                if cnt == count:
                    return (i, item)

    def save(self, file_pathname):
        """save pipeline list to file"""
        with open(file_pathname, 'w') as f:
            return f.write(self.code(verbose=False))

    def remove(self, position):
        """remove an element from pipeline list given its position"""
        return self._ppl.remove(position)

    def code(self, verbose=True):
        return pp_doc(self.pipeline, 4, sort_keys=False, verbose=verbose)

    def __call__(self, verbose=False, **kwargs):
        """perform the aggregation when called
        >>> Aggregation_object()

        `for kwargs see: <http://api.mongodb.org/python/current/api/pymongo/collection.html?highlight=aggregate/>`_

        :param bool verbose: if True will print results but return None
        :param dict kwargs: if any of kwargs are specified override any arguments provided on instance initialization.
        """
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
