"""**some examples**

:example_001:
     - `see question in stack overflow <http://stackoverflow.com/questions/31526793/aggr
       egate-group-by-query-in-mongodb-showing-top-5-count-results-by-date>`_

    :Usage:
        >>> example = Examples(); example.ex001()
        >>> res = example.ex001(1)
        stage#=  1...
        stage#=  2...
"""


from random import randint
from datetime import datetime, timedelta
from bson import SON
from Hellas.Delphi import DotDot

from mongoUtils.aggregation import Aggregation
from mongoUtils.client import muClient                  # or alternatively this client
from mongoUtils.configuration import testDbConStr       # import connection string

client = muClient(testDbConStr)                         # establish a client connection
db = client.get_default_database()


class Examples(object):
    def __init__(self):
        pass

    @classmethod
    def random_date(cls, start, end):
        return start + timedelta(seconds=randint(0, int((end - start).total_seconds())))

    @classmethod
    def ex001_insertdocs(cls, n_docs):
        """inserts test docs to collection"""
        categories = ('food', 'news', 'tv', 'gossip', 'football', 'tennis', 'cinema', 'recipes')
        d1 = datetime.strptime('5/1/2015', '%m/%d/%Y')  # starting date
        d2 = datetime.strptime('6/1/2015', '%m/%d/%Y')  # ending date

        def random_doc():
            cat_lst = []
            for i in range(1, randint(2, 5)):
                cat_lst.append(categories[randint(0, len(categories) - 1)])
            cat_lst = list(set(cat_lst))  # remove duplicates
            cat_lst = [{'title': i} for i in cat_lst]
            doc = DotDot({'article': {'category': cat_lst}, 'published': cls.random_date(d1, d2)})
            return doc

        db.drop_collection('ex_001')
        for i in range(1, n_docs):
            db.ex_001.insert_one(random_doc())
        return db.ex001

    @classmethod
    def ex001_aggregate(cls, print_n=2):
        """performs aggregation and prints results of each stage in pipeline"""
        ag = Aggregation(db.ex_001)
        print ("original", db.ex_001.find_one())
        ag.project({'yymmdd': {'$dateToString': {'date': '$published', 'format': '%Y-%m-%d'}}, 'article': '$article'})
        ag(print_n)
        ag.unwind('$article.category')
        ag(print_n)
        ag.group({'_id': {'yymmdd': '$yymmdd', 'title': '$article.category.title'}, 'count': {'$sum': 1}})
        ag(print_n)
        ag.sort(SON([('_id.yymmdd', 1), ('count', - 1)]))
        ag(print_n)
        ag.group({'_id': '$_id.yymmdd', 'item': {'$push': {'item': '$_id.title', 'count': '$count'}}})
        ag(print_n)
        return ag

    @classmethod
    def ex001(cls, print_n=10):
        cls.ex001_insertdocs(10000)
        return cls.ex001_aggregate(print_n)

ex_cl = Examples()  # ; ex_cl.ex001()
