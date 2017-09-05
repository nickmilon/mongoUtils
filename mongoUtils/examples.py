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


from random import randint, choice, random
from datetime import datetime, timedelta
from bson import SON, ObjectId
from Hellas.Delphi import DotDot
from mongoUtils.aggregation import Aggregation, AggrLookUp
from mongoUtils.client import muClient                  # or alternatively this client
from mongoUtils.configuration import testDbConStr       # import connection string
from mongoUtils.helpers import muBulkOps
from calendar import weekday
import string
from Hellas.Sparta import DotDot

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
        for i in range(1, n_docs+1):
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
    def ex002_insertdocs(cls, n_docs):
        """inserts test docs to collection"""
        sites = ('A', 'B', 'C', 'D', 'E')
        def random_doc():
            site = sites[randint(0, len(sites) - 1)]
            weekday = randint(1, 7)
            value = randint(-1, 1)
            {"_id": id, "site": "site A", "weekday": 1, "value": 1}
            doc = DotDot({'site': site, 'weekday': weekday, 'value': value})
            print (doc)
            return doc

        db.drop_collection('ex_002')
        for i in range(1, n_docs+1):
            db.ex_002.insert_one(random_doc())
        return db.ex_002

    @classmethod
    def ex002_aggregate(cls, match={'site': 'A'}, print_n=2):
        """performs aggregation and prints results of each stage in pipeline"""
        ag = Aggregation(db.ex_002)
        print ("original", db.ex_001.find_one())
        if match is not None:
            ag.match(match)
        ag.group({'_id': {'site': '$site', 'weekday': '$weekday', 'value': '$value'}, 'count': {'$sum': 1}})
        ag(print_n)
        ag.group({'_id': {'site': '$_id.site', 'weekday': '$_id.weekday'},
                  'items': {'$push': {'value': "$_id.value", 'count': "$count"}}})
        ag(print_n)
        return ag

    @classmethod
    def ex003_insertdocs(cls, n_docs):
        """inserts test docs to collection"""

        d1 = datetime.strptime('5/1/2015', '%m/%d/%Y')  # starting date
        d2 = datetime.strptime('8/1/2015', '%m/%d/%Y')  # ending date

        def random_doc(): 
            doc = DotDot({'temperature': randint(10, 100), 'timestamp': cls.random_date(d1, d2)})
            return doc

        db.drop_collection('ex_003')
        for i in range(1, n_docs+1):
            db.ex_003.insert_one(random_doc())
        return db.ex_003

    @classmethod
    def ex003_aggregate(cls, print_n=2):
        """performs aggregation and prints results of each stage in pipeline"""
        ag = Aggregation(db.ex_003)
        ag.sort({'temperature': 1})
        ag(print_n)
        ag.group({'_id': {"_id": {"day": {"$dayOfYear": "$timestamp"}}},
                  "max_temperature": {"$max": "$temperature"}, "timestamp": {"$last": "$timestamp"}})
        ag(print_n)

        return ag

    @classmethod
    def ex004_insertdocs(cls):
        """ lookup example
        """
        db.drop_collection('ex_004_material')
        db.drop_collection('ex_004_user')
        materials = {'id': "m123", 'color': "red",
                     'owner': [{'ownerid': "1111", 'other': "foo"}, {'ownerid': "2222", 'other': "bar"}]
                     }
        users = [{'id': "1111", 'name': "john", 'age': "30"}, {'id': "2222", 'name': "Jane", 'age': "23"}]
        db.ex_004_material.insert_one(materials)
        db.ex_004_user.insert_many(users)

    @classmethod
    def ex004_aggregate(cls, print_n=2):
        """performs aggregation and prints results of stages in pipeline"""
        ag = AggrLookUp(db.ex_004_material, 'ex_004_user', localField='owner.ownerid', foreingField='id', AS='owner_info', orphans=None)
        ag.add('$unwind', '$owner', 0)              # add unwind on top (we need it for #lookup
        ag(print_n)
        ag.add('$unwind', '$owner_info')            # unwind so we cat take element instead of list
        ag(print_n)
        ag.add('$group',  {'_id': "$id",            # group by id (supposed unique)
                           'id': {'$first': "$id"},
                           'color': {'$first': "$color"},
                           'owner': {'$push': {'ownerid': '$owner.ownerid', 'other': '$owner.other', 'owner_info': '$owner_info'}
                                     }
                           })
        ag(print_n)
        ag.add('$project',  {'_id': 0, 'owner.owner_info._id': 0})  # get rid of _id (s) 
        ag(print_n)
        return ag

    @classmethod
    def ex005_aggregate_text_search_stage(cls, collection_object, text_to_search):
        """ a text search example

        :note:: text search is only allowed in first stage of aggregation pipeline
        """
        ag = Aggregation(collection_object)
        ag.match({'$text': {'$search': text_to_search}})
        ag.project({'document': '$$ROOT', 'score': {'$meta': 'textScore'}})
        return ag

    @classmethod
    def ex005_aggregate_facet1_stage(cls, collection_object, text_to_search):
        """ a text search example

        :note:: text search is only allowed in first stage of aggregation pipeline
        """
        agr1 = cls.ex005_aggregate_text_search_stage(collection_object, text_to_search)
        ag = Aggregation(collection_object)
        ag.facet({'agr1': agr1.pipeline})
        return ag

    @classmethod
    def ex006_insertdocs(cls):
        db.drop_collection('ex_006')
        documents = []
        for i in range(100):
            letters = choice(string.ascii_letters[0:15])
            documents.append({'hgvs_id': "".join([str(randint(0, 9)), letters]),
                              'sample_id': "CDE",
                             'number': i * random()*50 - 30})
            documents.append({'hgvs_id': "".join([str(randint(0, 9)), letters]),
                              'sample_id': 'ABC',
                              'number': i * random() * 50 - 30})
            documents.append({'hgvs_id': "".join([str(randint(0, 9)), letters]),
                              'sample_id': 'GEF',
                              'number': i * random()*50 - 30})

        for i in range(10):    # add some unique values for sample_id 'ABC'
            letters = choice(string.ascii_letters[0:15])
            documents.append({'hgvs_id': "55" + letters,
                              'sample_id': 'ABC',
                              'number': i * random() * 50 - 30})
        db.ex_006.insert_many(documents)
        return db.ex_006

    """ ex007 algo to find if a value fits within a range of values 
    """
    @classmethod
    def ex007_insertdocs(cls, max_docs=1000000, max_range=100):
        db.drop_collection('ex_007')
        col = db['ex_007']
        bulk = muBulkOps(col, ordered=False, ae_n=10, dwc={'w': 1})
        doc = {'_id': 1, 'to': randint(1, max_range), 'cnt': 0}
        dt_start = datetime.now()
        while doc['cnt'] < max_docs:
            doc['cnt'] += 1
            doc['_id'] = doc['to'] + randint(1, max_range)
            doc['to'] = doc['_id'] + randint(1, max_range)
            bulk.insert(doc.copy())  # copy coz we modify it while in pending execute
        bulk.execute_if_pending()
        doc['docs_per_second'] = doc['cnt'] / (datetime.now() - dt_start).total_seconds()
        print(doc)
        return col

    @classmethod
    def ex007_find_range(cls, val=None):
        col = db['ex_007']
        if val is None:
            val = col.find_one(sort=[('_id', -1)])['_id']  # just get last one
        dt_start = datetime.now()
        range = list(col.find({"_id": {'$lte': val}}, sort=[('_id', -1)], limit=1)) 
        print('range', range)
        if range and val <= range[0]['to']:
            rt = range[0]
        else:
            rt = None 
        millis = (datetime.now() - dt_start).total_seconds() * 1000
        return (millis, val, rt)

    @classmethod
    def ex010_time_series(cls, dt_start=None, dt_end=None, step_secs=60*60*24, start=355125):
        """
        """
        dt_start = datetime(2017, 4, 1, 23, 59, 59) if dt_start is None else dt_start
        dt_end = datetime(2017, 7, 16, 23, 59, 59) if dt_start is None else dt_start
        col = db['ex_010']
        db.drop_collection('ex_010')
        cnt = 1
        dt = dt_start
        while dt <= dt_end:
            col.insert_one()
            doc = {'_id': cnt, "uid": ObjectId("596272cc02e50216ec0061cc"), 'date': dt.timestamp()}
            tr = randint(start, start + (cnt * 500))
            if dt.weekday() == 5 or dt.weekday() == 6:
                tr = int(tr * 0.7)
            factor = randint(4, 7) / 10.0
            tbb = int(tr * factor)
            doc.update({'tr': dt.timestamp(), 'tr': tr, 'tbb': tbb, 'month': dt.month})
            cnt += 1
            dt += timedelta(seconds=step_secs)

    @classmethod
    def ex001(cls, print_n=10):
        cls.ex001_insertdocs(10000)
        return cls.ex001_aggregate(print_n)

ex_cl = Examples()                                                    