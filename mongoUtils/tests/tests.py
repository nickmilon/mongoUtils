# -*- coding: utf-8 -*-
"""Library Tests

.. Note::
    - for safety test database is not dropped after tests are run it must be dropped manually
"""
import unittest
import gzip
import json

from mongoUtils.client import muClient
from mongoUtils.configuration import testDbConStr
from mongoUtils import _PATH_TO_DATA
from mongoUtils import importsExports
from mongoUtils import mapreduce
from mongoUtils.aggregation import AggrCounts, Aggregation

#     with gzip.open(_PATH_TO_DATA + "tweets_sample_10000.json.gz", 'rb') as fin:
#         cls.tweets_sample = simplejson.load(fin)
#     for i in range(0, len(cls.tweets_sample)):
#         cls.tweets_sample[i]['_aux'] = {'SeqGlobal': i}
#     cls.tweets_sample = [simplejson.dumps(doc) for doc in cls.tweets_sample]
#     cls.tweets_sample_len = len(cls.tweets_sample)


try:
    import xlrd
    xlrd_installed = True
except ImportError:
    xlrd_installed = False


def setUpStrEr(msg):
    return "{}" "check your testDbConStr in configuration.py".format(msg)


class Test(unittest.TestCase):

    def setUp(self):
        try:
            self.client = muClient(testDbConStr, connectTimeoutMS=5000, serverSelectionTimeoutMS=5000)
            self.server_info = self.client.server_info()
            self.db = self.client.get_default_database()
        except Exception as e:
            self.fail(setUpStrEr("test setup Error " + e.message))

    def tearDown(self):
        # self.db.drop_collections_startingwith(['muTest_'])
        pass

    def test_01_importsample(self):
        with gzip.open(_PATH_TO_DATA + "muTest_tweets.json .gz", 'rb') as fin:
            tweets_sample = json.load(fin)
        self.db.drop_collections_startingwith(['muTest_tweets'])
#         for  i in tweets_sample:
#             print i
#             self.db.muTest_tweets1000.insert(i)
        self.db.muTest_tweets.insert_many(tweets_sample)
        cnt = self.db.muTest_tweets.find().count()
        self.assertEqual(cnt, 1000, str(cnt) + " only records written to db instead of 1000")
        l = []
        for i in self.db.muTest_tweets.find():
            i['_id'] = i['user']['id_str']
            if not i['_id'] in l:
                l.append(i['_id'])
                self.db.muTest_tweets_users.insert_one(i['user'])

    @unittest.skipIf(xlrd_installed is False, 'pip install xlrd to test importing excel workbooks')
    def test_02_imports_xlsx(self):
        """tests imports from xlsx workbooks and sheets"""
        res = importsExports.ImportXls(_PATH_TO_DATA + "example_workbook.xlsx", "muTest_weather",
                                       self.db, stats_every=0)()
        self.assertEqual(res['rows'], 367, "can't import  example_workbook")
        res = importsExports.import_workbook(_PATH_TO_DATA + "example_workbook.xlsx", self.db, stats_every=0)
        self.assertGreater(res[0]['rows'], 300, "can't import  example_workbook")

    def test_aggregation(self):
        aggr_obj = Aggregation(self.db.muTest_tweets_users, allowDiskUse=True)
        aggr_obj.match({'lang': 'en'})
        aggr_obj.group({'_id': None, "avg_followers": {"$avg": "$followers_count"}})
        res = aggr_obj().next()
        self.assertAlmostEqual(res['avg_followers'], 2943.8, 1, "wrong aggregation average")
        res = AggrCounts(self.db.muTest_tweets_users, "lang",  sort={'count': -1})().next()
        self.assertEqual(res['count'], 352, "wrong aggregation count")

    def test_mapreduce(self):
        res = mapreduce.group_counts(self.db.muTest_tweets_users, 'lang', out={"replace": "muTest_mr"}, verbose=0)
        res00 = res[0].find(sort=[('value', -1)])[0]
        self.assertAlmostEqual(res00['value'], 352, 1, "wrong map reduce value in replace")
        res = mapreduce.group_counts(self.db.muTest_tweets_users, 'lang', out={"inline": 1}, verbose=0)
        res00 = sorted(res[0], key=lambda x: x['value'])[-1]
        self.assertAlmostEqual(res00['value'], 352, 1, "wrong map reduce value in inline")
        res = mapreduce.mr2('Orphans', self.db.muTest_tweets, 'user.screen_name',
                            col_b=self.db.muTest_tweets_users, col_b_key='screen_name',
                            col_b_query={'screen_name': {'$ne': 'Albert000G'}}, verbose=0)
        self.assertAlmostEqual(res[0].find({'value.b': 0}).count(), 1, "wrong map reduce count")
        res = mapreduce.mr2('Join', self.db.muTest_tweets, 'user.screen_name', col_b=self.db.muTest_tweets_users,
                            col_b_key='screen_name', col_b_query={'screen_name': {'$ne': 'Albert000G'}}, verbose=0)
        res = res[0].find_one({'value.b': None})['value']['a']['user']['screen_name']
        self.assertEqual(res, 'Albert000G', "wrong aggregation mr2 result")

if __name__ == "__main__":
    unittest.main()
