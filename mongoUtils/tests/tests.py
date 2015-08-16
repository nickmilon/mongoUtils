# -*- coding: utf-8 -*-
"""Library Tests

.. Note::
    - for safety test database is not dropped after tests are run it must be dropped manually
"""
import unittest
import gzip
import json
import codecs

from mongoUtils.client import muClient
from mongoUtils.configuration import testDbConStr
from mongoUtils import _PATH_TO_DATA
from mongoUtils import importsExports, mapreduce, schema, helpers
from mongoUtils.aggregation import AggrCounts, Aggregation
from mongoUtils.tests.PubSubBench import ps_tests

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
            reader = codecs.getreader("utf-8")
            # read through reader for python3 see
            # http://stackoverflow.com/questions/6862770/python-3-let-json-object-accept-bytes-or-let-urlopen-output-strings
            tweets_sample = json.load(reader(fin))
        self.db.drop_collections_startingwith(['muTest_', 'del_'])
#         for  i in tweets_sample:
#             print i
#             self.db.muTest_tweets1000.insert(i)
        self.db.muTest_tweets.insert_many(tweets_sample)
        self.db.muTest_tweets.create_index([('user.screen_name', 1)], background=True) 
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
        res = next(aggr_obj())
        self.assertAlmostEqual(res['avg_followers'], 2943.8, 1, "wrong aggregation average")
        res = next(AggrCounts(self.db.muTest_tweets_users, "lang",  sort={'count': -1})())
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

    def test_schema(self):
        r = schema.schema(self.db.muTest_tweets_users, meta=True, verbose=0)
        fields = r[0][0].find()[0]['value']['fields']
        self.assertTrue('_id' in fields, "_id not found in schema")

    def test_helpers_coll_range(self):
        """get min max of a collection field"""
        res = helpers.coll_range(self.db.muTest_tweets_users, 'id_str')
        self.assertEqual(res[1], '999314042', "wrong coll_range results")

    def test_helpers_coll_copy(self):
        res = helpers.coll_copy(self.db.muTest_tweets, self.db['muTest_tweets_copy'],
                                create_indexes=True, dropTarget=True, write_options={}, verbose=0)
        self.assertEqual(res.count(), self.db.muTest_tweets.count(), "error in coll_copy")

    def test_helpers_coll_chunks(self):
        """guarantees that all documents are fetched and no overlaps occur
        """
        doc_count = self.db.muTest_tweets.count()
        res = helpers.coll_chunks(self.db.muTest_tweets, '_id', 0.3)
        out_lst = []
        for i in res:
            out_lst.extend([i['_id'] for i in self.db.muTest_tweets.find(i[1], projection={})])
        out_lst = set(out_lst)
        self.assertEqual(len(out_lst), doc_count, "wrong coll_chunks ranges or overlaps occurred")

    def test_pubsub(self):
        res = ps_tests('speed', testDbConStr)
        self.assertGreater(res.msgsPerSecPub, 1000, "message publishing too slow")
        self.assertGreater(res.msgsPerSecSub, 1000, "message reading too slow")
        res = ps_tests('speedThread', testDbConStr)
        self.assertGreater(res.msgsPerSec, 500, "message absorption too slow")
        self.assertEqual(res.msgs, 2000, "messages skipped")

if __name__ == "__main__":
    unittest.main()
