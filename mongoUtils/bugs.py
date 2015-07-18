"""Created on May 20, 2015 use this module to demonstrate bugs (if any) related to this package, pymongo or MongoDB"""

from pymongo import MongoClient


def bug_example():
    client = MongoClient()
    col_1 = client.temp.col_1
    col_2 = client.temp.col_2
    col_1X2 = client.temp.col_1_2
    col_1.database.drop_collection(col_1.name)
    col_2.database.drop_collection(col_2.name)
    col_1X2.database.drop_collection(col_1X2.name)
    for i in range(1, 11):
        col_1.insert({'_id': i})
        col_2.insert({'_id': i})
    for c1 in col_1.find(sort=[('_id', -1)]):
        for c2 in col_2.find(sort=[('_id', -1)]):
            id = {'c1': c1['_id'], 'c2': c2['_id']}
            print (id)
            col_1X2.insert({'_id': id})
    find_1_cnt = col_1.count()
    find_2_cnt = col_2.count()
    find_1X2_cnt = col_1X2.count()
    assert find_1X2_cnt == find_1_cnt * find_2_cnt
    return col_1, col_2, col_1X2
