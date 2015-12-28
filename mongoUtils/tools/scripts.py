'''
some usefull scripts functions etc
Created on Dec 12, 2015

@author: milon
'''

from Hellas.Delphi import time_func


@time_func
def query_bm(col, query, operations):
    for i in range(0, operations):
        r = col.find(query).count()

@time_func
def query_bm_one(col, query, operations):
    for i in range(0, operations):
        r = col.find_one(query)