'''
Created on Mar 14, 2013

@author: nickmilon
db operations
'''

from mongoUtils.utils import parse_js_default


def db_js_add_fun(db, fun_name, fun_str):
    ''' to use the actuall function from mongo shell you have to execute db.loadServerScripts(); first
    '''
    r = db.system_js[fun_name] = fun_str
    return r


def db_js_add_fun_default(db, file_name, fun_name):
    fun_str = parse_js_default(file_name, fun_name)
    db_js_add_fun(db, fun_name, fun_str)
    return fun_str


def db_js_function_names(db):
    return db.system.js.find()
