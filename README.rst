
==========
mongoUtils
==========

| A toolcase with utilities for `mongoDB <http://docs.mongodb.org/manual/>`__
  based mainly on `pymongo <http://api.mongodb.org/python/current/>`__
  useful also as mongoDB example usage and best practices.
| This library has been in developement as `pymongo_ext <https://github.com/nickmilon/pymongo_ext>`_ 
  until major changes in pymongo version 3 broke backward compatibility, it is now rewrittern from scratch.

____

.. Note::
  - `detailed documentation <http://miloncdn.appspot.com/docs/mongoUtils/index.html>`_
  - `github repository <https://github.com/nickmilon/mongoUtils>`_
  - for any bugs/suggestions feel free to issue a ticket in github's `issues <https://github.com/nickmilon/mongoUtils/issues>`_ 

____

:Installation: 
   ``pip install mongoUtils`` 

____

:Dependencies:
 - `pymongo <http://api.mongodb.org/python/current/>`__ (installed automatically by setup)
 - `Hellas <http://miloncdn.appspot.com/docs/Hellas/index.html>`_ (installed automatically by setup) a small python utilities library
 - `xlrd library <https://pypi.python.org/pypi/xlrd>`_  (used only for importing Excel workbooks into mongo >>> pip install xlrd)

____

:Usage:
   | See documentation of individual modules and classes
   | All examples require the existance of testing database and collections which are installed during execution of tests.
   | Also if mongoDB is not running on local host port 27017, testDbConStr connection string in configuration.py should be edited.  
   | Most examples require establishing a connection to testing database.
   
   >>> from pymongo import MongoClient;                        # import MongoClient
   >>> from mongoUtils.client import muClient                  # or alternativly this client
   >>> from mongoUtils.configuration import testDbConStr       # import conection string
   >>> client = muClient(testDbConStr)                         # establish a client connection
   >>> db = client.get_default_database()                      # get test database

____

:Tests:
   - to run tests
      ``python -m mongoUtils.tests.tests -v``

 
