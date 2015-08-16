"""configuration module

:testDbConStr:
    `(see connection string)  <http://docs.mongodb.org/manual/reference/connection-string/>`_
    used to connect to a mongoDB server for tests and examples
    database name part of connection string is the database on which to contact tests

    .. Warning::
        - for safety database is not dropped after tests are run it must be dropped manually
        - it is possible for an individual collection in db to be dropped/overwritten if its name starts with muTest\_
"""


testDbConStr = "mongodb://localhost:27017/mongoUtilsTests"

#  ######################################### don't write after this line #########################
# from mongoUtils.client import muClient

TEST_CLIENT = None


def get_dbtest():
    """returns test db"""
    global TEST_CLIENT
    if TEST_CLIENT is None:
        from mongoUtils.client import muClient as TEST_CLIENT
    return TEST_CLIENT(testDbConStr).get_default_database()
