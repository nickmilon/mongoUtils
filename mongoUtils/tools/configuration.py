'''
Created on Oct 24, 2015

@author: milon
'''
from Hellas.Sparta import DotDot


try:
    import yaml
except ImportError:
    print ("this module requires yaml library (>> pip install pyyaml)")



def mongo_configuration_dict(config_file_path = '/etc/mongod.conf'):
    """

    """
    with file('/etc/mongod.conf', 'r') as cf:
        config_dict=yaml.load(cf)
    return DotDot(config_dict)


def connection_uri(username=None, password=None, host="127.0.0.1", port="27017", database=''): 
    '''
    constructs a connection uri `see also : <https://docs.mongodb.org/manual/reference/connection-string/>`
    uri has no argument string optional arguments as replicaSet etc.
    optinal arguments may have to be provided on client creation
        - mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
    '''
    name_pwd = '' if username is None else "{}:{}@".format(username, password) 
    frmt = "mongodb://{}{}:{}/{}?"
    return frmt.format(name_pwd, host, port, database)


def connection_uri_from_conf_file(config_file_path=None, username=None, password=None, database):
    conf = mongo_configuration_dict(config_file_path)
    rt = connection_uri(username, password, conf.net.bindIp, str(conf.net.port), database)
    if conf.get('replication') is not None:
        replSetName = conf.replication.get('replSetName')
        rt += "replSetName=".format(conf.replication.replSetName)
    return rt 