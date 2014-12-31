'''
Created on Dec 13, 2014

@author: nickmilon
'''


def indexed_fields(a_collection):
    return [i['key'][0][0] for i in a_collection.index_information().values()]


def parse_js_fun(file_path, function_name, replace_vars=None):
    """ helper function to get a js function string from a file containing js functions.
        usefull if we want to call js functions from python as in mongoDB map reduce
        Function must be named starting in first column and end with '}' in first column
        Args:
            file_path (str): full path_name
            function name (str): name of function
            replace_vars (optional) a tuple to replace %s variables in functions
        Returns: a js function as string
    """
    rt = ''
    start = 'function {}'.format(function_name)
    with open(file_path, 'r') as fin:
        ln = fin.readline()
        while ln:
            if ln.startswith(start) or rt:
                rt += ln
            if rt and ln.startswith('}'):
                break
            ln = fin.readline()
    if rt and replace_vars:
        return rt % replace_vars
    else:
        return rt
