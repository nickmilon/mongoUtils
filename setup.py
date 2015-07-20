"""
Created on Feb 24, 2013
@author: nickmilon
see: https://docs.python.org/2/distutils/setupscript.html
"""
#######################
import re
import os
from setuptools import setup, find_packages
cur_path = os.path.dirname(os.path.realpath(__file__))
read_init = open(cur_path+'/mongoUtils/__init__.py').read()
__version__ = re.search("__version__\s*=\s*'(.*)'", read_init, re.M).group(1)
__author__ = re.search("__author__\s*=\s*'(.*)'", read_init, re.M).group(1)
print('installing packages', find_packages()) 
######################
setup(
    packages=find_packages(),
    package_data={'mongoUtils': ['../js*.*', '../data/*.*']},
    name="mongoUtils",
    version=__version__,
    author=__author__,
    author_email="nickmilon/gmail/com",
    maintainer="nickmilon",
    maintainer_email="nickmilon/gmail/com",
    url="https://github.com/nickmilon/mongoUtils",
    description="Utilities for mongoDB mainly based on pymongo python mongoDB driver, "
                "see:http://github.com/mongodb/mongo-python-driver",
    long_description="see: readme",
    download_url="https://github.com/nickmilon/mongoUtils.git",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Topic :: Database"],
    license="MIT or Apache License, Version 2.0",
    keywords=["mongo", "mongodb", "pymongo", "mongo utilities", 'database', 'nosql', 'big data'],
    # requirements and specs
    zip_safe=False,
    tests_require=["nose"],
    install_requires=[
        'pymongo',
        'Hellas'
    ],
)
