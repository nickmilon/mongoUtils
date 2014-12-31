'''
Created on Feb 24, 2013
@author: nickmilon
see: https://docs.python.org/2/distutils/setupscript.html
'''
from setuptools import setup, find_packages
version = '0.1.1'
print 'packages', find_packages()
setup(
    packages=find_packages(),
    # package_data={'mongoUtils': ['js/*.js']},
    package_data={'mongoUtils': ['../js/*.*']},
    name="mongoUtils",
    version=version,
    author="nickmilon",
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
    keywords=["mongo", "mongodb", "pymongo", "mongo utilities"],
    # requirements and specs
    zip_safe=False,
    tests_require=["nose"],
    install_requires=[
        'pymongo'
    ],
)
