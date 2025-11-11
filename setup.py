#!/usr/bin/env python

from setuptools import setup

setup(
    name='pyspark-apps',
    version='1.0.1',
    packages=['pyspark_app','pyspark_app.app','pyspark_app.harvester','pyspark_app.operation','pyspark_app.utils','pyspark_app.datatransformer','pyspark_app.datafile'],
    description='Pyspark applications to generate data report on all kinds of big data',
    url='https://github.com/dbca-wa/pyspark-apps',
    author='Department of Biodiversity, Conservation and Attractions',
    author_email='asi@dbca.wa.gov.au',
    maintainer='Department of Biodiversity, Conservation and Attractions',
    maintainer_email='asi@dbca.wa.gov.au',
    license='Apache License, Version 2.0',
    zip_safe=False,
    keywords=['pyspark','datascience'],
    install_requires=[
        'numpy==2.3.0',
        'pandas==2.3.3',
        'h5py==3.15.1',
        'pytz==2025.2',
        'psutil==7.1.3',
        'azure-storage-blob==12.27.1',
        'maxminddb==3.0.0',
        'msal==1.34.0'
    ],
    classifiers=[
        'Framework :: Pyspark',
        'Environment :: Spark',
        'Intended Audience :: Developers',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
