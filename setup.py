# -*- coding: utf-8 -*-
from setuptools import setup

__author__ = 'Martin Uhrin'
__license__ = 'LGPLv3'

about = {}
with open('mincepy/version.py') as f:
    exec(f.read(), about)

setup(
    name='mincepy',
    version=about['__version__'],
    description='Python object storage with versioning made simple',
    long_description=open('README.rst').read(),
    url='https://github.com/muhrin/mincepy.git',
    author='Martin Uhrin',
    author_email='martin.uhrin.10@ucl.ac.uk',
    license=__license__,
    classifiers=[
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='database schemaless nosql orm object-store concurrent optimistic-locking',
    install_requires=[
        'contextlib2; python_version<"3.7"',
        'deprecation',
        'dnspython',  # Needed to be able to connect using domain name rather than IP
        'pymongo<4.0',
        'mongomock',
        'bidict',
        'networkx',  # For reference graphs
        'pyblake2; python_version<"3.6"',
        'pytray>=0.2.1',
        'stevedore',
        'click',
        'tabulate',
    ],
    python_requires='>=3.7',
    extras_require={
        'cli': ['click', 'tabulate'],
        'gui': ['mincepy-gui'],
        'dev': [
            'ipython',
            'mongomock',
            'pip',
            'pytest>4',
            'pytest-benchmark',
            'pytest-cov',
            'pre-commit',
            'prospector',
            'pylint',
            'twine',
            'yapf',
        ],
        'docs': [
            'nbsphinx',
            'sphinx',
            'sphinx-autobuild',
        ],
        'sci': ['mincepy-sci'],
    },
    packages=['mincepy', 'mincepy.cli', 'mincepy.mongo', 'mincepy.hist'],
    include_package_data=True,
    test_suite='test',
    provides=['mincepy.plugins'],
    entry_points={
        'console_scripts': ['mince = mincepy.cli.main:mince'],
        'mincepy.plugins.types': ['native_types = mincepy.provides:get_types'],
    })
