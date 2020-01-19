# -*- coding: utf-8 -*-
from setuptools import setup

__author__ = "Martin Uhrin"
__license__ = "GPLv3 and MIT, see LICENSE file"

about = {}
with open('mincepy/version.py') as f:
    exec(f.read(), about)

setup(name='mincepy',
      version=about['__version__'],
      description="Object storage with versioning made simple",
      long_description=open('README.rst').read(),
      url='https://github.com/muhrin/mincepy.git',
      author='Martin Uhrin',
      author_email='martin.uhrin.10@ucl.ac.uk',
      license=__license__,
      classifiers=[
          'License :: OSI Approved :: MIT License',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
      ],
      keywords='workflow',
      install_requires=['pymongo', 'bidict', 'pyblake2; python_version<"3.6"'],
      extras_require={
          'cli': ['click', 'tabulate'],
          'dev': [
              'pip',
              'pytest>4',
              'pytest-cov',
              'pre-commit',
              'yapf',
              'prospector',
              'pylint',
              'twine',
          ],
      },
      packages=['mincepy'],
      test_suite='test')
