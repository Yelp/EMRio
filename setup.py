import os
from setuptools import setup
setuptools_kwargs = {
        'install_requires': [
            'boto>=2.2.0',
            'PyYAML',
            'simplejson>=2.0.9',
        ],
        'provides': ['emrio'],
        'tests_require': ['unittest2'],
    }


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="emrio",
    version="0.0.1",
    author="Sean Myers",
    author_email="SeanMyers0608@gmail.com",
    description=("EMR instance optimizer will take your past EMR history and"
                    "attempt to optimize the max reserved instances for it"),
    license="Apache?",
    keywords="EMRio EMR Instance Optimizer Reserved Instances",
    url="http://github.com/Yelp/EMRio",
    packages=['emrio_lib', 'tests'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
    ],
    entry_points={
        'console_scripts': [
            'emrio = emrio_lib.EMRio:main'
        ]
    }
)
