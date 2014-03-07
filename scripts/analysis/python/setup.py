import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup (
    name = "procmon",
    version = 1.0,
    author = "Douglas Jacobsen",
    author_email = "dmjacobsen@lbl.gov",
    description = ("Procmon analysis toolkit for python", "procmon analysis"),
    license = "Closed",
    keywords = "procmon data analysis",
    url = None,
    long_description = read('README'),
    packages=['procmon','procmon/tests'],
    test_suite="procmon.tests",
    requires=['nose',],
)
