import os
from nose.tools import with_setup
from MusicalGraph.MusicalGraph import MusicalGraph


def setup_func():
    gpath = "/data/test.pickle"
    if os.path.exists(gpath):
        os.remove(gpath)


def cleanup_func():
    os.remove("/data/test.pickle")


@with_setup(setup_func, cleanup_func)
def test_initialize_graph():
    MusicalGraph("/data/test.pickle")
