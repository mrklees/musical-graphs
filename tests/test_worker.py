from nose.tools import with_setup
import os
from MusicalGraph.MusicalGraph import MusicalGraph
from MusicalGraph.Worker import Worker


def setup_func():
    gpath = "./data/test/test.pickle"
    if os.path.exists(gpath):
        os.remove(gpath)
    MusicalGraph(gpath)


def cleanup_func():
    os.remove("./data/test/test.pickle")


@with_setup(setup_func, cleanup_func)
def test_credential():
    graph = MusicalGraph("./data/test/test.pickle")
    worker = Worker(graph)
    worker.generate_token()


def test_add_str_artsts():
    graph = MusicalGraph("./data/test/test.pickle")
    worker = Worker(graph)
    try:
        worker.add_artists("Logic")
    except AssertionError:
        pass


def test_add_list_artsts():
    graph = MusicalGraph("./data/test/test.pickle")
    worker = Worker(graph)
    worker.add_artists(["Logic"])
    assert "Logic" in graph.G.nodes


def test_search():
    graph = MusicalGraph("./data/test/test.pickle")
    worker = Worker(graph)
    response = worker.search_for_artist("Logic")
    assert response['name'] == "Logic"


def test_search_process():
    graph = MusicalGraph("./data/test/test.pickle")
    worker = Worker(graph)
    response = worker.search_for_artist("Logic")
    processed = worker.get_artist_meta(response)
    print(processed)
    assert len(processed) == 3
    assert 'id' in processed.keys()


def test_add_genre():
    graph = MusicalGraph("./data/test/test.pickle")
    worker = Worker(graph)
    worker.add_artists(["Logic"])
    worker.add_genres(['Logic'])
    assert len(graph.G.nodes) > 1
