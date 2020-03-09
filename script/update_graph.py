import sys
sys.path.append(".")

import pandas as pd
from MusicalGraph.MusicalGraph import MusicalGraph
from MusicalGraph.Worker import Worker

history_path = "./data/s3/history.csv"
graph_path = "./data/s4/graph.pickle"

graph = MusicalGraph(graph_path)
worker = Worker(graph)

history = pd.read_csv(history_path)
artists = list(history.artist.unique())

worker.add_artists(artists)