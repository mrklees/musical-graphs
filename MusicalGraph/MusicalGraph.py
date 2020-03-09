import networkx as nx
import os


class MusicalGraph(object):
    """Object for managing the connection to and abilities of our Graph

    Our graph will have nodes of many types includes artists, songs, and genres. 
    This will ideally help us identify relationships between artists. 
    """
    def __init__(self, write_path='data/build/graph.pickle'):
        self.write_path = write_path
        self.initialize_graph()

    def initialize_graph(self):
        if os.path.exists(self.write_path):
            print("Reading in graph")
            self.G = nx.read_gpickle(self.write_path)
        else:
            print("Generating a new graph")
            self.G = nx.Graph()
            nx.write_gpickle(self.G, self.write_path)

    def save(self):
        nx.write_gpickle(self.G, self.write_path)
