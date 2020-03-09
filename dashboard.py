import pandas as pd
import networkx as nx
from bokeh.plotting import figure
from bokeh.models import Range1d, MultiLine, Circle, HoverTool, BoxZoomTool, ResetTool
from bokeh.models.graphs import from_networkx
from bokeh.transform import factor_cmap
import streamlit as st

from MusicalGraph.MusicalGraph import MusicalGraph

st.title('Exploring My Music Preferences Graphically')

page = st.sidebar.selectbox(
  "Which graph do you want to look at",
  ("All Artists Graph", "Detailed Graph")
)

data_path = './data/s3/history.csv'
graph_path = './data/s4/graph.pickle'


def load_graph(graph_path):
    graph = MusicalGraph(graph_path)
    return graph


def plot_artist_graph(graph):
    plot = figure(
        title="Full Artist Graph",
        plot_width=1200,
        plot_height=800,
        x_range=Range1d(-1.1, 1.1),
        y_range=Range1d(-1.1, 1.1)
    )

    node_hover_tool = HoverTool(tooltips=[("index", "@index"), ("type", "@type")])
    plot.add_tools(node_hover_tool, BoxZoomTool(), ResetTool())

    graph_renderer = from_networkx(
        graph,
        nx.kamada_kawai_layout,
        scale=1,
        center=(0, 0)
    )

    graph_renderer.node_renderer.glyph = Circle(
        size=10#, 
        #fill_color=factor_cmap('type', 'Spectral4', ("actor", "genre"))
    )
    graph_renderer.edge_renderer.glyph = MultiLine(line_alpha=0.8, line_width=1)
    plot.renderers.append(graph_renderer)
    return plot


if page == "All Artists Graph":
    graph = load_graph(graph_path)
    graph.G.remove_nodes_from(list(nx.isolates(graph.G)))
    st.write(plot_artist_graph(graph.G))
