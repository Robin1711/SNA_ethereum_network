import zipfile
import pandas as pd
import json
import networkx as nx
import time
import matplotlib.pyplot as plt


def load_updated_graph(year: int) -> nx.Graph:
    path = f"graphs/updated_intersection_graph_{year}.gexf"
    graph = nx.read_gexf(path=path)
    return graph


def load_graph(year: int) -> nx.Graph:
    """
    Load the complete graph from the specified year. NOTE: The graph file should be zipped.
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    :return: The loaded graph, an nx.Graph()
    """
    path = f"graphs/intersection_graph_{year}.gexf"
    graph = nx.read_gexf(path=path)
    return graph


def update_graph_with_graph(G1: nx.Graph, G2: nx.Graph) -> nx.Graph:
    node_difference = set(G2.nodes).difference(set(G1.nodes))
    print(f"updating with {len(node_difference)} nodes")
    G1.add_nodes_from(node_difference)
    return G1.copy()


def update_graph(year: int) -> None:
    G_update = load_graph(year)
    for update_year in {2018, 2019, 2020, 2021, 2022}.difference({year}):
        G_update = update_graph_with_graph(G_update, load_graph(update_year))

    # print(f"Removing self-loops from Graph")
    # G_update.remove_edges_from(nx.selfloop_edges(G_update))
    nx.write_gexf(G_update, f"graphs/updated_intersection_graph_{year}.gexf")
    print_graph_stats(G_update)


def print_graph_stats(G: nx.Graph) -> None:
    print(f"no_nodes {G.number_of_nodes()}")
    print(f"no_edges {G.number_of_edges()}")
    print(f"density {nx.density(G)}")

    print(f"no_weakly_connected_components {len([len(wcc) for wcc in nx.weakly_connected_components(G)])}")
    print(f"largest_weakly_connected_components {max([len(wcc) for wcc in nx.weakly_connected_components(G)])}")
    print(f"no_strongly_connected_components {len([len(scc) for scc in nx.strongly_connected_components(G)])}")
    print(f"largest_strongly_connected_components {max([len(scc) for scc in nx.strongly_connected_components(G)])}")
    print(f"no_self_loops {nx.number_of_selfloops(G)}")
    max_5_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree(weight='weight')), reverse=True)[:5]])
    print(f"weighted max 5 degrees: {max_5_degs}")
    max_5_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree(weight=None)), reverse=True)[:5]])
    print(f"max 5 degrees: {max_5_degs}")


if __name__ == "__main__":
    YEARS = [2018, 2019, 2020, 2021, 2022]
    for year in YEARS:
        print(f"\nYEAR = {year}")
        update_graph(year)

    print("Create MultiDiGraph")
    graph_list = [load_updated_graph(y) for y in YEARS]
    MDG = nx.MultiDiGraph()
    MDG.add_nodes_from(graph_list[0])  # Adding from first entry year=2018, because nodes are same in every year

    for route_idx, graph in zip(YEARS, graph_list):
        # Using year to be route_index
        edges = [(n1, n2, {'label': route_idx, 'route': route_idx, 'weight': graph.get_edge_data(n1, n2)['weight']})
                 for (n1, n2) in graph.edges()]
        keys = MDG.add_edges_from(edges)
        print(MDG)

    graph = nx.write_gexf(MDG, path="graphs/multi_digraph.gexf")
    for n in [n for n in graph_list[0].nodes()][:5]:
        print(MDG[n])
        print()
