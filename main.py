import time
import pandas as pd
import zipfile
import networkx as nx
import random
import numpy as np

random.seed(190)
# These seem to be representative parameters for creating a graph
# I.e. percentage unique nodes in dataset and number of edges per node
# We can go lower if necessary ofcourse - to decrease dataset even more
NODE_PERCENTAGE = 0.1
EDGE_FRACTION = 0.5
APPLY_FILTERING = True


def load_graph(year: int, filtered=APPLY_FILTERING) -> nx.Graph:
    """
    Load the graph from the specified year
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    :return: The loaded graph, an nx.Graph()
    """
    print("Loading graph..")
    path = f"graphs/transactions_{year}.gexf" if not filtered else f"graphs/transactions_{year}_filtered.gexf"
    return nx.read_gexf(path=path)


def save_graph(G: nx.Graph, year: int, filtered=APPLY_FILTERING) -> None:
    """
    Save the given graph to a file with the specified year, also dependent on APPLY_FILTERING
    :param G: nx.Graph, graph to be saved
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    """
    print("Saving graph..")
    path = f"graphs/transactions_{year}.gexf" if not filtered else f"graphs/transactions_{year}_filtered.gexf"
    nx.write_gexf(G, path=path)


def load_data(year=2018) -> pd.DataFrame:
    """
    This function loads the data given the year from zipfiles in the folder data.
    :param year: int - year of which the transactional data should be loaded
    :return Dataframe: dataframe with given transactions - Columns = ['Unnamed: 0', 'from_address', 'to_address']
    """
    print("Loading data..")
    zf = zipfile.ZipFile(f"data/transactions_{year}_query_df.csv.zip")
    df = pd.read_csv(zf.open(f"transactions_{year}_query_df.csv"))
    df = df[['from_address', 'to_address']]
    return df


def get_nodes_and_edges(df: pd.DataFrame) -> ([str], [(str, str, int)]):
    """
    Also based on global parameters APPLY_FILTERING, NODE_PERCENTAGE, EDGE_PERCENTAGE
    :param df: given dataframe from load_data
    :return: nodes, edges - a list of unique nodes, and a list of all edges between them
    """
    print("Retrieving nodes and edges..")
    unique_nodes = np.unique(df['from_address'].tolist() + df["to_address"].tolist()).tolist()
    if APPLY_FILTERING:
        unique_nodes = random.choices(list(unique_nodes), k=int(len(unique_nodes)*NODE_PERCENTAGE))
        boolean_series_from = df['from_address'].isin(unique_nodes)
        boolean_series_to = df['to_address'].isin(unique_nodes)
        df = pd.concat([df[boolean_series_from], df[boolean_series_to]])
        df = df.sample(frac=EDGE_FRACTION)

    # [(str, str)] --> [(str, str, int)]
    edges = list(zip(df['from_address'].tolist(), df['to_address'].tolist()))
    # edges_weights = [(node1, node2, edges.count((node1, node2))) for (node1, node2) in set(edges)]    # works, but takes long O(n^2)
    print(f"edges: {edges[:3]}")
    edge_weight_dict = dict.fromkeys(set(edges), 0)
    for edge in set(edges):     # works, and is faster O(n)
        edge_weight_dict[edge] += 1

    # flatten dict
    edges_weights = [(node1, node2, w) for ((node1, node2), w) in zip(edge_weight_dict.keys(), edge_weight_dict.values())]
    edges_weights = sorted(edges_weights, key=lambda x: x[2], reverse=True)
    print(f"edges_weights: {edges_weights[:3]}")
    print("\tUsed data - statistics:")
    print(f"\t\t#{len(unique_nodes)} unique nodes/addresses involved in {df.shape[0]} transactions = {(len(unique_nodes) / df.shape[0]) * 100:.2f}%")
    print(f"\t\t#{len(set(edges))} unique edges/transactions of all {df.shape[0]} transactions = {(len(set(edges)) / df.shape[0]) * 100:.2f}%")
    print(f"\t\t#edges: {len(edges)} -> {len(edges) / len(unique_nodes):.2f} edge per node")
    return unique_nodes, edges_weights


def create_graph(nodes: [str], edges: [((str, str), int)]) -> nx.Graph:
    """
    Creating graph from the given nodes and edges. It is assumed the nodes are unique, and the edges do not include
    nodes that are not in the supplied nodes
    :param nodes: list of strings, the addresses of the wallets
    :param edges: list of tuples, where the elements are two strings - the addresses of the transaction
    :return: The loaded graph, an nx.Graph()
    """
    print("Creating graph..")
    edges = [(n_1, n_2) for (n_1, n_2, weight) in edges]
    G = nx.DiGraph()
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)
    return G


def print_graph_stats(G: nx.Graph) -> None:
    """
    Now prints some basic graph statistics/metrics for general comparison between graphs
    :param G: Given graph generated by one of the transaction datasets
    """
    print(f"GRAPH STATS:\n\t#e: {G.number_of_edges()}, #n: {G.number_of_nodes()}")
    max_degs = ', '.join([f'{val}'for val in sorted((d for n, d in G.degree()), reverse=True)[:5]])
    print(f"\ttop 5 maximal degrees: {max_degs}")


if __name__ == "__main__":
    # year, mock = 2018, True
    years = [2018, 2019, 2020, 2021, 2022]
    year = 2019

    start_time = time.time()
    df = load_data(year=year)
    nodes, edges = get_nodes_and_edges(df)
    G = create_graph(nodes, edges)
    load_data_create_graph_time = time.time()-start_time

    print_graph_stats(G)
    save_graph(G, year=year)

    start_time = time.time()
    G = load_graph(year=year)
    load_graph_from_file = time.time()-start_time

    print(f"\nTime loading data and creating graph = {load_data_create_graph_time:.2f}s")
    print(f"Time loading graph from file.gexf = {(load_graph_from_file):.2f}s")