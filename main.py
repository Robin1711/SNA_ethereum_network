import os
import time
import pandas as pd
import zipfile
import networkx as nx
import random
from os.path import exists

SEED = 9131
random.seed(SEED)
# These seem to be representative parameters for creating a graph
# I.e. percentage unique nodes in dataset and number of edges per node
# We can go lower if necessary ofcourse - to decrease dataset even more
NODE_PERCENTAGE = 0.075
EDGE_FRACTION = 0.5
APPLY_FILTERING = True


def load_complete_graph(year: int) -> nx.Graph:
    """
    Load the complete graph from the specified year. NOTE: The graph file should be zipped.
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    :return: The loaded graph, an nx.Graph()
    """
    path = f"graphs/complete_graphs/transactions_{year}.gexf.zip"
    print(f"LOADING - Loading complete graph from file: {path}")
    zf = zipfile.ZipFile(path)
    return nx.read_gexf(zf.open(f"transactions_{year}.gexf"))


def load_filtered_graph(year: int) -> nx.Graph:
    """
    Load the filtered graph from the specified year
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    :return: The loaded graph, an nx.Graph()
    """
    path = f"graphs/filtered_graphs/transactions_{year}_filtered.gexf"
    print(f"LOADING - Loading filtered graph from file: {path}")
    return nx.read_gexf(path=path)


def load_graph(year: int, filtering: bool) -> nx.Graph:
    return load_complete_graph(year) if not filtering else load_filtered_graph(year)


def save_graph(G: nx.Graph, year: int, filtering=APPLY_FILTERING) -> None:
    """
    Save the given graph to a file with the specified year, also dependent on APPLY_FILTERING
    :param G: nx.Graph, graph to be saved
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    """
    print("SAVING - Saving graph..")
    if filtering:
        path = f"graphs/filtered_graphs/transactions_{year}_filtered.gexf"
        nx.write_gexf(G, path=path)
    else:
        path = f"graphs/complete_graphs/transactions_{year}.gexf"
        nx.write_gexf(G, path=path)
        with zipfile.ZipFile(f"graphs/complete_graphs/transactions_{year}.gexf.zip", 'w') as zip:
            zip.write(f"graphs/complete_graphs/transactions_{year}.gexf")
        # Remove the genereted non-zipped file
        os.remove(path)


def load_zipped_data(year: int) -> pd.DataFrame:
    """
    This function loads the data given the year from zipfiles in the folder data.
    :param year: int - year of which the transactional data should be loaded
    :return Dataframe: dataframe with given transactions - Columns = ['Unnamed: 0', 'from_address', 'to_address']
    """
    print("LOADING - Loading data..")
    zf = zipfile.ZipFile(f"data/transactions_{year}_query_df.csv.zip")
    df = pd.read_csv(zf.open(f"transactions_{year}_query_df.csv"))
    df = df[['from_address', 'to_address']]
    return df


def get_nodes_and_edges(df: pd.DataFrame, filtering=APPLY_FILTERING) -> ([str], [(str, str, int)]):
    """
    Also based on global parameters APPLY_FILTERING, NODE_PERCENTAGE, EDGE_PERCENTAGE
    :param df: given dataframe from load_data
    :return: nodes, edges - a list of unique nodes, and a list of all edges between them
    """
    print("COMPUTING - Retrieving nodes and edges..")
    unique_nodes = pd.unique(df['from_address'].tolist() + df["to_address"].tolist()).tolist()
    if filtering:
        unique_nodes = random.choices(list(unique_nodes), k=int(len(unique_nodes) * NODE_PERCENTAGE))
        boolean_series_from = df['from_address'].isin(unique_nodes)
        boolean_series_to = df['to_address'].isin(unique_nodes)
        df_filtered = pd.concat([df[boolean_series_from], df[boolean_series_to]])
        df_filtered = df_filtered.sample(frac=EDGE_FRACTION, random_state=SEED)
        print(f"\t\tNOTE: Filtering has been applied - limiting the unique nodes and limiting the edges related to them")
        print(f"\t\t\tThe dataset used now is {((df_filtered.shape[0] / df.shape[0]) * 100):.2f}% of the original")
        df = df_filtered

    # Calculate edge weights    # [(str, str)] --> [(str, str, int)]
    edges = list(zip(df['from_address'].tolist(), df['to_address'].tolist()))
    unique_edges = pd.unique(edges).tolist()
    edge_weight_dict = dict.fromkeys(unique_edges, 0)
    for edge in unique_edges:
        edge_weight_dict[edge] = edge_weight_dict[edge] + 1
    edges_weights = [(node1, node2, w) for ((node1, node2), w) in
                     zip(edge_weight_dict.keys(), edge_weight_dict.values())]
    edges_weights = sorted(edges_weights, key=lambda x: x[2], reverse=True)

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
    print("COMPUTING - Creating graph..")
    G = nx.DiGraph()
    G.add_nodes_from(nodes)

    ###    If no weights needed - use below:
    # edges = [(n_1, n_2) for (n_1, n_2, weight) in edges]
    # G.add_edges_from(edges)

    ###   With weights - use below:
    G.add_weighted_edges_from(edges)
    return G


def load_create_graph(year: int, filtering=APPLY_FILTERING, load_from_file=None, save_to_file=None) -> nx.Graph:
    path = f"graphs/complete_graphs/transactions_{year}.gexf.zip" if not filtering else f"graphs/filtered_graphs/transactions_{year}_filtered.gexf"
    load_from_file = load_from_file if load_from_file is not None else input(f"\nFile {path} exists. Would you like to use this? Y/n  ").lower() == 'y'
    if exists(path) and load_from_file:
        start_time = time.time()
        G = load_graph(year=year, filtering=filtering)
        load_graph_from_file = time.time() - start_time
        print(f"LOADED - Loaded graph from file in {(load_graph_from_file):.2f}s")
    else:  # file does not exist or user wants to newly create graph
        start_time = time.time()
        df = load_zipped_data(year=year)
        nodes, edges = get_nodes_and_edges(df, filtering=filtering)
        G = create_graph(nodes, edges)
        load_data_create_graph_time = time.time() - start_time
        print(f"LOADED - Loaded data and created graph from file in {(load_data_create_graph_time):.2f}s")
        save_to_file = save_to_file if save_to_file is not None else input(f"Save created graph for year={year} and filtering={filtering}? Y/n  ") == 'y'
        if save_to_file:
            start_time = time.time()
            save_graph(G, year=year, filtering=filtering)
            save_graph_to_file = time.time() - start_time
            print(f"SAVED - Save the newly created graph to a gexf file {(save_graph_to_file):.2f}s")
    return G


def print_graph_stats(G: nx.Graph) -> None:
    """
    Now prints some basic graph statistics/metrics for general comparison between graphs
    :param G: Given graph generated by one of the transaction datasets
    """
    print(f"\tGRAPH STATS:\n\t#e: {G.number_of_edges()}, #n: {G.number_of_nodes()}")
    max_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree()), reverse=True)[:5]])
    print(f"\t\ttop 5 maximal degrees: {max_degs}")


if __name__ == "__main__":
    print("Filterede graphs examples:")
    print("Year=2020, filtering=True, load_from_file=False, save_to_file=True")
    G = load_create_graph(2020, filtering=True, load_from_file=False, save_to_file=True)
    print_graph_stats(G)

    print("\n\nYear=2020, filtering=True, load_from_file=True")
    G = load_create_graph(2020, filtering=True, load_from_file=True)
    print_graph_stats(G)

    print("\n\n\nComplete graphs examples")
    print("Year=2020, filtering=False, load_from_file=False, save_to_file=True")
    G = load_create_graph(2020, filtering=False, load_from_file=False, save_to_file=True)
    print_graph_stats(G)

    print("\n\nYear=2020, filtering=False, load_from_file=True")
    G = load_create_graph(2020, filtering=False, load_from_file=True)
    print_graph_stats(G)
