import json
import os
import time
import pandas as pd
import zipfile
import networkx as nx
import random
import itertools

SEED = 9131
random.seed(SEED)
# These seem to be representative parameters for creating a graph
# I.e. percentage unique nodes in dataset and number of edges per node
# We can go lower if necessary of course - to decrease dataset even more
NODE_PERCENTAGE = 0.005     #   0.075
EDGE_FRACTION = 0.6         #   0.5
APPLY_FILTERING = False


def load_complete_graph(year: int) -> nx.Graph:
    """
    Load the complete graph from the specified year. NOTE: The graph file should be zipped.
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    :return: The loaded graph, an nx.Graph()
    """
    path = f"graphs/complete_graphs/transactions_{year}.gexf.zip"
    print(f"LOADING - Loading complete graph from file: {path}")
    zf = zipfile.ZipFile(path)
    graph = nx.read_gexf(zf.open(f"graphs/complete_graphs/transactions_{year}.gexf"))
    print(f"LOADED - Loaded complete graph from file")
    return graph


def load_filtered_graph(year: int) -> nx.Graph:
    """
    Load the filtered graph from the specified year
    :param year: int, year of the graph, needed for specification of the file path name to be stored in
    :return: The loaded graph, an nx.Graph()
    """
    path = f"graphs/filtered_graphs/transactions_{year}_filtered.gexf"
    print(f"LOADING - Loading filtered graph from file: {path}")
    graph = nx.read_gexf(path=path)
    print(f"LOADED - Loaded filtered graph from file")
    return graph


def load_graph(year: int, filtering: bool) -> nx.Graph:
    return load_complete_graph(year) if not filtering else load_filtered_graph(year)


def write_graph(year: int, G: nx.Graph, filtering=APPLY_FILTERING) -> None:
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
        # Remove the generated non-zipped file
        os.remove(path)

    print(f"SAVED - Saved graph to file: {path}")


def write_json(year: int, to_be_written: dict, filtering=APPLY_FILTERING, file_name: str="numbers.json") -> None:
    """
    Write given dictionary to specified json file for the specified year.
    :param year: year for which data has to be written
    :param to_be_written: dictionary to be added to the json data
    :param filtering: True | False
    :param file_name: json file to be written to
    """
    graph_numbers = dict()
    if os.path.exists(file_name):
        with open('numbers.json', 'r') as json_file:
            graph_numbers = json.load(json_file)

    primal_key = f"{year}" if not filtering else f"{year}_filtered"
    if primal_key not in graph_numbers.keys():
        graph_numbers[primal_key] = dict()

    for key in to_be_written:
        graph_numbers[primal_key][key] = to_be_written[key]

    with open('numbers.json', 'w+', encoding='utf8') as json_file:
        json.dump(graph_numbers, json_file, ensure_ascii=False, indent=2, sort_keys=True)


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
    print("LOADED - Loaded data..")
    return df


def get_nodes_and_edges(year: int, df: pd.DataFrame, filtering=APPLY_FILTERING) -> ([str], [(str, str, int)]):
    """
    Also based on global parameters APPLY_FILTERING, NODE_PERCENTAGE, EDGE_PERCENTAGE
    :param df: given dataframe from load_data
    :return: nodes, edges - a list of unique nodes, and a list of all edges between them
    """
    data_stat_dict = dict()
    print("COMPUTING - Retrieving nodes and edges..")
    unique_nodes = pd.unique(df['from_address'].tolist() + df["to_address"].tolist()).tolist()
    if filtering:
        unique_nodes = random.choices(list(unique_nodes), k=int(len(unique_nodes) * NODE_PERCENTAGE))
        boolean_series_from = df['from_address'].isin(unique_nodes)
        boolean_series_to = df['to_address'].isin(unique_nodes)
        df_filtered = pd.concat([df[boolean_series_from], df[boolean_series_to]])
        df_filtered = df_filtered.sample(frac=EDGE_FRACTION, random_state=SEED)
        print(f"\tNOTE: Filtering has been applied - limiting the unique nodes and limiting the edges related to them")
        print(f"\t\tThe dataset used now is {((df_filtered.shape[0] / df.shape[0]) * 100):.2f}% of the original")
        data_stat_dict['percentage_data_used'] = f"{((df_filtered.shape[0] / df.shape[0]) * 100):.2f}%"
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
    data_stat_dict['percentage_unique_nodes'] = f"{(len(unique_nodes) / df.shape[0]) * 100:.2f}%"
    data_stat_dict['percentage_unique_edges'] = f"{(len(set(edges)) / df.shape[0]) * 100:.2f}%"
    data_stat_dict['mean_edges_per_node'] = f"{len(edges) / len(unique_nodes):.2f}"
    write_json(year, data_stat_dict, filtering=filtering)
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
    if os.path.exists(path) and load_from_file:
        start_time = time.time()
        G = load_graph(year=year, filtering=filtering)
        load_graph_from_file = time.time() - start_time
        print(f"LOADED - Loaded graph from file in {(load_graph_from_file):.2f}s")
    else:  # file does not exist or user wants to newly create graph
        start_time = time.time()
        df = load_zipped_data(year=year)
        nodes, edges = get_nodes_and_edges(year, df, filtering=filtering)
        G = create_graph(nodes, edges)
        load_data_create_graph_time = time.time() - start_time
        print(f"LOADED - Loaded data and created graph from file in {(load_data_create_graph_time):.2f}s")
        save_to_file = save_to_file if save_to_file is not None else input(f"Save created graph for year={year} and filtering={filtering}? Y/n  ") == 'y'
        if save_to_file:
            start_time = time.time()
            write_graph(year, G, filtering=filtering)
            save_graph_to_file = time.time() - start_time
            print(f"SAVED - Saved the newly created graph to a gexf file in {(save_graph_to_file):.2f}s")
    return G


def graph_stats(G: nx.Graph) -> dict:
    """
    Save numbers for a graph to a json file
    Statistics/metrics for general comparison between graphs
    :param G: Given graph generated by one of the transaction datasets
    """
    print("COMPUTING - Graph statistics")
    # Basics
    start_basics = time.time()
    numbers_dict = dict()
    numbers_dict["basics"] = dict()
    numbers_dict["basics"]["no_nodes"] = G.number_of_nodes()
    numbers_dict["basics"]["no_di_edges"] = G.number_of_edges()
    numbers_dict["basics"]["density"] = nx.density(G)
    print(f"\tBasics in {time.time()-start_basics:.2f}s")

    # Connectedness
    start_connectedness = time.time()
    numbers_dict["connectedness"] = dict()
    numbers_dict["connectedness"]["no_weakly_connected_components"] = len([len(wcc) for wcc in nx.weakly_connected_components(G)])
    numbers_dict["connectedness"]["largest_weakly_connected_components"] = max([len(wcc) for wcc in nx.weakly_connected_components(G)])
    numbers_dict["connectedness"]["no_strongly_connected_components"] = len([len(scc) for scc in nx.strongly_connected_components(G)])
    numbers_dict["connectedness"]["largest_strongly_connected_components"] = max([len(scc) for scc in nx.strongly_connected_components(G)])

    numbers_dict["connectedness"]["percentage_nodes_largest_weakly_connected_component"]\
    = f"{numbers_dict['connectedness']['largest_weakly_connected_components'] / numbers_dict['basics']['no_nodes'] * 100:.2f}%"
    numbers_dict["connectedness"]["percentage_nodes_largest_strongly_connected_component"]\
        = f"{numbers_dict['connectedness']['largest_strongly_connected_components']/numbers_dict['basics']['no_nodes'] * 100:.2f}%"
    print(f"\tConnectedness in {time.time()-start_connectedness:.2f}s")

    # Paths
    start_paths = time.time()
    numbers_dict["paths"] = dict()
    numbers_dict["paths"]["no_self_loops"] = nx.number_of_selfloops(G)
    # all_shortest_paths = list(itertools.chain(*[path_dict.values() for (node, path_dict) in nx.shortest_path_length(G)])) # [(str, {str -> int})]
    # numbers_dict["paths"]["average_shortest_path"] = sum(all_shortest_paths)/len(all_shortest_paths)
    # numbers_dict["paths"]["diameter_longest_shortest_path"] = max(all_shortest_paths)
    print(f"\tPaths in {time.time()-start_paths:.2f}s")

    # Centralities
    # start_centralities = time.time()
    # numbers_dict["centralities"] = dict()
    # max_5_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree()), reverse=True)[:5]])
    # print(f"\t\tDegs done")
    # max_5_closeness = ', '.join([f'{val:.5f}' for val in sorted((val for val in nx.closeness_centrality(G).values()), reverse=True)[:5]])
    # print(f"\t\tCloseness done")
    # # max_5_betweennes = ', '.join([f'{val:.5f}' for val in sorted((val for val in nx.betweenness_centrality(G).values()), reverse=True)[:5]])
    # # print(f"\t\tBetweeenness done")
    # max_5_eigenvectors = ', '.join([f'{val:.5f}' for val in sorted((val for val in nx.eigenvector_centrality(G, max_iter=99999).values()), reverse=True)[:5]])
    # print(f"\t\tEigenvectors done")
    # numbers_dict["centralities"]["max_5_degree_centralities"] = max_5_degs
    # numbers_dict["centralities"]["max_5_eigenvector_centralities"] = max_5_eigenvectors
    # numbers_dict["centralities"]["max_5_closeness_centralities"] = max_5_closeness
    # # numbers_dict["centralities"]["max_5_betweenness_centralities"] = max_5_betweennes
    # print(f"\tCentralities in {time.time()-start_centralities:.2f}s")
    print(f"COMPUTED - Graph statistics in {time.time()-start_basics:.2f}s")
    return numbers_dict


if __name__ == "__main__":
    year = int(input("What year would you like to analyse? 20xx [2018,2019,2020,2021,2022]\n"))
    print(f"\n\nYEAR = {year}\n")
    start_year = time.time()
    G = load_create_graph(year=year, load_from_file=False, save_to_file=True)
    numbers_dict = graph_stats(G)
    write_json(year, to_be_written=numbers_dict)
    print(f"\nYEAR = {year} done in {time.time()-start_year:.2f}s\n")

