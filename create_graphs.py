import zipfile
import pandas as pd
import json
import networkx as nx
import time
import matplotlib.pyplot as plt


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
    df['index_column'] = df.index
    print("LOADED - Loaded data..")
    return df


def update_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update data to only ~16K nodes AND remove transactions where nan is included
    :param df: dataframe from load_zipped_data
    :return: updated dataframe
    """
    # Remove nan's from to_address and from_address -> (scam contract creations or something)
    df = df[df['to_address'].notna() & df['from_address'].notna()]

    with open('intersection_nodes.json', 'r', encoding='utf8') as json_file:
        intersection_node_list = json.load(json_file)
        df = df[df['from_address'].isin(intersection_node_list) & df['to_address'].isin(intersection_node_list)]

    return df


def get_nodes_and_edges(df: pd.DataFrame) -> ([str], [(str, str, int)]):
    """
    Also based on global parameters APPLY_FILTERING, NODE_PERCENTAGE, EDGE_PERCENTAGE
    :param df: given dataframe from load_data
    :return: nodes, edges - a list of unique nodes, and a list of all edges between them
    """
    # Unique nodes
    unique_nodes = pd.unique(df['from_address'].tolist() + df["to_address"].tolist()).tolist()
    edges = list(zip(df['from_address'].tolist(), df['to_address'].tolist()))
    unique_edges = pd.unique(edges).tolist()
    edge_weight_dict = dict.fromkeys(unique_edges, 0)
    for edge in edges:
        edge_weight_dict[edge] = edge_weight_dict[edge] + 1
    edges_weights = [(node1, node2, w) for ((node1, node2), w) in
                     zip(edge_weight_dict.keys(), edge_weight_dict.values())]
    edges_weights = sorted(edges_weights, key=lambda x: x[2], reverse=True)
    return unique_nodes, edges_weights


if __name__ == "__main__":

    # for year in [2018,2019,2020,2021,2022]:
    for year in [2018]:
        print(f"\nYEAR={year}")
        df = load_zipped_data(year)
        df = update_data(df)
        nodes, edges = get_nodes_and_edges(df)
        
        # Create and save Graph
        G = nx.DiGraph()
        G.add_nodes_from(nodes)
        G.add_weighted_edges_from(edges, weight='weight')
        nx.write_gexf(G, path=f"graphs/intersection_graph_{year}.gexf")

        print(f"no_nodes {G.number_of_nodes()}")
        print(f"no_edges {G.number_of_edges()}")
        print(f"density {nx.density(G)}")

        print(f"no_weakly_connected_components {len([len(wcc) for wcc in nx.weakly_connected_components(G)])}")
        print(f"largest_weakly_connected_components {max([len(wcc) for wcc in nx.weakly_connected_components(G)])}")
        print(f"no_strongly_connected_components {len([len(scc) for scc in nx.strongly_connected_components(G)])}")
        print(f"largest_strongly_connected_components {max([len(scc) for scc in nx.strongly_connected_components(G)])}")
        print(f"no_self_loops {nx.number_of_selfloops(G)}")
        max_5_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree(weight='weight')), reverse=True)[:5]])
        print(f"max 5 degrees: {max_5_degs}")
        max_5_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree(weight=None)), reverse=True)[:5]])
        print(f"max 5 degrees: {max_5_degs}")

        # start_pr = time.time()
        # output_pr = nx.pagerank(G)
        # print(f"pagerank took {time.time()-start_pr:.2f}s")

        # pagerank_values = sorted(output_pr.values(), reverse=True)
        # pagerank_values = [float(i)/sum(pagerank_values) for i in pagerank_values]
        # print(pagerank_values)
        # pagerank_values = [float(i) / max(pagerank_values) for i in pagerank_values]
        # print(pagerank_values)
        # plt.hist(pagerank_values)
        # print(pagerank_values)
        # plt.show()
