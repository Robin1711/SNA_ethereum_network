import json

import pandas as pd
import zipfile
import time
import itertools

def get_nodes_and_edges(df: pd.DataFrame) -> ([str], [(str, str, int)]):
    """
    Also based on global parameters APPLY_FILTERING, NODE_PERCENTAGE, EDGE_PERCENTAGE
    :param df: given dataframe from load_data
    :return: nodes, edges - a list of unique nodes, and a list of all edges between them
    """
    print("\tCOMPUTING - Retrieving nodes and edges..")
    unique_nodes = pd.unique(df['from_address'].tolist() + df["to_address"].tolist()).tolist()

    # Calculate edge weights    # [(str, str)] --> [(str, str, int)]
    edges = list(zip(df['from_address'].tolist(), df['to_address'].tolist()))
    unique_edges = pd.unique(edges).tolist()
    edge_weight_dict = dict.fromkeys(unique_edges, 0)
    for edge in unique_edges:
        edge_weight_dict[edge] = edge_weight_dict[edge] + 1
    edges_weights = [(node1, node2, w) for ((node1, node2), w) in
                     zip(edge_weight_dict.keys(), edge_weight_dict.values())]
    edges_weights = sorted(edges_weights, key=lambda x: x[2], reverse=True)

    return unique_nodes, edges_weights


def load_zipped_data(year: int) -> pd.DataFrame:
    """
    This function loads the data given the year from zipfiles in the folder data.
    :param year: int - year of which the transactional data should be loaded
    :return Dataframe: dataframe with given transactions - Columns = ['Unnamed: 0', 'from_address', 'to_address']
    """
    print(f"YEAR={year}")
    print(f"\tLOADING - Loading data..")
    zf = zipfile.ZipFile(f"data/transactions_{year}_query_df.csv.zip")
    df = pd.read_csv(zf.open(f"transactions_{year}_query_df.csv"))
    df = df[['from_address', 'to_address']]
    return df


def nodes_edges(year: int) -> ([str], [(str, str, int)]):
    start_time_function = time.time()
    ne = get_nodes_and_edges(load_zipped_data(year))
    print(f"In {time.time()-start_time_function:.2f}s\n")
    return ne


def do_intersection(nodes_list: list) -> None:
    intersection_set = set(nodes_list[0])
    for nodes in nodes_list[1:]:
        intersection_set = intersection_set.intersection(set(nodes))

    print(f"\nIntersection of all nodes results in {len(intersection_set)} shared nodes over all networks")
    with open('intersection_nodes.json', 'w+', encoding='utf8') as json_file:
        json.dump(list(intersection_set), json_file, ensure_ascii=False, indent=2)
        print(f"\tWritten to intersection_nodes.json.. ")


def do_union(nodes_list: list) -> None:
    union_set = set(nodes_list[0])
    for nodes in nodes_list[1:]:
        union_set = union_set.union(set(nodes))

    print(f"\nUnion of all nodes results in {len(union_set)} shared nodes over all networks")
    with open('union_nodes.json', 'w+', encoding='utf8') as json_file:
        json.dump(list(union_set), json_file, ensure_ascii=False, indent=2)
        print(f"\tWritten to union_nodes.json..")


if __name__ == "__main__":
    start_time_program = time.time()
    # nodes_2018, _ = nodes_edges(2018)
    # nodes_2019, _ = nodes_edges(2019)
    # nodes_2020, _ = nodes_edges(2020)
    # nodes_2021, _ = nodes_edges(2021)
    # nodes_2022, _ = nodes_edges(2022)
    # nodes_list = [ nodes_2018, nodes_2019, nodes_2020, nodes_2021, nodes_2022 ]

    with open('union_nodes.json', 'r', encoding='utf8') as json_file:
        union_nodes = json.load(json_file)

    with open('intersection_nodes.json', 'r', encoding='utf8') as json_file:
        intersection_nodes = json.load(json_file)

    print(f"Total time loading = {time.time()-start_time_program:.2f}s")
    print(f"#intersection nodes: {len(intersection_nodes)}")
    print(f"#union nodes: {len(union_nodes)}")
    print(f"percentage: {(len(intersection_nodes) / len(union_nodes)) * 100 :.2f}%")
    print(f"\n\nProgram ran for {time.time()-start_time_program:.2f}s\n")

