import networkx as nx
import pandas as pd


def load_multidigraph() -> nx.Graph:
    return nx.read_gexf(path=f"graphs/multi_digraph.gexf")


def load_updated_graph(year: int) -> nx.Graph:
    return nx.read_gexf(path=f"graphs/updated_intersection_graph_{year}.gexf")


def load_graph(year: int) -> nx.Graph:
    return nx.read_gexf(path=f"graphs/intersection_graph_{year}.gexf")


def graph_stats(G: nx.Graph) -> dict:
    stats_dict = dict()
    stats_dict['no_edges'] = G.number_of_edges()
    stats_dict['no_nodes'] = G.number_of_nodes()
    stats_dict['density'] = nx.density(G)  # f"{nx.density(G):.8f}"
    stats_dict['no_self_loops'] = nx.number_of_selfloops(G)

    weakly_connected_components = [wcc for wcc in nx.weakly_connected_components(G)]
    stats_dict['no_weakly_connected_components'] = len([len(wcc) for wcc in weakly_connected_components])
    stats_dict['largest_weakly_connected_component'] = max([len(wcc) for wcc in weakly_connected_components])

    strongly_connected_components = [wcc for wcc in nx.strongly_connected_components(G)]
    stats_dict['no_strongly_connected_components'] = len([len(wcc) for wcc in strongly_connected_components])
    stats_dict['largest_strongly_connected_component'] = max([len(wcc) for wcc in strongly_connected_components])

    max_degs = sorted([deg for deg in G.degree(weight=None)], reverse=True)
    stats_dict['5_largest_degrees'] = [d for n, d in max_degs[:5]]
    stats_dict['5_nodes_largest_degrees'] = [n for n, d in max_degs[:5]]

    max_weighted_degs = sorted([deg for deg in G.degree(weight='weight')], reverse=True)
    stats_dict['5_largest_weighted_degrees'] = [d for n, d in max_weighted_degs[:5]]
    stats_dict['5_nodes_largest_weighted_degrees'] = [n for n, d in max_weighted_degs[:5]]

    return stats_dict


if __name__ == "__main__":
    YEARS = [2018, 2019, 2020, 2021, 2022]
    colnames = ['year', 'no_edges', 'no_nodes', 'density', 'no_self_loops'
        , 'no_weakly_connected_components', 'largest_weakly_connected_component'
        , 'no_strongly_connected_components', 'largest_strongly_connected_component'
        , '5_largest_degrees', '5_nodes_largest_degrees'
        , '5_largest_weighted_degrees', '5_nodes_largest_weighted_degrees'
                ]
    df = pd.DataFrame(columns=colnames)
    print(f"df_shape = {df.shape}")

    for year in YEARS:
        G = load_graph(year)
        stats_dict = graph_stats(G)
        stats_dict['year'] = f"{year}"
        new_stats_df = pd.DataFrame(pd.Series(stats_dict).to_frame().T)
        df = pd.concat([df, new_stats_df], ignore_index=True)

    for year in YEARS:
        G = load_updated_graph(year)
        stats_dict = graph_stats(G)
        stats_dict['year'] = f"{year}_updated"
        new_stats_df = pd.DataFrame(pd.Series(stats_dict).to_frame().T)
        df = pd.concat([df, new_stats_df], ignore_index=True)

    df.to_csv('yearly_graph_stats.csv', index=False, mode='w+')
