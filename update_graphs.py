import itertools
import networkx as nx
import random

# BASICS
BASICS = True
COMPONENTS = False
DEGREES = False
DIAMETERS = False
CLIQUES = False
CENTRALITIES = False


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

    print(f"Removing self-loops from Graph")
    G_update.remove_edges_from(nx.selfloop_edges(G_update))
    nx.write_gexf(G_update, f"graphs/updated_intersection_graph_{year}.gexf")
    print_graph_stats(G_update)


def print_graph_stats(G: nx.DiGraph, MDG=False) -> None:
    random_sampled_nodes = ['0xea56fbd68b7cda9f3b3332c7cc5c5c5d5b91b9f0', '0x88c3a16f640248437bfd264d9ad38f7f7051eb65',
     '0x80fb784b7ed66730e8b1dbd9820afd29931aab03']

    # BASICS
    if BASICS:
        print(f"no_nodes {G.number_of_nodes()}")
        print(f"no_edges {G.number_of_edges()}")
        print(f"density {nx.density(G)}")
        print(random.sample(G.nodes, 3))

    # COMPONENTS
    if COMPONENTS:
        print(f"no_weakly_connected_components {len([len(wcc) for wcc in nx.weakly_connected_components(G)])}")
        print(f"largest_weakly_connected_components {max([len(wcc) for wcc in nx.weakly_connected_components(G)])}")
        print(f"no_strongly_connected_components {len([len(scc) for scc in nx.strongly_connected_components(G)])}")
        print(f"largest_strongly_connected_components {max([len(scc) for scc in nx.strongly_connected_components(G)])}")
        print(f"no_self_loops {nx.number_of_selfloops(G)}")

    if DEGREES:
        max_5_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree(weight='weight')), reverse=True)[:5]])
        print(f"weighted max 5 degrees: {max_5_degs}")
        max_5_degs = ', '.join([f'{val}' for val in sorted((d for n, d in G.degree(weight=None)), reverse=True)[:5]])
        print(f"max 5 degrees: {max_5_degs}")
        weighted_degrees = [d for n, d in G.degree(weight='weight')]
        degrees = [d for n, d in G.degree(weight=None)]
        print(f"average weighted degree: {sum(weighted_degrees)/len(weighted_degrees)}")
        print(f"average degree: {sum(degrees)/len(degrees)}")

        print(f"average weighted in/out degree: {(sum(weighted_degrees)/len(weighted_degrees))/2}")
        print(f"average in.out degree: {(sum(degrees)/len(degrees))/2}")

    if DIAMETERS:
        shortest_paths_list = nx.shortest_path_length(G)
        path_lengths = [d.values() for d in [pathdict for (node, pathdict) in shortest_paths_list]]
        path_lengths = [val for val in itertools.chain(*path_lengths)]
        print(f"average shortest path: {sum(path_lengths)/len(path_lengths)}")
        print(f"longest shortest path (diameter): {max(path_lengths)}")

    if CLIQUES:
        clique_list = [clique for clique in nx.find_cliques_recursive(G)]
        print(f"no_cliques: {len(clique_list)}")
        print(f"no_cliques > 1: {len([len(clique) for clique in clique_list if len(clique) > 1])}")
        print(f"no_cliques > 2: {len([len(clique) for clique in clique_list if len(clique) > 2])}")
        print(f"no_cliques > 3: {len([len(clique) for clique in clique_list if len(clique) > 3])}")
        print(f"no_cliques > 4: {len([len(clique) for clique in clique_list if len(clique) > 4])}")
        print(f"no_cliques > 5: {len([len(clique) for clique in clique_list if len(clique) > 5])}")
        max_clique_size = max([len(clique) for clique in clique_list])
        print(f"largest_clique: {max_clique_size}")
        print(f"clique: {clique_list[[len(clique) for clique in clique_list].index(max_clique_size)]}")

    if CENTRALITIES:
        # Clustering Coefficients
        for node in random_sampled_nodes:
            print(f"\tNODE = {node}")
            # Degree centrality
            degree_centralities = nx.degree_centrality(G)
            print(f"\tdegree centrality: {degree_centralities[node]}")

            # Eigenvector centrality
            if not MDG:
                eigenvector_centralities = nx.eigenvector_centrality(G)
                print(f"\teigenvector centrality: {eigenvector_centralities[node]}")

            # Betweenness centrality
            betweenness_centralities = nx.betweenness_centrality(G, k=int(15671*0.01))
            print(f"\tbetweenness centrality: {betweenness_centralities[node]}")

            # Closeness centrality
            closeness_centrality = nx.closeness_centrality(G)
            print(f"\tcloseness centrality: {closeness_centrality[node]}")


if __name__ == "__main__":
    YEARS = [2018, 2019, 2020, 2021, 2022]
    for year in YEARS:
        print(f"\nYEAR = {year}")
        update_graph(year)

    print("\n\nCreate MultiDiGraph")
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

    print("\nMDG Stats:")
    print_graph_stats(MDG, MDG=True)
