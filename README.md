# SNA_ethereum_network
Our project for Social Network Analysis. We are analysing public Ethereum data to see how the network evolved over the
past years, finding some major players: miners, exchanges, contracts, and even scams. 
The report for this project is also included in the pdf in the folder report 
The data could not be uploaded, however, the graphs we have used in the final version are the updated intersection graphs
under the "graphs" directory.

# Code
The code consists from the following 4 scripts:
* meta_analysis.py
* create_graphs.py ( + intersection.json)
* update_graphs.py
* create_stats.py

Additional analysis on the graphs was done by using Gephi.

### meta_analysis.py
This script was used for some analysis on the huge csv files that we downloaded from the ethereum network.

### create_graphs.py ( + intersection.json)
This script was used for creation of the narrow decreased graphs from the large data.
The script uses the file intersection_nodes.json, which is generated by meta_analysis.py.

### update_graphs.py
This script was used for updates on the created graphs and to create the MultiDiGraph.
This script was also mainly used for outputting graph metrics.

### create_stats.py
This script was created for the purpose of retrieving graph metrics, however has only been used to create the file
analyses/yearly_graph_stats.csv


# Data folder
We were unable to upload al data to github. The BQ.ipynb notebook was used to download the data from Google Big Query.
The data is stored in the data folder under names with the format: transactions_{year}_query_df.csv.zip 
These files are used by create_graphs.py 


# Graphs folder
Graphs are stored in this folder under the names:
* intersection_graph_{year}.gexf
* updated_intersection_graph_{year}.gexf
* multi_digraph.gexf
The narrowed raw graphs are stored in intersection_graph_{year}.gexf. In the files updated_intersection_graph_{year}.gexf
the updated graphs are stored, such that they all contain identical nodes.
MultiDiGraph.gexf contains the complete Mulitplex graph


# Analysis folder
This folder contains 4 files:
* analysis_multi_plus_all_years.csv
* analysis_multiplex.csv
* numbers.json
* yearly_graph_stats.csv

The files analysis_multiplex.csv and analysis_multi_plus_all_years.csv are extracted from Gephi. These contain
node-specific metrics of the MultiDiGraph. The file yearly_graph_stats.csv contains basic overall graph metrics for
each year. The file numbers.json contains information on the large original graphs. We had created them, but they were
to large to work with.