# SNA_ethereum_network
Our project for Social Networks Analysis. We are analysing public Ethereum data to see how the network evolved over the
past years.
.. More about this

# Data
We couldn't upload all the data, since it is huge.
Data is supposed to be in folder data with name being:
transactions_{year}_query_df.csv.zip
Graphs will be saved in the folder graphs with names:
transactions_2019.gexf OR transactions_2019_filtered.gexf
Might upload an example filtered graph to work with and show everything works 

# Code
Everything is in main.py :)
For now decided on DiGraphs with weights, not MultiGraphs with parallel edges
Definitely DiGraph en no 'normal' Graph, since all metrics for a Graph can be deducted from a weighted DiGraph 