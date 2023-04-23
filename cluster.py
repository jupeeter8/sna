from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from neo4j.data import Record
from neo4j import unit_of_work
from neo4j import Result

from py2neo import Graph, Node, Relationship

from typing import List, Tuple

from collections import defaultdict
from itertools import combinations
import networkx as nx

from sklearn.preprocessing import LabelEncoder
from sklearn.cluster import KMeans

import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234567890"))

# Define a function to extract data from Neo4j graph
@unit_of_work(timeout=10.0)
def get_nodes_and_relationships(tx) -> Tuple[List[Node], List[Relationship]]:
    nodes = []
    relationships = []

    # Query for all nodes and relationships in the graph
    node_query = "MATCH (n) RETURN n"
    rel_query = "MATCH ()-[r]->() RETURN r"

    for record in tx.run(node_query):
        nodes.append(record["n"])

    for record in tx.run(rel_query):
        relationships.append(record["r"])

    return nodes, relationships

# Define a function to create a graph from Neo4j data
def create_graph(nodes: List[Node], relationships: List[Relationship]) -> nx.Graph:
    graph = nx.Graph()

    # Add nodes to the graph
    for node in nodes:
        graph.add_node(node.id, profession=node["profession"])

    # Add edges to the graph
    for relationship in relationships:
        start_node_id = relationship.start_node.id
        end_node_id = relationship.end_node.id
        graph.add_edge(start_node_id, end_node_id)

    return graph

# Define a function to perform clustering on the graph
def perform_clustering(graph: nx.Graph):
    # Get the profession of each node
    profession_dict = dict(graph.nodes(data="profession"))
    professions = list(profession_dict.values())

    # Encode professions as integers for clustering
    le = LabelEncoder()
    profession_labels = le.fit_transform(professions)

    # Calculate the adjacency matrix of the graph
    adjacency_matrix = nx.adjacency_matrix(graph)

    # Perform clustering using KMeans
    kmeans = KMeans(n_clusters=3)
    clusters = kmeans.fit_predict(adjacency_matrix)

    # Create a dictionary mapping node IDs to cluster labels
    cluster_dict = dict(zip(list(graph.nodes()), clusters))

    # Create a dictionary mapping professions to cluster labels
    profession_cluster_dict = defaultdict(list)
    for node_id, cluster_label in cluster_dict.items():
        profession = profession_dict[node_id]
        profession_cluster_dict[profession].append(cluster_label)

    # Calculate the average number of nodes in each cluster for each profession
    profession_cluster_size_dict = {}
    for profession, cluster_labels in profession_cluster_dict.items():
        cluster_size_dict = defaultdict(int)
        for cluster_label in cluster_labels:
            cluster_size_dict[cluster_label] += 1
        profession_cluster_size_dict[profession] = dict(cluster_size_dict)

    print(profession_cluster_size_dict)

# Connect to Neo4j and extract data from the graph
with driver.session() as session:
    try:
        nodes, relationships = session.write_transaction(get_nodes_and_relationships)
        graph = create_graph(nodes, relationships)
        perform_clustering(graph)
    except ServiceUnavailable as ex:
        print("The database is currently unavailable: ", ex)
