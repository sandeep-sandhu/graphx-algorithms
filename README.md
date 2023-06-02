# Graph algorithms in Apache Spark GraphX

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/sandeep-sandhu/graphx-algorithms/graphs/commit-activity)
![Issues open](https://img.shields.io/github/issues/sandeep-sandhu/graphx-algorithms.svg)
![last committed](https://img.shields.io/github/last-commit/sandeep-sandhu/graphx-algorithms)
![Build Status](https://github.com/sandeep-sandhu/graphx-algorithms/actions/workflows/scala.yml/badge.svg)
![License Type](https://img.shields.io/github/license/sandeep-sandhu/graphx-algorithms.svg)


This repository contains an implementation of several graph algorithms on the
Apache Spark GraphX platform in scala language.

The following algorithms have been implemented:
1. Shortest path finding
2. Triangle counting
3. Degree counting

The algorithms will be implemented using either the GraphX message-passing API or
the pregel API available in Spark GraphX.
The method will be chosen based on the algorithm's performance on large graphs.

The code has been tested in scala version 2.12 and 2.13 with Apache Spark versions 3.2x, 3.3x and 3.4

This repository will implement the following graph algorithms:
1. Delta-Stepping Single-Source Shortest Path
1. Dijkstra Source-Target Shortest Path
1. Dijkstra Single-Source Shortest Path
1. A* Shortest Path
1. Yen’s Shortest Path
1. Breadth First Search
1. Depth First Search
1. Page Rank
1. Article Rank
1. Eigenvector Centrality
1. Betweenness Centrality
1. Degree Centrality
1. Closeness Centrality
1. CELF
1. Harmonic Centrality
1. HITS
1. Random Walk With Restarts Sampling
1. Random Walk
1. Louvain Community detection
1. Label Propagation
1. Weakly Connected Components
1. Triangle Count
1. Local Clustering Coefficient
1. K-1 Coloring
1. Modularity Optimization
1. K-Means Clustering
1. Leiden
1. Strongly Connected Components
1. Speaker-Listener Label Propagation
1. Approximate Maximum k-cut
1. Conductance metric
1. Modularity metric
1. Node Similarity
1. Filtered Node Similarity
1. K-Nearest Neighbors
1. Filtered K-Nearest Neighbors
1. Minimum Weight Spanning Tree
1. Minimum Directed Steiner Tree
1. Minimum Weight k-Spanning Tree
1. All Pairs Shortest Path
1. FastRP Node embedding
1. GraphSAGE Node embedding
1. Node2Vec Node embedding
1. HashGNN Node embedding
1. Adamic Adar link prediction
1. Common Neighbors link prediction
1. Preferential Attachment link prediction
1. Resource Allocation link prediction
1. Same Community link prediction
1. Total Neighbors link prediction
