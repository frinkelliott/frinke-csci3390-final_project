# Large Scale Data Processing: Final Project Report

**Authors:** Elliott Frink • Zachary Blest • Lukas Gearin  

---

## Algorithm‐Benchmark Results

| Algorithm          | File                         | Size of Matching | Time | Configuration                         |
|--------------------|------------------------------|------------------|-------------------|---------------------------------------|
| Edmonds Blossom    | `log_normal_100.csv`         | 50               | 0.2 s             | Local – Apple M1 Max, 40 GB RAM       |
| Edmonds Blossom    | `musae_ENGB_edges.csv`       | 2,968            | 0.5 s             | Local – Apple M1 Max, 40 GB RAM       |
| **FMU**            | `soc‑pokec‑relationships.csv`| 602,907          | 1,348 s (≈ 22 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |
| **FMU**            | `soc‑LiveJournal1.csv`       | 1,701,377        | 1,818 s (≈ 30 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |
| **FMU**            | `twitter_original_edges.csv` | 94,018           | 1,920 s (≈ 32 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |
| **FMU**            | `com‑orkut.ungraph.csv`      | 1,390,647        | 2,360 s (≈ 39 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |

---

## Approaches

### Edmonds‑Blossom Algorithm
The algorithm takes a general graph G = (V, E) and finds a maximum matching M. The algorithm starts with an empty matching and then iteratively improves it by adding edges, one at a time, to build augmenting paths in the matching M. Adding to an augmenting path can grow a matching since every other edge in an augmenting path is an edge in the matching; as more edges are added to the augmenting path, more matching edges are discovered. The blossom algorithm has three possible results after each iteration. Either the algorithm finds no augmenting paths, in which case it has found a maximum matching; an augmenting path can be found in order to improve the outcome; or a blossom can be found in order to manipulate it and discover a new augmenting path. The algorithm guarantees finding a maximum cardinality matching using an augmenting path approach, which generalizes the ideas from bipartite matching like the Hungarian algorithm. Its correctness is backed by Edmonds' Matching Theorem that ensures if no augmenting path exists, the current matching is the maximum. The algorithm has a time complexity of O(n^4), which is impractical for large scale graphs. This is what led us to only use this approach for the first two csv files, as they are of a suitable size.

### Fischer‑Mitrović‑Uitto (FMU) Algorithm
This algorithm is a deterministic, semi-streaming (1+ε)-approximation algorithm for maximum cardinality matching. It requires only poly(1/ε) passes over the input, which is a notable improvement over previous algorithms that relied on randomized techniques. This is significant because the previous algorithms that relied on randomness had exponential time. The researchers not only ran this on a semi-streaming model they also ran it on different models, such as Linear-Memory MPC, Sublinear-Memory MPC, and CONGEST models. The run times for each were O(log log n · poly(1/ε)), Õ(√log n · poly(1/ε)), and O(log n · poly(1/ε)), respectively. These improvements are significant because prior work for each of these models required exponential-in-1/ε time complexities. By contrast, this algorithm achieves near-optimal communication rounds while preserving a strong approximation guarantee. The size of the matching it computes is at most a factor (1+ε) larger than the optimal matching, which is a near-optimal guarantee. 

This algorithm works by taking in a graph G and an approximate parameter ε and then applies a greedy maximal matching algorithm on the first pass, which is known as a 2-approximation. This step creates M, where no two matched edges share a vertex. The algorithm then iterates through phases 1,2,..., poly(1/ε), where each phase searches for short augmenting paths by using a parallel depth-first search from unmatched (free) nodes. When an augmenting path is found, the corresponding DFS tree is removed from the graph to maintain independence across searches. Each phase increases the size of the matching by a (1+1/poly(1/ε)) factor. After all phases, the resulting matching is guaranteed to be within a (1+ε) factor of the optimal, offering a very good approximation while keeping resource usage low, which leads to greater scalability.

---

## Implementation & Trade‑Offs

When implementing this algorithm, we ran into a few issues, so we had to make a few adjustments. Within the original algorithm, each phase uses 1/ε^6 PASS-BUNDLES, but when we did this, our algorithm would never end. Many would not be safe, meaning they would break the rules of the algorithm (vertices would have multiple edges connected to them). To combat this, we hard-coded the number of PASS-BUNDLES in each phase to be 10. We also limit the length of augmenting paths to be less than or equal to 3. These changes may have impacted the effectiveness of our implementation, but they kept the resulting matching consistent and avoided infinite loops. Another benefit of capping the augmenting paths at three is that each path is tightly local, so it's very unlikely that the same path is chosen independently in two different partitions. Since this is the case, the algorithm almost never had to wipe the vertices' path data for collision recovery, leading to higher efficiency as well as still keeping the matching consistent through the phases.

When running this algorithm, we did not have any guarantee how it was going to perform because of the changes that we made; some of the lemmas about the original algorithm were no longer true. What we do know is that the implementation guarantees a valid maximal matching (≤ 2-approx) in at most nine streamed passes; additional phases typically improve the ratio, but our implementation of the algorithm no longer carries the (1+ε) worst-case guarantee of Fischer‑Mitrović‑Uitto. Our results in practice showed much better performance than a simple 2-approx. Our time also scaled quite nicely with respect to the size of the graphs.

The algorithm scales automatically with additional machines by setting Spark partitions to twice the default parallelism. This enables an even distribution of tasks, vertices, and edges across the cluster. Using EdgePartition2D reduces cross-node communication by localizing high-degree vertices. Persisting and checkpointing the graph between phases allows Spark to overlap network I/O with computation, minimizing shuffle bottlenecks. These design choices yield near-linear scalability.

---

## Sources

- Fischer, M., Mitrović, S., & Uitto, J. *Deterministic (1+ε)-Approximate Maximum Matching with poly(1/ε) Passes in the Semi‑Streaming Model and Beyond.* **STOC 2022**, pp. 1–13. https://doi.org/10.1145/3519935.3520039  
- Edmonds, J. *Paths, Trees and Flowers.* **Canadian J. Math.** 17 (1965): 449‑467. https://doi.org/10.4153/CJM-1965-045-4
