# Large Scale Data Processing: Final Project Report

**Authors:** Elliott Frink • Zachary Blest • Lukas Gearin  

---

## Algorithm‐Benchmark Results

| Algorithm          | File                         | Size of Matching | Time (wall‑clock) | Configuration                         |
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
The Edmonds–Blossom algorithm takes a general graph G=(V,E) and incrementally builds a matching M by searching for **augmenting paths**—alternating paths that start and end at unmatched vertices. Each iteration either:  
1. finds no augmenting path, proving M is maximum;  
2. discovers an augmenting path and augments M; or  
3. detects a **blossom** (odd cycle), contracts it, and continues the search.  

Correctness follows from **Edmonds' Matching Theorem**: if no augmenting path exists, the current matching is maximum. The algorithm's O(n^4) time complexity renders it impractical for massive graphs, so we restricted it to the two smallest data sets.

### Fischer‑Mitrović‑Uitto (FMU) Algorithm
FMU is a **deterministic, semi‑streaming (1+ε)-approximation** for maximum matching that needs only poly(1/ε) streamed passes. Earlier results relied on randomness with exponential‑in‑(1/ε) running times; FMU improves this to O(log log n · poly(1/ε)) in the semi‑streaming model and achieves similarly near‑optimal bounds in Linear‑Memory MPC, Sublinear‑Memory MPC, and CONGEST models.

The algorithm:

1. **Pass 0 – Greedy Maximal Matching:** obtains a 2‑approximate matching M.  
2. **Phases 1,...,poly(1/ε):** each phase performs parallel DFS from every unmatched vertex, searching for *short* augmenting paths. When a path is found, its DFS tree is removed to prevent interaction between concurrent searches. Each successful phase enlarges M by a multiplicative factor (1+1/poly(1/ε)).  
3. After all phases, |M| is within (1+ε) of optimal.

---

## Implementation & Trade‑Offs

When implementing this algorithm, we ran into a few issues, so we had to make a few adjustments. Within the original algorithm, each phase uses 1/ε^6 PASS-BUNDLES, but when we did this, our algorithm would never end. Many would not be safe, meaning they would break the rules of the algorithm (vertices would have multiple edges connected to them). To combat this, we hard-coded the number of PASS-BUNDLES in each phase to be 10. We also limit the length of augmenting paths to be less than or equal to 3. These changes may have impacted the effectiveness of our implementation, but they kept the resulting matching consistent and avoided infinite loops. Another benefit of capping the augmenting paths at three is that each path is tightly local, so it's very unlikely that the same path is chosen independently in two different partitions. Since this is the case, the algorithm almost never had to wipe the vertices' path data for collision recovery, leading to higher efficiency as well as still keeping the matching consistent through the phases.

When running this algorithm, we did not have any guarantee how it was going to perform because of the changes that we made; some of the lemmas about the original algorithm were no longer true. What we do know is that the implementation guarantees a valid maximal matching (≤ 2‑approx) in at most nine streamed passes; additional phases typically improve the ratio, but our implementation of the algorithm no longer carries the (1 + ε) worst‑case guarantee of Fischer‑Mitrović‑Uitto. Our results in practice showed much better performance than a simple 2-approx. Our time also scaled quite nicely with respect to the size of the graphs.

The algorithm scales automatically with additional machines by setting Spark partitions to twice the default parallelism. This enables an even distribution of tasks, vertices, and edges across the cluster. Using EdgePartition2D reduces cross-node communication by localizing high-degree vertices. Persisting and checkpointing the graph between phases allows Spark to overlap network I/O with computation, minimizing shuffle bottlenecks. These design choices yield near-linear scalability.

---

## Sources

- Fischer, M., Mitrović, S., & Uitto, J. *Deterministic (1+ε)-Approximate Maximum Matching with poly(1/ε) Passes in the Semi‑Streaming Model and Beyond.* **STOC 2022**, pp. 1–13. https://doi.org/10.1145/3519935.3520039  
- Edmonds, J. *Paths, Trees and Flowers.* **Canadian J. Math.** 17 (1965): 449‑467. https://doi.org/10.4153/CJM-1965-045-4
