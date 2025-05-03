# Large Scale Data Processing: Final Project Report

**Authors:** Elliott Frink • Zachary Blest • Lukas Gearin  

---

## Algorithm‐Benchmark Results

| Algorithm          | File                         | Size of Matching | Time (wall‑clock) | Configuration                         |
|--------------------|------------------------------|------------------|-------------------|---------------------------------------|
| Edmonds Blossom    | `log_normal_100.csv`         | 50               | 0.2 s             | Local – Apple M1 Max, 40 GB RAM       |
| Edmonds Blossom    | `musae_ENGB_edges.csv`       | 2 968            | 0.5 s             | Local – Apple M1 Max, 40 GB RAM       |
| **FMU**            | `soc‑pokec‑relationships.csv`| 602 907          | 1 348 s (≈ 22 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |
| **FMU**            | `soc‑LiveJournal1.csv`       | 1 701 377        | 1 818 s (≈ 30 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |
| **FMU**            | `twitter_original_edges.csv` | 94 018           | 1 920 s (≈ 32 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |
| **FMU**            | `com‑orkut.ungraph.csv`      | 1 390 647        | 2 360 s (≈ 39 min)| 1 × Master + 4 × Workers (e2‑std‑4)   |

---

## Approaches

### Edmonds‑Blossom Algorithm
The Edmonds–Blossom algorithm takes a general graph \(G=(V,E)\) and incrementally builds a matching \(M\) by searching for **augmenting paths**—alternating paths that start and end at unmatched vertices. Each iteration either  
1. finds no augmenting path, proving \(M\) is maximum;  
2. discovers an augmenting path and augments \(M\); or  
3. detects a **blossom** (odd cycle), contracts it, and continues the search.  

Correctness follows from **Edmonds’ Matching Theorem**: if no augmenting path exists, the current matching is maximum. The algorithm’s \(O(n^{4})\) time complexity renders it impractical for massive graphs, so we restricted it to the two smallest data sets.

### Fischer‑Mitrović‑Uitto (FMU) Algorithm
FMU is a **deterministic, semi‑streaming \((1+\varepsilon)\)-approximation** for maximum matching that needs only \(\operatorname{poly}(1/\varepsilon)\) streamed passes. Earlier results relied on randomness with exponential‑in‑\(1/\varepsilon\) running times; FMU improves this to \(O\!\bigl(\log\log n\;\operatorname{poly}(1/\varepsilon)\bigr)\) in the semi‑streaming model and achieves similarly near‑optimal bounds in Linear‑Memory MPC, Sublinear‑Memory MPC, and CONGEST models.

The algorithm:

1. **Pass 0 – Greedy Maximal Matching:** obtains a 2‑approximate matching \(M\).  
2. **Phases \(1,\dotsc,\operatorname{poly}(1/\varepsilon)\):** each phase performs parallel DFS from every unmatched vertex, searching for *short* augmenting paths. When a path is found, its DFS tree is removed to prevent interaction between concurrent searches. Each successful phase enlarges \(M\) by a multiplicative factor \(\bigl(1+1/\operatorname{poly}(1/\varepsilon)\bigr)\).  
3. After all phases, \(|M|\) is within \((1+\varepsilon)\) of optimal.

---

## Implementation & Trade‑Offs

Our Spark GraphX implementation diverges slightly from the theoretical FMU specification:

* **Pass‑Bundle Limit.** The original algorithm dedicates \(1/\varepsilon^{6}\) PASS‑BUNDLES per phase. We fix this to 10 to avoid infinite loops and rule violations (e.g., vertices with multiple incident path edges).  
* **Path‑Length Cap.** We restrict augmenting paths to length ≤ 3, keeping searches local. Collisions between paths in different partitions become vanishingly rare, so expensive collision‑recovery wipes are almost never triggered.  
* **Scalability.** We set the number of Spark partitions to \(2\times\texttt{defaultParallelism}\). As executors are added, Spark spawns more tasks automatically and distributes vertices and edges evenly. The **EdgePartition2D** strategy co‑locates high‑degree vertices, slashing cross‑node traffic. Persisting and checkpointing the graph lets Spark overlap network I/O with computation, mitigating shuffle bottlenecks. In practice, doubling machines yields almost proportional throughput.

These choices guarantee a **valid maximal matching (≤ 2‑approx)** in ≤ 9 passes; empirical results beat this bound and scale smoothly with graph size, though the formal \((1+\varepsilon)\) guarantee no longer strictly holds.

---

## Sources

- Fischer, M., Mitrović, S., & Uitto, J. *Deterministic \((1+\varepsilon)\)-Approximate Maximum Matching with \(\operatorname{poly}(1/\varepsilon)\) Passes in the Semi‑Streaming Model and Beyond.* **STOC 2022**, pp. 1–13. https://doi.org/10.1145/3519935.3520039  
- Edmonds, J. *Paths, Trees and Flowers.* **Canadian J. Math.** 17 (1965): 449‑467. https://doi.org/10.4153/CJM-1965-045-4
