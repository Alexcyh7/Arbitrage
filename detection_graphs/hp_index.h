#ifndef HP_INDEX_H
#define HP_INDEX_H

#include "directed_graph.h"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <limits>
#include <cstdint>

// A path between two hot points stored in the index
struct IndexPath {
    double weight;                              // sum of edge weights along the path
    uint32_t length;                            // number of edges
    std::vector<uint32_t> vertices;             // full vertex sequence (including endpoints)
    std::vector<std::pair<uint32_t, uint32_t>> edges; // edge list for invalidation
};

// A partial path found during forward/backward DFS
struct PartialPath {
    double weight;
    std::vector<uint32_t> vertices;  // vertex sequence
};

class HotPointIndex {
public:
    HotPointIndex(DirectedGraph& graph, uint32_t k, uint32_t threshold);

    // Build the initial index from the current graph state
    void build();

    // Full scan: find the most negative cycle of length <= k in the entire graph
    // Returns (best_weight, best_cycle). best_cycle includes start repeated at end.
    void find_best_negative_cycle();

    // Dynamic update: update edge weight and search for new negative cycles
    void update_edge(uint32_t src, uint32_t dst, double new_weight);

    // Current best state (public for main.cpp access)
    double current_best_weight_ = std::numeric_limits<double>::infinity();
    std::vector<uint32_t> current_best_cycle_;

private:
    DirectedGraph& graph_;
    uint32_t k_;           // max cycle length
    uint32_t threshold_;   // degree threshold for hot points

    // Hot point set
    std::unordered_set<uint32_t> hot_points_;

    // Index: precomputed paths between hot point pairs
    // index_paths_[hi][hj] = vector of IndexPath from hi to hj
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, std::vector<IndexPath>>> index_paths_;

    // Identify hot points based on degree threshold
    void identify_hot_points();

    // Build index paths between all hot point pairs via bounded DFS
    void build_index_paths();

    // DFS from a hot point to find paths to other hot points (length <= k, no intermediate hot points)
    void hp_dfs(uint32_t start, uint32_t current, double weight,
                std::vector<uint32_t>& path, std::unordered_set<uint32_t>& visited);

    // Step 1: Forward DFS from source vertex, collecting partial paths and hot points reached
    // partial_to_dest[vertex] = list of PartialPaths reaching that vertex
    // hot_points_reached[hp] = list of PartialPaths reaching that hot point
    void forward_dfs(uint32_t source, uint32_t dest,
                     uint32_t current, double weight,
                     std::vector<uint32_t>& path,
                     std::unordered_set<uint32_t>& visited,
                     std::vector<PartialPath>& direct_paths,
                     std::unordered_map<uint32_t, std::vector<PartialPath>>& fwd_hot_paths);

    // Step 2: Backward DFS from dest vertex on reverse graph
    void backward_dfs(uint32_t source, uint32_t dest,
                      uint32_t current, double weight,
                      std::vector<uint32_t>& path,
                      std::unordered_set<uint32_t>& visited,
                      std::vector<PartialPath>& direct_paths,
                      std::unordered_map<uint32_t, std::vector<PartialPath>>& bwd_hot_paths);

    // Step 3: Combine forward paths, index paths, and backward paths
    void combine_paths(uint32_t edge_src, uint32_t edge_dst, double edge_weight,
                       const std::unordered_map<uint32_t, std::vector<PartialPath>>& fwd_hot_paths,
                       const std::unordered_map<uint32_t, std::vector<PartialPath>>& bwd_hot_paths);

    // Check if a candidate cycle improves current best and update
    void try_update_best(double weight, const std::vector<uint32_t>& cycle);

    // Check if path is simple (no repeated vertices)
    bool is_simple(const std::vector<uint32_t>& vertices) const;

    // Update index paths as by-product of search
    void update_index_from_search(
        const std::unordered_map<uint32_t, std::vector<PartialPath>>& fwd_hot_paths,
        const std::unordered_map<uint32_t, std::vector<PartialPath>>& bwd_hot_paths);
};

#endif // HP_INDEX_H
