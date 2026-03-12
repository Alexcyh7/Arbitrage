#include "hp_index.h"
#include <algorithm>
#include <iostream>
#include <chrono>

HotPointIndex::HotPointIndex(DirectedGraph& graph, uint32_t k, uint32_t threshold)
    : graph_(graph), k_(k), threshold_(threshold) {}

void HotPointIndex::build() {
    identify_hot_points();
    build_index_paths();
}

void HotPointIndex::identify_hot_points() {
    hot_points_.clear();
    for (auto v : graph_.vertices()) {
        uint32_t deg = graph_.num_neighbors(v);
        if (deg >= threshold_) {
            hot_points_.insert(v);
        }
    }
    std::cout << "HP-Index: " << hot_points_.size() << " hot points (threshold=" << threshold_ << ")" << std::endl;
}

// DFS from a hot point start to find paths to other hot points
// Paths have length <= k and do not pass through intermediate hot points
void HotPointIndex::hp_dfs(uint32_t start, uint32_t current, double weight,
                           std::vector<uint32_t>& path, std::unordered_set<uint32_t>& visited) {
    if (path.size() > k_) return;

    // If we reached another hot point (not start), record this path
    if (current != start && hot_points_.count(current)) {
        IndexPath ip;
        ip.weight = weight;
        ip.length = static_cast<uint32_t>(path.size() - 1);
        ip.vertices = path;
        for (size_t i = 0; i + 1 < path.size(); i++) {
            ip.edges.emplace_back(path[i], path[i + 1]);
        }
        index_paths_[start][current].push_back(ip);
        // Don't continue past hot points
        return;
    }

    if (path.size() >= k_) return;  // can't extend further

    auto [neighbors, weights, deg] = graph_.get_out_neighbors(current);
    for (uint32_t i = 0; i < deg; i++) {
        uint32_t nxt = neighbors[i];
        double w = weights[i];
        if (visited.count(nxt)) continue;

        visited.insert(nxt);
        path.push_back(nxt);
        hp_dfs(start, nxt, weight + w, path, visited);
        path.pop_back();
        visited.erase(nxt);
    }
}

void HotPointIndex::build_index_paths() {
    index_paths_.clear();
    for (uint32_t hp : hot_points_) {
        std::vector<uint32_t> path = {hp};
        std::unordered_set<uint32_t> visited = {hp};
        hp_dfs(hp, hp, 0.0, path, visited);
    }

    // Count total index paths
    size_t total = 0;
    for (auto& [src, dst_map] : index_paths_) {
        for (auto& [dst, paths] : dst_map) {
            total += paths.size();
        }
    }
    std::cout << "HP-Index: " << total << " indexed paths" << std::endl;
}

// Helper: full scan DFS to find negative cycles from start (free function)
static void full_scan_dfs_impl(const DirectedGraph& graph, uint32_t k,
                        uint32_t start, uint32_t current, double weight,
                        std::vector<uint32_t>& path, std::unordered_set<uint32_t>& visited,
                        double& best_weight, std::vector<uint32_t>& best_cycle) {
    uint32_t depth = static_cast<uint32_t>(path.size()) - 1;

    // Check if we can close the cycle back to start
    if (depth >= 2 && depth <= k) {
        auto [neighbors, weights, deg] = graph.get_out_neighbors(current);
        for (uint32_t i = 0; i < deg; i++) {
            if (neighbors[i] == start) {
                double cycle_weight = weight + weights[i];
                if (cycle_weight < best_weight) {
                    best_weight = cycle_weight;
                    best_cycle = path;
                    best_cycle.push_back(start);
                }
            }
        }
    }

    if (depth >= k) return;

    auto [neighbors, weights, deg] = graph.get_out_neighbors(current);
    for (uint32_t i = 0; i < deg; i++) {
        uint32_t nxt = neighbors[i];
        double w = weights[i];

        // Only visit vertices with ID >= start to avoid counting same cycle multiple times
        if (nxt < start) continue;
        if (nxt == start) continue;  // closing handled above
        if (visited.count(nxt)) continue;

        visited.insert(nxt);
        path.push_back(nxt);
        full_scan_dfs_impl(graph, k, start, nxt, weight + w, path, visited, best_weight, best_cycle);
        path.pop_back();
        visited.erase(nxt);
    }
}

void HotPointIndex::find_best_negative_cycle() {
    current_best_weight_ = std::numeric_limits<double>::infinity();
    current_best_cycle_.clear();

    for (auto start : graph_.vertices()) {
        std::vector<uint32_t> path = {start};
        std::unordered_set<uint32_t> visited = {start};
        full_scan_dfs_impl(graph_, k_, start, start, 0.0, path, visited,
                           current_best_weight_, current_best_cycle_);
    }
}

// Forward DFS from source (Step 1 of GraphS)
// Searches forward from source in the graph, stops at hot points or depth k
void HotPointIndex::forward_dfs(uint32_t source, uint32_t dest,
                                uint32_t current, double weight,
                                std::vector<uint32_t>& path,
                                std::unordered_set<uint32_t>& visited,
                                std::vector<PartialPath>& direct_paths,
                                std::unordered_map<uint32_t, std::vector<PartialPath>>& fwd_hot_paths) {
    uint32_t depth = static_cast<uint32_t>(path.size()) - 1;

    // If we reached dest directly, record it
    if (current == dest && depth > 0) {
        PartialPath pp;
        pp.weight = weight;
        pp.vertices = path;
        direct_paths.push_back(pp);
        return;  // don't continue past dest
    }

    // If we hit a hot point (not source), record and stop this branch
    if (current != source && hot_points_.count(current)) {
        PartialPath pp;
        pp.weight = weight;
        pp.vertices = path;
        fwd_hot_paths[current].push_back(pp);
        return;  // stop at hot point
    }

    if (depth >= k_) return;  // length limit (need at least 1 more edge for the closing edge)

    auto [neighbors, weights, deg] = graph_.get_out_neighbors(current);
    for (uint32_t i = 0; i < deg; i++) {
        uint32_t nxt = neighbors[i];
        double w = weights[i];
        if (visited.count(nxt) && nxt != dest) continue;
        if (nxt == dest && depth + 1 < 2) continue;  // cycle must have length >= 2 (really >= 3 with the edge)

        bool was_visited = visited.count(nxt);
        if (!was_visited) visited.insert(nxt);
        path.push_back(nxt);
        forward_dfs(source, dest, nxt, weight + w, path, visited, direct_paths, fwd_hot_paths);
        path.pop_back();
        if (!was_visited) visited.erase(nxt);
    }
}

// Backward DFS from dest on reverse graph (Step 2 of GraphS)
void HotPointIndex::backward_dfs(uint32_t source, uint32_t dest,
                                 uint32_t current, double weight,
                                 std::vector<uint32_t>& path,
                                 std::unordered_set<uint32_t>& visited,
                                 std::vector<PartialPath>& direct_paths,
                                 std::unordered_map<uint32_t, std::vector<PartialPath>>& bwd_hot_paths) {
    uint32_t depth = static_cast<uint32_t>(path.size()) - 1;

    // If we reached source directly via reverse edges, record it
    if (current == source && depth > 0) {
        PartialPath pp;
        pp.weight = weight;
        pp.vertices = path;  // stored in reverse order (dest ... source)
        direct_paths.push_back(pp);
        return;
    }

    // If we hit a hot point (not dest), record and stop
    if (current != dest && hot_points_.count(current)) {
        PartialPath pp;
        pp.weight = weight;
        pp.vertices = path;
        bwd_hot_paths[current].push_back(pp);
        return;
    }

    if (depth >= k_) return;

    auto [neighbors, weights, deg] = graph_.get_in_neighbors(current);
    for (uint32_t i = 0; i < deg; i++) {
        uint32_t prev = neighbors[i];  // prev -> current in original graph
        double w = weights[i];
        if (visited.count(prev) && prev != source) continue;
        if (prev == source && depth + 1 < 2) continue;

        bool was_visited = visited.count(prev);
        if (!was_visited) visited.insert(prev);
        path.push_back(prev);
        backward_dfs(source, dest, prev, weight + w, path, visited, direct_paths, bwd_hot_paths);
        path.pop_back();
        if (!was_visited) visited.erase(prev);
    }
}

bool HotPointIndex::is_simple(const std::vector<uint32_t>& vertices) const {
    // Check no repeated vertices (except first==last for cycle)
    std::unordered_set<uint32_t> seen;
    for (size_t i = 0; i + 1 < vertices.size(); i++) {
        if (!seen.insert(vertices[i]).second) return false;
    }
    return true;
}

void HotPointIndex::try_update_best(double weight, const std::vector<uint32_t>& cycle) {
    if (weight < current_best_weight_ && is_simple(cycle)) {
        current_best_weight_ = weight;
        current_best_cycle_ = cycle;
    }
}

// Step 3: combine forward partial paths through index to backward partial paths
void HotPointIndex::combine_paths(
    uint32_t /*edge_src*/, uint32_t /*edge_dst*/, double edge_weight,
    const std::unordered_map<uint32_t, std::vector<PartialPath>>& fwd_hot_paths,
    const std::unordered_map<uint32_t, std::vector<PartialPath>>& bwd_hot_paths) {

    // Case 1: Forward meets backward at the same hot point (no index path needed)
    for (auto& [hp, fwd_list] : fwd_hot_paths) {
        auto bwd_it = bwd_hot_paths.find(hp);
        if (bwd_it == bwd_hot_paths.end()) continue;

        for (auto& fwd : fwd_list) {
            for (auto& bwd : bwd_it->second) {
                // Total cycle: edge(edge_src -> edge_dst) + fwd_path(edge_dst ... hp) + bwd_path_reversed(hp ... edge_src)
                double total_weight = edge_weight + fwd.weight + bwd.weight;
                uint32_t total_length = static_cast<uint32_t>(fwd.vertices.size() + bwd.vertices.size() - 1);
                // -1 because hp appears in both; +1 for the trigger edge => total edges = total_length
                if (total_length > k_ + 1) continue;  // cycle length = edges count

                // Build the cycle: edge_dst -> ... -> hp -> ... -> edge_src -> edge_dst
                std::vector<uint32_t> cycle;
                // fwd.vertices = [edge_dst, ..., hp]
                for (auto v : fwd.vertices) cycle.push_back(v);
                // bwd.vertices = [edge_src, ..., hp] — we need reversed minus hp
                for (int j = static_cast<int>(bwd.vertices.size()) - 2; j >= 0; j--) {
                    cycle.push_back(bwd.vertices[j]);
                }
                // Close the cycle
                cycle.push_back(cycle[0]);

                try_update_best(total_weight, cycle);
            }
        }
    }

    // Case 2: Forward reaches hi, backward reaches hj, index connects hi -> hj
    for (auto& [hi, fwd_list] : fwd_hot_paths) {
        auto idx_it = index_paths_.find(hi);
        if (idx_it == index_paths_.end()) continue;

        for (auto& [hj, idx_paths] : idx_it->second) {
            auto bwd_it = bwd_hot_paths.find(hj);
            if (bwd_it == bwd_hot_paths.end()) continue;

            for (auto& fwd : fwd_list) {
                for (auto& idx : idx_paths) {
                    for (auto& bwd : bwd_it->second) {
                        double total_weight = edge_weight + fwd.weight + idx.weight + bwd.weight;
                        uint32_t fwd_edges = static_cast<uint32_t>(fwd.vertices.size() - 1);
                        uint32_t bwd_edges = static_cast<uint32_t>(bwd.vertices.size() - 1);
                        uint32_t total_edges = 1 + fwd_edges + idx.length + bwd_edges;
                        if (total_edges > k_) continue;

                        // Build cycle: edge_dst -> ... -> hi -> [index] -> hj -> ... -> edge_src -> edge_dst
                        std::vector<uint32_t> cycle;
                        // fwd: [edge_dst, ..., hi]
                        for (auto v : fwd.vertices) cycle.push_back(v);
                        // index: [hi, ..., hj] — skip first (hi already added)
                        for (size_t i = 1; i < idx.vertices.size(); i++) {
                            cycle.push_back(idx.vertices[i]);
                        }
                        // bwd: [edge_src, ..., hj] — reversed, skip first (hj already added)
                        for (int j = static_cast<int>(bwd.vertices.size()) - 2; j >= 0; j--) {
                            cycle.push_back(bwd.vertices[j]);
                        }
                        cycle.push_back(cycle[0]);

                        try_update_best(total_weight, cycle);
                    }
                }
            }
        }
    }
}

void HotPointIndex::update_index_from_search(
    const std::unordered_map<uint32_t, std::vector<PartialPath>>& fwd_hot_paths,
    const std::unordered_map<uint32_t, std::vector<PartialPath>>& bwd_hot_paths) {

    // From forward search: if source is a hot point and we reached another hot point,
    // that's a new index path candidate. But the source of forward DFS is edge_dst,
    // which may or may not be a hot point. We handle this lazily — only update
    // when both endpoints are hot points.

    // For now we do a lightweight update: if any forward path starts from a hot point
    // to another hot point, add it to the index if it's better.
    // This is the "by-product" maintenance described in the paper.

    // Forward paths that start from a hot point endpoint
    for (auto& [hp_end, paths] : fwd_hot_paths) {
        for (auto& pp : paths) {
            if (pp.vertices.empty()) continue;
            uint32_t hp_start = pp.vertices[0];
            if (!hot_points_.count(hp_start)) continue;

            // Check if this is a new/better path from hp_start to hp_end
            bool already_exists = false;
            auto& existing = index_paths_[hp_start][hp_end];
            for (auto& ip : existing) {
                if (ip.vertices == pp.vertices) {
                    if (pp.weight < ip.weight) {
                        ip.weight = pp.weight;
                    }
                    already_exists = true;
                    break;
                }
            }
            if (!already_exists && pp.vertices.size() - 1 <= k_) {
                IndexPath ip;
                ip.weight = pp.weight;
                ip.length = static_cast<uint32_t>(pp.vertices.size() - 1);
                ip.vertices = pp.vertices;
                for (size_t i = 0; i + 1 < pp.vertices.size(); i++) {
                    ip.edges.emplace_back(pp.vertices[i], pp.vertices[i + 1]);
                }
                existing.push_back(ip);
            }
        }
    }

    // Similarly for backward paths (reversed)
    for (auto& [hp_end, paths] : bwd_hot_paths) {
        for (auto& pp : paths) {
            if (pp.vertices.empty()) continue;
            uint32_t hp_start = pp.vertices[0];
            if (!hot_points_.count(hp_start)) continue;

            // bwd path is in reverse order: [dest, ..., hp_end]
            // In forward graph this is hp_end -> ... -> dest
            // We want to store forward paths, so reverse
            std::vector<uint32_t> fwd_verts(pp.vertices.rbegin(), pp.vertices.rend());
            uint32_t from_hp = fwd_verts[0];  // hp_end
            uint32_t to_hp = fwd_verts.back();  // dest = hp_start

            if (!hot_points_.count(to_hp)) continue;

            auto& existing = index_paths_[from_hp][to_hp];
            bool already_exists = false;
            for (auto& ip : existing) {
                if (ip.vertices == fwd_verts) {
                    if (pp.weight < ip.weight) ip.weight = pp.weight;
                    already_exists = true;
                    break;
                }
            }
            if (!already_exists && fwd_verts.size() - 1 <= k_) {
                IndexPath ip;
                ip.weight = pp.weight;
                ip.length = static_cast<uint32_t>(fwd_verts.size() - 1);
                ip.vertices = fwd_verts;
                for (size_t i = 0; i + 1 < fwd_verts.size(); i++) {
                    ip.edges.emplace_back(fwd_verts[i], fwd_verts[i + 1]);
                }
                existing.push_back(ip);
            }
        }
    }
}

void HotPointIndex::update_edge(uint32_t src, uint32_t dst, double new_weight) {
    // Update the graph
    graph_.update_edge(src, dst, new_weight);

    // 3-step search for new cycles involving the updated edge (src -> dst)
    // The new edge is (src, dst). A cycle through this edge is:
    //   src -> dst -> ... -> src
    // So we search for paths from dst to src of length <= k-1

    // Step 1: Forward DFS from dst
    std::vector<PartialPath> fwd_direct;
    std::unordered_map<uint32_t, std::vector<PartialPath>> fwd_hot;
    {
        std::vector<uint32_t> path = {dst};
        std::unordered_set<uint32_t> visited = {dst, src};  // exclude src from intermediate
        // But src is the destination, so we allow it in forward_dfs as dest
        visited.erase(src);
        forward_dfs(src, src, dst, 0.0, path, visited, fwd_direct, fwd_hot);
    }

    // Step 2: Backward DFS from src (on reverse graph)
    std::vector<PartialPath> bwd_direct;
    std::unordered_map<uint32_t, std::vector<PartialPath>> bwd_hot;
    {
        std::vector<uint32_t> path = {src};
        std::unordered_set<uint32_t> visited = {src, dst};  // exclude dst from intermediate
        visited.erase(dst);
        backward_dfs(dst, src, src, 0.0, path, visited, bwd_direct, bwd_hot);
    }

    // Direct cycles from forward DFS (dst -> ... -> src, then src -> dst closes it)
    for (auto& pp : fwd_direct) {
        // pp.vertices = [dst, ..., src]
        uint32_t cycle_len = static_cast<uint32_t>(pp.vertices.size());  // edges in this path + 1 for closing
        if (cycle_len > k_) continue;

        double cycle_weight = new_weight + pp.weight;
        std::vector<uint32_t> cycle;
        // cycle: src -> dst -> ... -> src
        cycle.push_back(src);
        for (auto v : pp.vertices) cycle.push_back(v);
        // Last vertex should be src already
        if (cycle.back() != src) cycle.push_back(src);
        try_update_best(cycle_weight, cycle);
    }

    // Direct cycles from backward DFS (src <- ... <- dst in reverse graph = dst -> ... -> src in forward)
    for (auto& pp : bwd_direct) {
        // pp.vertices = [src, ..., dst] (backward order)
        uint32_t cycle_len = static_cast<uint32_t>(pp.vertices.size());
        if (cycle_len > k_) continue;

        double cycle_weight = new_weight + pp.weight;
        // Reverse to get forward order: dst -> ... -> src
        std::vector<uint32_t> cycle;
        cycle.push_back(src);
        cycle.push_back(dst);
        for (int j = static_cast<int>(pp.vertices.size()) - 2; j > 0; j--) {
            cycle.push_back(pp.vertices[j]);
        }
        cycle.push_back(src);
        try_update_best(cycle_weight, cycle);
    }

    // Step 3: Combine through hot points
    combine_paths(src, dst, new_weight, fwd_hot, bwd_hot);

    // Update index as by-product
    update_index_from_search(fwd_hot, bwd_hot);
}
