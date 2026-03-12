#include "cycle_detector.h"
#include <chrono>
#include <queue>
#include <sstream>
#include <cassert>
#include <algorithm>
#include <iomanip>
#include <vector>

using CycleInfo = std::pair<double, std::vector<uint32_t>>;
using CycleInfoCmp = std::function<bool(const CycleInfo&, const CycleInfo&)>;
// assign colors to each node
void KCycleColorCoding::assign_colors() {
    node_colors.clear();
    for(auto v: graph.vertices()) {
        node_colors[v] = dist(gen_);
    }
}

// print the top n most negative cycles
template<typename QueueType>
void print_top_cycles(const QueueType& top_cycles, uint k) {
    if (!top_cycles.empty()) {
        std::cout << "  Found " << top_cycles.size() << " candidate cycle(s):\n";
        auto copy = top_cycles;
        for (uint32_t i = 0; i < top_cycles.size(); ++i) {
            auto& [weight, cycle] = copy.top();
            std::cout << "    Cycle " << i + 1 << ": ";
            for (size_t j = 0; j < cycle.size(); ++j) {
                std::cout << cycle[j];
                if (j < cycle.size() - 1)
                    std::cout << " -> ";
            }
            std::cout << " (Weight: " << weight << ")" << "\n";
            copy.pop();
        }
    } else {
        std::cout << "  No valid " << k << "-cycle found.\n";
    }
}

void KCycleColorCoding::backword_dfs(
    uint32_t dst_node,
    uint32_t current_node,
    double current_weight,
    std::vector<uint32_t>& back_path,
    int color_set,
    double& local_best_weight,
    std::vector<uint32_t>& local_best_cycle,
    std::vector<uint32_t>& backword_visited,
    bool force_compute 
) {
    uint depth = back_path.size();
    if(depth == k_) {
        return;
    }

    auto [neighbors, weights, deg] = graph.get_in_neighbors(current_node);
    for(uint32_t i = 0; i < deg; i++) {
        uint32_t pre= neighbors[i];
        // std::cout << "i: " << i << " deg: " << deg << " pre: " << pre << std::endl;
        double w = weights[i];

        int pre_color = 1 << node_colors.at(pre);
        // std::cout << "pre_color: " << pre_color << std::endl;
        if(color_set & pre_color) {
            continue;
        }

        if(pre < dst_node) {
            // std::cout << "pre < dst_node" << std::endl;
            double total_weight = current_weight + w;
            auto it = full_dp_table[pre][dst_node].find(color_set);
            bool changed = false;
            if(std::find(backword_visited.begin(), backword_visited.end(), pre) == backword_visited.end()) {
                full_dp_table[pre][dst_node][color_set] = total_weight;
                backword_visited.push_back(pre);
                changed = true;
            }
            else if(it == full_dp_table[pre][dst_node].end()) {
                full_dp_table[pre][dst_node][color_set] = total_weight;
                changed = true;
            } else {
                if(it->second > total_weight) {
                    it->second = total_weight;
                    changed = true;
                }
            }
            // std::cout << "changed: " << changed << std::endl;

            if(changed || force_compute) {
                std::vector<uint32_t> path;
                path.push_back(pre);
                for(int j = back_path.size() - 1; j >= 0; j--) {
                    path.push_back(back_path[j]);
                }
                dp_search_k_cycle(pre, dst_node, total_weight, path, color_set, local_best_weight, local_best_cycle);

                back_path.push_back(pre);
                backword_dfs(dst_node, pre, total_weight, back_path, color_set, local_best_weight, local_best_cycle, backword_visited, false);
                back_path.pop_back();
                color_set &= ~pre_color;
            }
        }
    }
    
}

void KCycleColorCoding::get_all_back_dfs_nodes(
    uint32_t dst_node,
    std::vector<uint32_t>& back_dfs_node
) {
    std::unordered_set<uint32_t> visited;
    std::vector<uint32_t> stack;
    std::vector<uint32_t> path;
    path.push_back(dst_node);
    stack.push_back(dst_node);

    while (!stack.empty()) {
        uint32_t current_node = stack.back();
        stack.pop_back();

        if (visited.find(current_node) != visited.end()) {
            continue;
        }

        visited.insert(current_node);
        path.push_back(current_node);

        if (path.size() <= k_) {
            back_dfs_node.push_back(current_node);
        }

        if (path.size() < k_) {
            auto [neighbors, weights, deg] = graph.get_in_neighbors(current_node);
            for (uint32_t i = 0; i < deg; i++) {
                uint32_t neighbor = neighbors[i];
                if (visited.find(neighbor) == visited.end()) {
                    stack.push_back(neighbor);
                }
            }
        }

        if (path.size() > 1) {
            path.pop_back();
        }
    }
}

void KCycleColorCoding::update_edge_weight(
    uint32_t src_node, 
    uint32_t dst_node, 
    double weight,
    double& local_best_weight,
    std::vector<uint32_t>& local_best_cycle
) {
    // bool force_recompute = true;
    bool force_recompute = false;
    double old_weight = 0;
    if(graph.get_edge_weight(src_node, dst_node, old_weight)) {
        if(weight < old_weight) {
            force_recompute = false;
        }
    }

    graph.update_edge(src_node, dst_node, weight);
    if(node_colors.find(src_node) == node_colors.end()) {
        node_colors[src_node] = dist(gen_);
        force_recompute = false;
    }
    if(node_colors.find(dst_node) == node_colors.end()) {
        node_colors[dst_node] = dist(gen_);
        force_recompute = false;
    }

    if(node_colors[src_node] == node_colors[dst_node]) {
        return;
    }

    if(force_recompute) {
        std::vector<uint32_t> back_dfs_node;
        get_all_back_dfs_nodes(dst_node, back_dfs_node);
        std::cout << "back_dfs_node size: " << back_dfs_node.size() << std::endl;
        std::sort(back_dfs_node.begin(), back_dfs_node.end());
        for(auto node: back_dfs_node) {
            std::cout << "node: " << node << std::endl;
            dp_from_a_node(node, local_best_weight, local_best_cycle);
        }
        assert(false);
        return;
    }

    int color_set = 1 << node_colors[src_node];
    color_set |= 1 << node_colors[dst_node];

    bool changed = false;
    if(full_dp_table[src_node][dst_node].find(color_set) == full_dp_table[src_node][dst_node].end()) {
        full_dp_table[src_node][dst_node][color_set] = weight;
        changed = true;
    } else {
        if(full_dp_table[src_node][dst_node][color_set] > weight) {
            full_dp_table[src_node][dst_node][color_set] = weight;
            changed = true;
        }
    }

    if(changed) {
        std::vector<uint32_t> path;
        path.push_back(src_node);
        path.push_back(dst_node);
        dp_search_k_cycle(src_node, dst_node, weight, path, color_set, local_best_weight, local_best_cycle);

        std::vector<uint32_t> back_path;
        back_path.push_back(dst_node);
        back_path.push_back(src_node);
        std::vector<uint32_t> backword_visited;
        backword_dfs(dst_node, src_node, weight, back_path, color_set, local_best_weight, local_best_cycle, backword_visited);
    }

}


void KCycleColorCoding::dp_search_k_cycle(
    uint32_t start,
    uint32_t current,
    double current_weight,
    std::vector<uint32_t>& path,
    int color_set,
    double& local_best_weight,
    std::vector<uint32_t>& local_best_cycle,
    bool enable_back_dp 
) {    
    uint depth = path.size();
    auto & dp_table = full_dp_table[start];
    // if the depth is k, check if current path can form a cycle
    if (depth == k_) {
        auto [neighbors, weights, deg] = graph.get_out_neighbors(current);
        for(uint32_t i = 0; i < deg; i++) {
            // if found a cycle, update the local_best_weight and local_best_cycle
            if (neighbors[i] == start) {
                double cycle_weight = current_weight + weights[i];
                // std::cout << "color_set: " << color_set << std::endl;
                // std::cout << dp_table[current].size() << std::endl;
                // std::cout << full_dp_table[start][current][color_set] << std::endl;
                // std::cout << current_weight << std::endl;
                // assert(false);

                if (cycle_weight < local_best_weight) {
                        batch_weight_threshold_ = local_best_weight - cycle_weight;
                        local_best_weight = cycle_weight;
                        local_best_cycle = path;
                        local_best_cycle.push_back(start);
                }
                return;
            }
        }
    }

    auto [neighbors, weights, deg] = graph.get_out_neighbors(current);
    for (uint32_t i = 0; i < deg; ++i) {
        uint32_t nxt = neighbors[i];
        double w = weights[i];

        // if the next node is smaller than the start node, skip
        // because the start node is the smallest node in the cycle (to avoid duplicate cycles)
        // if(nxt < start || enable_back_dp){
        if(!enable_back_dp && nxt < start){
            continue;
        }

        int nxt_color = 1 << node_colors.at(nxt);
        // color conflict, skip
        if (color_set & nxt_color) {
            continue;
        }

        color_set |= nxt_color;
        path.push_back(nxt);
        auto it = dp_table[nxt].find(color_set);
        // only continue the dps is the dp table is updated
        if(it == dp_table[nxt].end()) {
            dp_table[nxt][color_set] = current_weight + w;

        } else {
            if(it->second > current_weight + w) {
                it->second = current_weight + w;
            }
            else {
                path.pop_back();
                color_set &= ~nxt_color;
                continue;
            }
        }

        // if(enable_back_dp) {
            // std::vector<uint32_t> back_path;
            // back_path.push_back(nxt);
            // for(int j = path.size() - 1; j >= 0; j--) {
                // back_path.push_back(path[j]);
            // }
            // std::vector<uint32_t> backword_visited;
            // backword_dfs(nxt, start, current_weight + w, back_path, color_set, local_best_weight, local_best_cycle, backword_visited);
        // }
        // dfs
        dp_search_k_cycle(start, nxt, current_weight + w, path, color_set, local_best_weight, local_best_cycle, enable_back_dp);


        path.pop_back();
        color_set &= ~nxt_color;
    }

}

void KCycleColorCoding::back_dp_from_a_node(
    uint32_t dst_node
) {
    // auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<uint32_t> back_dfs_node;
    get_all_back_dfs_nodes(dst_node, back_dfs_node);

    // indexing the result for regular dfs
    std::unordered_map<int, std::vector<std::pair<uint32_t, double>>> color_set_to_nodes;
    for(auto color_map: full_dp_table[dst_node]) {
        for(auto [color_set, weight]: color_map.second) {
            color_set_to_nodes[color_set].push_back(std::make_pair(color_map.first, weight));
        }
    }

    // auto pre_time = std::chrono::high_resolution_clock::now();
    // auto pre_time_duration = std::chrono::duration_cast<std::chrono::milliseconds>(pre_time - start_time).count();
    // std::cout << "back_dfs_node time: " << pre_time_duration << " ms" << std::endl;

    std::vector<int> possible_color_sets;
    int dst_node_color_bit = 1 << node_colors[dst_node];
    int full_color_set = (1 << k_) - 1;

    // Iterate through all possible subsets of the full color set
    for (int subset = 0; subset <= full_color_set; ++subset) {
        // Check if the subset contains the color bit for dst_node
        if (subset & dst_node_color_bit) {
            possible_color_sets.push_back(subset);
        }
    }

    for(auto src_node: back_dfs_node) {
        double local_best_weight = std::numeric_limits<double>::infinity();
        std::vector<uint32_t> local_best_cycle;
        dp_from_a_node(src_node, local_best_weight, local_best_cycle);
    }

    // concat front dfs and back dfs
    // for(auto src_node: back_dfs_node) {
    //     for(auto & [color_set, src_weight]: full_dp_table[src_node][dst_node]) {
    //         int index_color = color_set ^ dst_node_color_bit;
    //         for(auto possible_color_set: possible_color_sets) {
    //             if(index_color & possible_color_set) {
    //                 continue;
    //             }
    //             for(auto [node, weight]: color_set_to_nodes[possible_color_set]) {
    //                 if(node < src_node) {
    //                     continue;
    //                 }
    //                 double total_weight = weight + src_weight; 
    //                 int current_color = color_set | possible_color_set;
    //                 full_dp_table[src_node][node][current_color] = total_weight;
    //                 // if(full_dp_table[src_node][node].find(current_color) == full_dp_table[src_node][node].end()) {
    //                     // full_dp_table[src_node][node][current_color] = total_weight;
    //                 // } else {
    //                     // if(full_dp_table[src_node][node][current_color] > total_weight) {
    //                         // full_dp_table[src_node][node][current_color] = total_weight;
    //                     // }
    //                 // }
    //             }
    //         }
    //     }
    // }

    // auto end_time = std::chrono::high_resolution_clock::now();
    // auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    // std::cout << "back_dp_from_a_node time: " << total_duration << " ms" << std::endl;

    return;
}


// Run dfs from a start node
void KCycleColorCoding::dp_from_a_node(
    uint32_t start_node, 
    double& local_best_weight,
    std::vector<uint32_t>& local_best_cycle,
    bool enable_back_dp
) {
    full_dp_table[start_node] = std::unordered_map<uint32_t, std::unordered_map<int, double>>();

    int color_set = 1 << node_colors[start_node];
    std::vector<uint32_t> path;
    path.push_back(start_node);

    dp_search_k_cycle(
        start_node, start_node, 0.0,
        path, color_set,
        local_best_weight, local_best_cycle,
        enable_back_dp
    );
    return;
}

void KCycleColorCoding::process_dynamic_update_by_edge(
    const std::string& line,
    double& trial_best_weight,
    std::vector<uint32_t>& trial_best_cycle
) {
    std::istringstream iss(line);
    uint32_t src_node, dst_node;
    double weight;
    std::string timestamp; 
    iss >> src_node >> dst_node >> weight >> timestamp;
    if(weight > graph.weight_threshold_) {
        return;
    }
    auto classification_start_time = std::chrono::high_resolution_clock::now();
    bool rescan_dp_table = false;
    if (!trial_best_cycle.empty() && 
        std::find(trial_best_cycle.begin(), trial_best_cycle.end(), src_node) != trial_best_cycle.end() &&
        std::find(trial_best_cycle.begin(), trial_best_cycle.end(), dst_node) != trial_best_cycle.end()) {
        double old_weight = 0;
        graph.get_edge_weight(src_node, dst_node, old_weight);
        if(weight < old_weight) {
            rescan_dp_table = true;
        }
    }
    auto classification_end_time = std::chrono::high_resolution_clock::now();
    total_update_classification_us_ += std::chrono::duration_cast<std::chrono::microseconds>(classification_end_time - classification_start_time).count();
    
    update_edge_weight(src_node, dst_node, weight, trial_best_weight, trial_best_cycle);
        
    auto rescan_start_time = std::chrono::high_resolution_clock::now();
    if(rescan_dp_table) {
        trial_best_weight = std::numeric_limits<double>::infinity();
        // trial_best_weight = -10; 
        trial_best_cycle.clear();
        uint32_t best_node = 0;
        int full_color_set = (1 << k_) - 1;
        for(auto node: graph.vertices()) {
            auto [pre_nodes, weights, deg] = graph.get_in_neighbors(node);
            for(uint32_t i = 0; i < deg; i++) {
                uint32_t pre_node = pre_nodes[i];
                double int_weight = weights[i];
                if(full_dp_table[node].find(pre_node) == full_dp_table[node].end()) {
                    continue;
                }
                if(full_dp_table[node][pre_node].find(full_color_set) != full_dp_table[node][pre_node].end()) {
                    double path_weight = full_dp_table[node][pre_node][full_color_set];
                    if(path_weight + int_weight < trial_best_weight) {
                        trial_best_weight = path_weight + int_weight;
                        best_node = node;
                    }
                }                     
            }
        }
        trial_best_weight = std::numeric_limits<double>::infinity();
        trial_best_cycle.clear();
        dp_from_a_node(best_node, trial_best_weight, trial_best_cycle);
    }
    auto rescan_end_time = std::chrono::high_resolution_clock::now();
    if(rescan_dp_table) {
        total_rescan_edge_us_ += std::chrono::duration_cast<std::chrono::microseconds>(rescan_end_time - rescan_start_time).count();
    }
    return;
}

void KCycleColorCoding::process_dynamic_update_by_batch(
    double& trial_best_weight,
    std::vector<uint32_t>& trial_best_cycle,
    std::vector<std::string>& batch_lines,
    bool rescan_dp_table = false
) {
    // Record batch size
    uint32_t batch_size = batch_lines.size();
    batch_sizes_.push_back(batch_size);
    // std::cout << "batch_size: " << batch_size << std::endl;
    total_batches_++;

    auto dag_start_time = std::chrono::high_resolution_clock::now();
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, double>> back_updated_edges;
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, double>> updated_edges;

    uint32_t src_node, dst_node;
    double weight;
    std::string timestamp;  
    for(auto line: batch_lines) {
        std::istringstream iss(line);
        iss >> src_node >> dst_node >> weight >> timestamp;
        if(weight > graph.weight_threshold_) {
            continue;
        }
        back_updated_edges[dst_node][src_node] = weight;
        updated_edges[src_node][dst_node] = weight;
    }
    auto dag_end_time = std::chrono::high_resolution_clock::now();
    total_dag_build_us_ += std::chrono::duration_cast<std::chrono::microseconds>(dag_end_time - dag_start_time).count();

    // Classification: check if rescan is needed
    auto classification_start_time = std::chrono::high_resolution_clock::now();
    for(auto line: batch_lines) {
        std::istringstream iss(line);
        iss >> src_node >> dst_node >> weight >> timestamp;
        if(weight > graph.weight_threshold_) {
            continue;
        }
        if (!rescan_dp_table &&
            !trial_best_cycle.empty() && 
            std::find(trial_best_cycle.begin(), trial_best_cycle.end(), src_node) != trial_best_cycle.end() &&
            std::find(trial_best_cycle.begin(), trial_best_cycle.end(), dst_node) != trial_best_cycle.end()) {
            double old_weight = 0;
            graph.get_edge_weight(src_node, dst_node, old_weight);
            if(weight < old_weight) {
                rescan_dp_table = true;
            }
        }
    }
    auto classification_end_time = std::chrono::high_resolution_clock::now();
    total_update_classification_us_ += std::chrono::duration_cast<std::chrono::microseconds>(classification_end_time - classification_start_time).count();

    // for(auto [dst_node, src_node_map]: back_updated_edges) {
    //     // std::cout << "dst_node: " << dst_node << " src_node_map size: " << src_node_map.size() << std::endl;
    //     if(src_node_map.size() <= 1) {
    //         update_edge_weight(src_node_map.begin()->first, dst_node, src_node_map.begin()->second, trial_best_weight, trial_best_cycle);
    //     } else {
    //         for(auto [src_node, weight]: src_node_map) {
    //             graph.update_edge(src_node, dst_node, weight);
    //             if(node_colors.find(src_node) == node_colors.end()) {
    //                 node_colors[src_node] = dist(gen_);
    //             }
    //             if(node_colors.find(dst_node) == node_colors.end()) {
    //                 node_colors[dst_node] = dist(gen_);
    //             }
    //         }
    //         // std::cout << "src_node_map size: " << src_node_map.size() << std::endl;
    //         std::vector<uint32_t> back_path;
    //         back_path.push_back(dst_node);
    //         std::vector<uint32_t> backword_visited;
    //         backword_dfs(dst_node, dst_node, src_node_map.begin()->second, back_path, 1 << node_colors[dst_node], trial_best_weight, trial_best_cycle, backword_visited, true);
    //     }
    // }

    for(auto [src_node, dst_node_map]: updated_edges) {
    auto topo_start_time = std::chrono::high_resolution_clock::now();
        if(node_colors.find(src_node) == node_colors.end()) {
            node_colors[src_node] = dist(gen_);
            full_dp_table[src_node] = std::unordered_map<uint32_t, std::unordered_map<int, double>>();
        }
        if(dst_node_map.size() <= 10) {
            auto topo_end_time = std::chrono::high_resolution_clock::now();
            total_topo_sort_us_ += std::chrono::duration_cast<std::chrono::microseconds>(topo_end_time - topo_start_time).count();
            for(auto [dst_node, weight]: dst_node_map) {
                if(dst_node == 0){
                    int color_set = 1 << node_colors[src_node];
                    if(color_set & (1 << node_colors[dst_node])){
                        continue;
                    }
                    color_set |= (1 << node_colors[dst_node]);
                    full_dp_table[src_node][dst_node][color_set] = weight;
                }
                else{
                    update_edge_weight(src_node, dst_node, weight, trial_best_weight, trial_best_cycle);
                }
            }
        } else {
            // std::vector<std::pair<uint32_t, double>> sorted_dst_node_map(dst_node_map.begin(), dst_node_map.end());
            // std::sort(sorted_dst_nod_map.begin(), sorted_dst_node_map.end(), [](const auto& a, const auto& b) {
                // return a.second > b.second;
            // });
            // for(auto [dst_node, weight]: sorted_dst_node_map) {
    auto topo_end_time = std::chrono::high_resolution_clock::now();
    total_topo_sort_us_ += std::chrono::duration_cast<std::chrono::microseconds>(topo_end_time - topo_start_time).count();
            for(auto [dst_node, weight]: dst_node_map) {
                graph.update_edge(src_node, dst_node, weight);

                if(node_colors.find(dst_node) == node_colors.end()) {
                    node_colors[dst_node] = dist(gen_);
                    full_dp_table[dst_node] = std::unordered_map<uint32_t, std::unordered_map<int, double>>();
                }
            }
            dp_from_a_node(src_node, trial_best_weight, trial_best_cycle, true);
            // back_dp_from_a_node(src_node);
        }
    }

    auto rescan_start_time = std::chrono::high_resolution_clock::now();
    if(rescan_dp_table) {
        trial_best_weight = std::numeric_limits<double>::infinity();
        // trial_best_weight = -10; 
        trial_best_cycle.clear();
        uint32_t best_node = 0;
        int full_color_set = (1 << k_) - 1;
        for(auto node: graph.vertices()) {
            auto [pre_nodes, weights, deg] = graph.get_in_neighbors(node);
            for(uint32_t i = 0; i < deg; i++) {
                uint32_t pre_node = pre_nodes[i];
                double int_weight = weights[i];
                if(full_dp_table[node].find(pre_node) == full_dp_table[node].end()) {
                    continue;
                }
                if(full_dp_table[node][pre_node].find(full_color_set) != full_dp_table[node][pre_node].end()) {
                    double path_weight = full_dp_table[node][pre_node][full_color_set];
                    if(path_weight + int_weight < trial_best_weight) {
                        trial_best_weight = path_weight + int_weight;
                        best_node = node;
                    }
                }                     
            }
        }
        trial_best_weight = std::numeric_limits<double>::infinity();
        trial_best_cycle.clear();
        dp_from_a_node(best_node, trial_best_weight, trial_best_cycle, true);
    }
    auto rescan_end_time = std::chrono::high_resolution_clock::now();
    if(rescan_dp_table) {
        total_rescan_batch_us_ += std::chrono::duration_cast<std::chrono::microseconds>(rescan_end_time - rescan_start_time).count();
    }

}

// Return a vector of top n most negative cycles instead of 1 most negative cycle.
std::vector<std::pair<double, std::vector<uint32_t>>> KCycleColorCoding::find_most_negative_k_cycle(
    const std::vector<unsigned int>& trial_seeds,
    const std::string& dynamic_graph_file
) {
    auto cmp = [](const CycleInfo& a, const CycleInfo& b) {
        return a.first < b.first;  // Max heap (largest weight on top)
    };
    std::priority_queue<CycleInfo, std::vector<CycleInfo>, decltype(cmp)> top_cycles(cmp);
    DirectedGraph graph_copy = graph;

    // Run multiple trials with predetermined seeds
    for (uint trial = 0; trial < num_trials_; ++trial) {
        graph = graph_copy;
        graph.total_update_edge_us_ = 0;
        reset_timing_accumulators();
        unsigned int trial_seed = trial_seeds[trial];
        std::cout << "trial_seed: " << trial_seed << std::endl;
        gen_.seed(trial_seed);
        auto start_time = std::chrono::high_resolution_clock::now();

        // Assign colors randomly to each vertex 
        assign_colors();
        auto color_assign_time = std::chrono::high_resolution_clock::now();

        full_dp_table.clear();

        double trial_best_weight = std::numeric_limits<double>::infinity();
        std::vector<uint32_t> trial_best_cycle;

        for(auto start_node: graph.vertices()) {
            full_dp_table[start_node] = std::unordered_map<uint32_t, std::unordered_map<int, double>>();
            dp_from_a_node(start_node, trial_best_weight, trial_best_cycle);
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  end_time - start_time).count();
        auto color_assign_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         color_assign_time - start_time).count();

        if (top_cycles.size() < n_) {
            top_cycles.emplace(trial_best_weight, trial_best_cycle);
        } else if (trial_best_weight < top_cycles.top().first) {
            top_cycles.pop();
            top_cycles.emplace(trial_best_weight, trial_best_cycle);
        }

        std::cout << "Trial " << (trial + 1) << ":\n";
        // print_top_cycles(top_cycles, k_);
        std::cout << "  Color assignment time: " << color_assign_duration << " ms\n";
        std::cout << "  Total trial time: " << total_duration << " ms\n";
        std::cout << "  Actual trial time: " << total_duration - color_assign_duration << " ms\n";

        std::cout << "Current most negative cycle: ";
        if (!trial_best_cycle.empty()) {
            for (size_t i = 0; i < trial_best_cycle.size(); ++i) {
                std::cout << trial_best_cycle[i];
                if (i < trial_best_cycle.size() - 1)
                    std::cout << " -> ";
            }
            std::cout << " (Weight: " << trial_best_weight << ")\n";
        } else {
            std::cout << "None\n";
        }

        std::cout << "batch_weight_threshold_: " << batch_weight_threshold_ << std::endl;
        // batch_weight_threshold_ = -1;

        // uint tmp_count = 0;
        // for(auto node: graph.vertices()) {
        //     back_dp_from_a_node(node);
        //     tmp_count++;
        //     if(tmp_count % 1000 == 999) {
        //         std::cout << "tmp_count: " << tmp_count << std::endl;
        //         assert(false);
        //     }
        // }

        auto start_dynamic_time = std::chrono::high_resolution_clock::now();
        std::ifstream dynamic_file(dynamic_graph_file);
        std::string line;
        uint32_t num_updates = 0;
        uint batch_count = 0;
        std::vector<std::string> batch_lines;
        std::cout << "batch_size_: " << batch_size_ << std::endl;
        double cumulative_weight = 0;
        while (std::getline(dynamic_file, line)) {
            num_updates++;
            if(enable_auto_batch_) {
                batch_lines.push_back(line);
                std::istringstream iss(line);
                uint32_t src_node, dst_node;
                double weight;
                std::string timestamp;
                iss >> src_node >> dst_node >> weight >> timestamp;
                // the edge does not exist in the graph, direct update batch
                if(!graph.get_edge_weight(src_node, dst_node, weight)) {
                    // std::cout << "Number of updates in this batch: " << batch_lines.size() << std::endl;
                    process_dynamic_update_by_batch(trial_best_weight, trial_best_cycle, batch_lines);
                    batch_lines.clear();
                    cumulative_weight = 0;
                }
                else{
                    double old_weight = 0;
                    // if in best cycle
                    if(!trial_best_cycle.empty() && 
                        std::find(trial_best_cycle.begin(), trial_best_cycle.end(), src_node) != trial_best_cycle.end() &&
                        std::find(trial_best_cycle.begin(), trial_best_cycle.end(), dst_node) != trial_best_cycle.end()) {
                        graph.get_edge_weight(src_node, dst_node, old_weight);
                        // Immediately process batch when edge is in best cycle
                        // std::cout << "Number of updates in this batch: " << batch_lines.size() << std::endl;
                        process_dynamic_update_by_batch(trial_best_weight, trial_best_cycle, batch_lines);
                        batch_lines.clear();
                        cumulative_weight = 0;
                        continue;
                    }
                    else{
                        graph.get_edge_weight(src_node, dst_node, old_weight);
                        if(weight < old_weight) {
                            cumulative_weight += old_weight - weight;
                        }
                    }
                    // std::cout << "cumulative_weight: " << cumulative_weight << std::endl;
                    // std::cout << "batch_weight_threshold_: " << batch_weight_threshold_ << std::endl;
                    if(cumulative_weight > batch_weight_threshold_) {
                        // std::cout << "cumulative_weight: " << cumulative_weight << std::endl;
                        // std::cout << "Number of updates in this batch: " << batch_lines.size() << std::endl;
                        process_dynamic_update_by_batch(trial_best_weight, trial_best_cycle, batch_lines);
                        batch_lines.clear();
                        cumulative_weight = 0;
                    }
                }
            } 
            else {
                if(batch_size_ <= 1){
                    process_dynamic_update_by_edge(line, trial_best_weight, trial_best_cycle);
                } else {
                    batch_lines.push_back(line);
                    batch_count++;
                    if(batch_count == batch_size_) {
                        process_dynamic_update_by_batch(trial_best_weight, trial_best_cycle, batch_lines, true);
                        batch_lines.clear();
                        batch_count = 0;
                    }
                }
            }
        }
        if(batch_lines.size() > 0) {
            process_dynamic_update_by_batch(trial_best_weight, trial_best_cycle, batch_lines);
        }
        dynamic_file.close();
        auto end_dynamic_time = std::chrono::high_resolution_clock::now();
        auto dynamic_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_dynamic_time - start_dynamic_time).count();
        std::cout << "  Dynamic update time: " << dynamic_duration << " ms\n";
        std::cout << "  Dynamic update time per update: " << dynamic_duration / double(num_updates) << " ms\n";

        // Aggregated timing summary (printed once per trial)
        std::cout << "Timing totals (us): "
                  << "DAG_build=" << total_dag_build_us_
                  << ", topo_sort=" << total_topo_sort_us_
                  << ", rescan_batch=" << total_rescan_batch_us_
                  << ", rescan_edge=" << total_rescan_edge_us_
                  << ", update_classification=" << total_update_classification_us_
                  << ", update_edge=" << graph.total_update_edge_us_
                  << std::endl;

        // Batch sizes: print each batch size once at the end
        if (!batch_sizes_.empty()) {
            std::cout << "Batch sizes (total_batches=" << total_batches_ << "):";
            for (auto size : batch_sizes_) {
                std::cout << " " << size;
            }
            std::cout << std::endl;
        }

        std::cout << "Current most negative cycle: ";
        if (!trial_best_cycle.empty()) {
            for (size_t i = 0; i < trial_best_cycle.size(); ++i) {
                std::cout << trial_best_cycle[i];
                if (i < trial_best_cycle.size() - 1)
                    std::cout << " -> ";
            }
            std::cout << " (Weight: " << trial_best_weight << ")\n";
        } else {
            std::cout << "None\n";
        }

        if (top_cycles.size() < n_) {
            top_cycles.emplace(trial_best_weight, trial_best_cycle);
        } else if (trial_best_weight < top_cycles.top().first) {
            top_cycles.pop();
            top_cycles.emplace(trial_best_weight, trial_best_cycle);
        }

        // Store last trial state for dynamic updates
        current_best_weight_ = trial_best_weight;
        current_best_cycle_ = trial_best_cycle;
    }

    std::vector<CycleInfo> result;
    while (!top_cycles.empty()) {
        result.push_back(top_cycles.top());
        top_cycles.pop();
    }
    return result;
}