#include "directed_graph.h"
#include <fstream>
#include <vector>
#include <unordered_map>
#include <random>
#include <iostream>
#include <algorithm>

// KCycleColorCoding class for detecting negative-weight k-cycles
class KCycleColorCoding {
public:
    KCycleColorCoding(DirectedGraph& graph, uint k, uint num_trials, uint n, unsigned int seed, uint batch_size, bool enable_auto_batch)
        : graph(graph), k_(k), num_trials_(num_trials), n_(n), gen_(seed), dist(0, k_ - 1), batch_size_(batch_size), enable_auto_batch_(enable_auto_batch) {}

    std::vector<std::pair<double, std::vector<uint32_t>>> find_most_negative_k_cycle(
        const std::vector<unsigned int>& trial_seeds,
        const std::string& dynamic_graph_file);

    // Dynamic update interface (call after find_most_negative_k_cycle)
    double current_best_weight_ = std::numeric_limits<double>::infinity();
    std::vector<uint32_t> current_best_cycle_;

    void dynamic_update_edge(uint32_t src, uint32_t dst, double weight) {
        update_edge_weight(src, dst, weight, current_best_weight_, current_best_cycle_);
    }

private:
    DirectedGraph graph;
    std::unordered_map<uint32_t, int> node_colors;
    std::unordered_map<uint32_t, std::unordered_map<uint32_t, std::unordered_map<int, double>>> full_dp_table;
    uint k_;
    uint num_trials_;
    uint n_;
    std::mt19937 gen_;
    std::uniform_int_distribution<int> dist;
    uint batch_size_;
    bool enable_auto_batch_;
    double batch_weight_threshold_;

    // Timing accumulators (microseconds) for dynamic update phase
    long long total_dag_build_us_ = 0;
    long long total_topo_sort_us_ = 0;   // (matches your "topological sorting/decomposition" bucket)
    long long total_rescan_batch_us_ = 0;
    long long total_rescan_edge_us_ = 0;
    long long total_update_classification_us_ = 0;

    // Batch size tracking
    std::vector<uint32_t> batch_sizes_;
    uint32_t total_batches_ = 0;

    inline void reset_timing_accumulators() {
        total_dag_build_us_ = 0;
        total_topo_sort_us_ = 0;
        total_rescan_batch_us_ = 0;
        total_rescan_edge_us_ = 0;
        total_update_classification_us_ = 0;
        batch_sizes_.clear();
        total_batches_ = 0;
    }

    void assign_colors();

    void dp_from_a_node(
        uint32_t start_node, 
        double& local_best_weight,
        std::vector<uint32_t>& local_best_cycle,
        bool enable_back_dp = false
    );
    
    void back_dp_from_a_node(
        uint32_t dst_node
    );

    void Output_most_negative_cycle(
        double& trial_best_weight,
        std::vector<uint32_t>& trial_best_cycle,
        std::string& output_string
    );

    void update_edge_weight(
        uint32_t src_node, 
        uint32_t dst_node, 
        double weight,
        double& local_best_weight,
        std::vector<uint32_t>& local_best_cycle);
    
    void backword_dfs(
        uint32_t dst_node,
        uint32_t current_node,
        double current_weight,
        std::vector<uint32_t>& back_path,
        int color_set,
        double& local_best_weight,
        std::vector<uint32_t>& local_best_cycle,
        std::vector<uint32_t>& backword_visited,
        bool force_compute = false
    );

    void process_dynamic_update_by_edge(
        const std::string& line,
        double& trial_best_weight,
        std::vector<uint32_t>& trial_best_cycle//,
        // bool rescan = false
    );

    void process_dynamic_update_by_batch(
        double& trial_best_weight,
        std::vector<uint32_t>& trial_best_cycle,
        std::vector<std::string>& batch_lines,
        bool rescan_dp_table
    );

    void dp_search_k_cycle(
        uint32_t start_node,
        uint32_t current_node,
        double current_weight,
        std::vector<uint32_t>& path,
        int color_set,
        double& local_best_weight,
        std::vector<uint32_t>& local_best_cycle,
        bool enable_back_dp = false
    );

    void get_all_back_dfs_nodes(
        uint32_t dst_node,
        std::vector<uint32_t>& back_dfs_node
    );

    // Memory usage calculation functions
    size_t calculate_dp_table_memory() const;
    size_t calculate_dag_structure_memory() const;
    void log_memory_usage(const std::string& context = "") const;
};