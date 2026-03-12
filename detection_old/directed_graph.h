#ifndef DIRECTED_GRAPH_H
#define DIRECTED_GRAPH_H

#include <string>
#include <vector>
#include <utility>
#include <tuple>
#include <cstdint>
#include <set>
#include <unordered_map>
#include <unordered_set>

class DirectedGraph {
public:
    // Meta information
    uint32_t num_vertices_;
    uint32_t num_edges_;

    uint32_t max_vertex_id_ = 100000;

    double weight_threshold_ = 1000;

    std::vector<bool> is_valid_vertex_;

    // vertices_ is a set of all vertices in the graph
    std::set<uint32_t> vertices_;

    
    std::unordered_map<uint32_t, std::vector<uint32_t>> out_neighbors_;
    std::unordered_map<uint32_t, std::vector<uint32_t>> in_neighbors_;

    std::unordered_map<uint32_t, std::vector<double>> out_weights_;
    std::unordered_map<uint32_t, std::vector<double>> in_weights_;

    // Degree tracking
    std::unordered_map<uint32_t, uint32_t> out_degree_;
    std::unordered_map<uint32_t, uint32_t> in_degree_;

    // Timing accumulators (microseconds)
    long long total_update_edge_us_ = 0;

    // Constructor
    explicit DirectedGraph()
        : num_vertices_(0), num_edges_(0), is_valid_vertex_(max_vertex_id_, false){}


    // Accessors
    inline uint32_t num_vertices() const {
        return num_vertices_;
    }

    inline uint32_t num_edges() const {
        return num_edges_;
    }

    inline std::set<uint32_t>& vertices() {
        return vertices_;
    }

    /**
     * Get out-neighbors of vertex u along with their weights.
     * Returns a tuple containing a pointer to the adjacency list, a pointer to the weights, and the degree.
     * Note: This creates temporary vectors for compatibility with the original interface.
     */
    std::tuple<uint32_t*, double*, uint32_t> get_out_neighbors(uint32_t u) const;

    /**
     * Get in-neighbors of vertex u along with their weights.
     * Returns a tuple containing a pointer to the adjacency list, a pointer to the weights, and the degree.
     * Note: This creates temporary vectors for compatibility with the original interface.
     */
    std::tuple<uint32_t*, double*, uint32_t> get_in_neighbors(uint32_t u) const;

    uint32_t num_out_neighbors(uint32_t u) {
        return out_degree_[u];
    }

    uint32_t num_in_neighbors(uint32_t u) {
        return in_degree_[u];
    }

    uint32_t num_neighbors(uint32_t u) {
        return in_degree_[u] + out_degree_[u];
    }

    void add_node(uint32_t u);

    void update_edge(uint32_t u, uint32_t v, double new_weight);
    // bool has_edge(uint32_t u, uint32_t v) const;
    bool get_edge_weight(uint32_t u, uint32_t v, double& weight);

    void load_edge_list(const std::string& graph_dir, char skip = '#');
    // void print_metadata();
    void print_graph();

};

#endif // DIRECTED_GRAPH_H