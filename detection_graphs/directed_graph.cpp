#include "directed_graph.h"
#include <iostream>
#include <fstream>
#include <unordered_set>
#include <sstream>
#include <chrono>
#include <vector>
#include <cassert>
#include <algorithm>

void DirectedGraph::load_edge_list(const std::string& graph_dir, char skip) {
    std::ifstream file(graph_dir);
    std::string line;
    while (std::getline(file, line)) {
        if (line[0] == skip) continue;
        std::istringstream iss(line);
        uint32_t u, v;
        double weight;
        iss >> u >> v >> weight;
        if(weight > weight_threshold_) {
            continue;
        }
        add_node(u);
        add_node(v);
        out_neighbors_[u].push_back(v);
        in_neighbors_[v].push_back(u);
        out_weights_[u].push_back(weight);
        in_weights_[v].push_back(weight);
        out_degree_[u]++;
        in_degree_[v]++;
    }
}

void DirectedGraph::update_edge(uint32_t u, uint32_t v, double new_weight) {
    auto start_time = std::chrono::high_resolution_clock::now();
    add_node(u);
    add_node(v);
    auto it = std::find_if(out_neighbors_[u].begin(), out_neighbors_[u].end(), 
                          [v](uint32_t neighbor) { return neighbor == v; });
    if(it != out_neighbors_[u].end()) {
        out_weights_[u][it - out_neighbors_[u].begin()] = new_weight;
    }
    else{
        out_neighbors_[u].push_back(v);
        out_weights_[u].push_back(new_weight);
        out_degree_[u]++;
    }

    it = std::find_if(in_neighbors_[v].begin(), in_neighbors_[v].end(), 
                          [u](uint32_t neighbor) { return neighbor == u; });
    if(it != in_neighbors_[v].end()) {
        in_weights_[v][it - in_neighbors_[v].begin()] = new_weight;
    }
    else{
        in_neighbors_[v].push_back(u);  
        in_weights_[v].push_back(new_weight);
        in_degree_[v]++;
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    total_update_edge_us_ += duration;
}


bool DirectedGraph::get_edge_weight(uint32_t u, uint32_t v, double& weight) {
    auto it = std::find_if(out_neighbors_[u].begin(), out_neighbors_[u].end(), 
                          [v](uint32_t neighbor) { return neighbor == v; });
    if(it != out_neighbors_[u].end()) {
        weight = out_weights_[u][it - out_neighbors_[u].begin()];
        return true;
    }
    return false;
}

void DirectedGraph::add_node(uint32_t u) {
    if(is_valid_vertex_[u]) {
        return;
    }
    if(u >= max_vertex_id_) {
        max_vertex_id_ *= u*2;
        is_valid_vertex_.resize(max_vertex_id_, false);
    }
    is_valid_vertex_[u] = true;
    vertices_.insert(u);
    out_neighbors_[u] = std::vector<uint32_t>();
    in_neighbors_[u] = std::vector<uint32_t>();
    out_weights_[u] = std::vector<double>();
    in_weights_[u] = std::vector<double>();
    out_degree_[u] = 0;
    in_degree_[u] = 0;
}

std::tuple<uint32_t*, double*, uint32_t> DirectedGraph::get_out_neighbors(uint32_t u) const {
    return std::make_tuple(const_cast<uint32_t*>(out_neighbors_.at(u).data()), 
                          const_cast<double*>(out_weights_.at(u).data()), 
                          out_degree_.at(u));
}

std::tuple<uint32_t*, double*, uint32_t> DirectedGraph::get_in_neighbors(uint32_t u) const {
    return std::make_tuple(const_cast<uint32_t*>(in_neighbors_.at(u).data()), 
                          const_cast<double*>(in_weights_.at(u).data()), 
                          in_degree_.at(u));
}

void DirectedGraph::print_graph() {
    for(uint32_t u : vertices_) {
        std::cout << "----------------------------------------" << std::endl;
        for(uint32_t i = 0; i < out_degree_[u]; i++) {
            std::cout << u << " " << out_neighbors_[u][i] << " " << out_weights_[u][i] << std::endl;
        }
        std::cout << "----------------------------------------" << std::endl;
    }
    std::cout << std::endl;
}