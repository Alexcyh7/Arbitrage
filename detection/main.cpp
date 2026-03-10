#include "pool.h"
#include "directed_graph.h"
#include "cycle_detector.h"

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <vector>
#include <limits>
#include <chrono>

const std::string WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <json_file> <seed> <quote_size_eth> [port] [k]"
                  << std::endl;
        return 1;
    }

    std::string json_file = argv[1];
    unsigned int seed = static_cast<unsigned int>(std::stoul(argv[2]));
    double quote_size_eth = std::stod(argv[3]);
    // int port = argc > 4 ? std::stoi(argv[4]) : 0;  // reserved for future use
    int k = argc > 5 ? std::stoi(argv[5]) : 3;

    double quote_size_wei = quote_size_eth * 1e18;

    // ── 1. Load and parse JSON ────────────────────────
    std::cout << "Loading pools from " << json_file << "..." << std::endl;
    auto load_start = std::chrono::high_resolution_clock::now();

    std::ifstream f(json_file);
    if (!f.is_open()) {
        std::cerr << "Failed to open " << json_file << std::endl;
        return 1;
    }
    json data = json::parse(f);
    f.close();

    std::vector<Pool> all_pools;
    for (auto& entry : data) {
        try {
            all_pools.push_back(parse_pool(entry));
        } catch (const std::exception& e) {
            continue; // skip unparseable pools
        }
    }

    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start).count();
    std::cout << "Parsed " << all_pools.size() << " pools in " << load_ms << " ms" << std::endl;

    // ── 2. Find WETH-connected tokens ─────────────────
    std::unordered_set<std::string> weth_connected;
    weth_connected.insert(WETH);

    // For each token, track which pool indices connect it to WETH
    std::unordered_map<std::string, std::vector<size_t>> weth_pool_indices;

    for (size_t i = 0; i < all_pools.size(); i++) {
        auto& pool = all_pools[i];
        if (pool.token0 == WETH) {
            weth_connected.insert(pool.token1);
            weth_pool_indices[pool.token1].push_back(i);
        } else if (pool.token1 == WETH) {
            weth_connected.insert(pool.token0);
            weth_pool_indices[pool.token0].push_back(i);
        }
    }

    std::cout << "WETH-connected tokens: " << weth_connected.size() << std::endl;

    // ── 3. Compute quote sizes ────────────────────────
    // For each token, swap quote_size_wei of WETH -> token via its best direct pool
    std::unordered_map<std::string, double> quote_sizes;
    quote_sizes[WETH] = quote_size_wei;

    for (auto& [token, pool_indices] : weth_pool_indices) {
        double best_output = 0;
        for (size_t idx : pool_indices) {
            double output = get_amount_out(all_pools[idx], WETH, quote_size_wei);
            if (output > best_output) {
                best_output = output;
            }
        }
        if (best_output > 0) {
            quote_sizes[token] = best_output;
        }
    }

    std::cout << "Tokens with valid quote sizes: " << quote_sizes.size() << std::endl;

    // ── 4. Assign integer IDs to tokens ───────────────
    std::unordered_map<std::string, uint32_t> token_to_id;
    std::vector<std::string> id_to_token;
    uint32_t next_id = 0;

    for (auto& [token, qs] : quote_sizes) {
        token_to_id[token] = next_id;
        id_to_token.push_back(token);
        next_id++;
    }

    // Print node mapping
    std::cout << "\n=== Node Mapping ===" << std::endl;
    for (uint32_t i = 0; i < id_to_token.size(); i++) {
        std::cout << "  " << i << " -> " << id_to_token[i];
        if (id_to_token[i] == WETH) std::cout << " (WETH)";
        std::cout << std::endl;
    }
    std::cout << std::endl;

    // ── 5. Build graph ────────────────────────────────
    // For each pool where both tokens have quote sizes,
    // compute edge weights in both directions.
    // Keep minimum weight per (src, dst) pair across all pools.
    auto build_start = std::chrono::high_resolution_clock::now();

    std::unordered_map<uint32_t, std::unordered_map<uint32_t, double>> edge_weights;

    for (auto& pool : all_pools) {
        // Both tokens must have quote sizes
        auto it0 = quote_sizes.find(pool.token0);
        auto it1 = quote_sizes.find(pool.token1);
        if (it0 == quote_sizes.end() || it1 == quote_sizes.end()) continue;

        uint32_t id0 = token_to_id[pool.token0];
        uint32_t id1 = token_to_id[pool.token1];
        double qs0 = it0->second;
        double qs1 = it1->second;

        // token0 -> token1
        double out01 = get_amount_out(pool, pool.token0, qs0);
        if (out01 > 0) {
            double weight01 = -std::log(out01 / qs1);
            auto& w = edge_weights[id0][id1];
            if (edge_weights[id0].find(id1) == edge_weights[id0].end() || weight01 < w) {
                w = weight01;
            }
        }

        // token1 -> token0
        double out10 = get_amount_out(pool, pool.token1, qs1);
        if (out10 > 0) {
            double weight10 = -std::log(out10 / qs0);
            auto& w = edge_weights[id1][id0];
            if (edge_weights[id1].find(id0) == edge_weights[id1].end() || weight10 < w) {
                w = weight10;
            }
        }
    }

    // Build DirectedGraph
    DirectedGraph graph;
    uint32_t total_edges = 0;
    for (auto& [src, dst_map] : edge_weights) {
        for (auto& [dst, weight] : dst_map) {
            graph.update_edge(src, dst, weight);
            total_edges++;
        }
    }

    auto build_end = std::chrono::high_resolution_clock::now();
    auto build_ms = std::chrono::duration_cast<std::chrono::milliseconds>(build_end - build_start).count();
    std::cout << "Graph built: " << graph.vertices().size() << " vertices, "
              << total_edges << " edges in " << build_ms << " ms" << std::endl;

    // ── 6. Run negative cycle detection ───────────────
    std::cout << "\n=== Running k=" << k << " negative cycle detection ===" << std::endl;

    std::vector<unsigned int> trial_seeds = {seed};
    KCycleColorCoding detector(graph, k, 1, 10, seed, 1, false);
    auto results = detector.find_most_negative_k_cycle(trial_seeds, "");

    // ── 7. Output results with token addresses ────────
    if (!results.empty()) {
        std::cout << "\n=== Arbitrage Opportunities ===" << std::endl;
        for (size_t r = 0; r < results.size(); r++) {
            auto& [weight, cycle] = results[r];
            if (weight >= 0) continue; // only show profitable cycles

            double profit_factor = std::exp(-weight);
            std::cout << "\nCycle " << (r + 1) << " (weight: " << weight
                      << ", profit factor: " << profit_factor << "):" << std::endl;

            for (size_t i = 0; i < cycle.size(); i++) {
                uint32_t nid = cycle[i];
                std::string token = (nid < id_to_token.size()) ? id_to_token[nid] : "???";
                std::cout << "  " << token;
                if (token == WETH) std::cout << " (WETH)";
                if (i < cycle.size() - 1) std::cout << " ->";
                std::cout << std::endl;
            }
        }
    } else {
        std::cout << "\nNo negative cycles found." << std::endl;
    }

    return 0;
}
