#include "pool.h"
#include "directed_graph.h"
#include "hp_index.h"

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <cmath>
#include <vector>
#include <limits>
#include <chrono>
#include <iomanip>

// Socket includes for TCP server
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <climits>

const std::string WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";

// ──────────────────────────────────────────────
//  Token ranking: determines starting node of cycle output
// ──────────────────────────────────────────────
// Lower rank value = higher priority. Tokens not in file get rank INT_MAX.
std::unordered_map<std::string, int> token_rank;

void load_token_ranking(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cout << "No token ranking file found at " << path << ", using default order." << std::endl;
        return;
    }
    std::string address, symbol;
    int rank = 0;
    while (f >> address >> symbol) {
        token_rank[str_to_lower(address)] = rank++;
    }
    std::cout << "Loaded token ranking: " << token_rank.size() << " tokens" << std::endl;
}

// Rotate cycle so the highest-ranked (lowest rank value) token is first.
// cycle format: [v0, v1, ..., vn, v0] where first == last
std::vector<uint32_t> rotate_cycle_by_rank(
    const std::vector<uint32_t>& cycle,
    const std::vector<std::string>& id_to_token)
{
    if (cycle.size() < 2) return cycle;
    size_t n = cycle.size() - 1;  // exclude repeated last element

    int best_rank = INT_MAX;
    size_t best_pos = 0;
    for (size_t i = 0; i < n; i++) {
        const std::string& token = id_to_token[cycle[i]];
        auto it = token_rank.find(token);
        int r = (it != token_rank.end()) ? it->second : INT_MAX;
        if (r < best_rank) {
            best_rank = r;
            best_pos = i;
        }
    }

    // Rotate
    std::vector<uint32_t> rotated;
    for (size_t i = 0; i < n; i++) {
        rotated.push_back(cycle[(best_pos + i) % n]);
    }
    rotated.push_back(rotated[0]);  // close the cycle
    return rotated;
}

// Convert double to integer string (no scientific notation)
inline std::string amount_to_string(double val) {
    if (val <= 0) return "0";
    char buf[128];
    snprintf(buf, sizeof(buf), "%.0f", val);
    return std::string(buf);
}

// ──────────────────────────────────────────────
//  Per-edge tracking: all pools contributing to each directed edge
// ──────────────────────────────────────────────
struct EdgePoolInfo {
    std::unordered_map<size_t, double> pool_weights; // pool_idx -> weight
    double best_weight = std::numeric_limits<double>::infinity();
    size_t best_pool_idx = 0;

    // Recompute best from all pool weights
    void recompute_best() {
        best_weight = std::numeric_limits<double>::infinity();
        for (auto& [pi, w] : pool_weights) {
            if (w < best_weight) {
                best_weight = w;
                best_pool_idx = pi;
            }
        }
    }
};

// Global edge tracking: all_edges[src_id][dst_id]
using EdgeMap = std::unordered_map<uint32_t, std::unordered_map<uint32_t, EdgePoolInfo>>;

// ──────────────────────────────────────────────
//  Compute edge weight for one direction of a pool
//  Returns weight; sets valid=true if computable
// ──────────────────────────────────────────────
inline double compute_edge_weight(
    const Pool& pool, const std::string& src_token,
    double qs_src, double qs_dst, bool& valid)
{
    double out = get_amount_out(pool, src_token, qs_src);
    if (out <= 0 || qs_dst <= 0) { valid = false; return 0; }
    valid = true;
    return -std::log(out / qs_dst);
}

// ──────────────────────────────────────────────
//  Simulate a cycle and return route JSON
// ──────────────────────────────────────────────
json simulate_cycle(
    const std::vector<uint32_t>& raw_cycle,
    const std::vector<std::string>& id_to_token,
    const std::unordered_map<std::string, double>& quote_sizes,
    const EdgeMap& all_edges,
    const std::vector<Pool>& all_pools,
    int64_t block_number)
{
    json result;
    result["profitable"] = false;

    if (raw_cycle.size() < 2) return result;

    // Rotate cycle so highest-ranked token is first
    auto cycle = rotate_cycle_by_rank(raw_cycle, id_to_token);

    std::string start_token = id_to_token[cycle[0]];
    auto qs_it = quote_sizes.find(start_token);
    if (qs_it == quote_sizes.end()) return result;
    double current_amount = qs_it->second;
    double from_amount = current_amount;

    std::cout << "\n=== Simulating Best Cycle ===" << std::endl;
    std::cout << "Start: " << amount_to_string(from_amount) << " of " << start_token << std::endl;

    json fills = json::array();
    size_t num_hops = cycle.size() - 1;

    for (size_t i = 0; i < num_hops; i++) {
        uint32_t src_id = cycle[i];
        uint32_t dst_id = cycle[i + 1];
        std::string src_token = id_to_token[src_id];
        std::string dst_token = id_to_token[dst_id];

        auto& edge_info = all_edges.at(src_id).at(dst_id);
        size_t pool_idx = edge_info.best_pool_idx;
        auto& pool = all_pools[pool_idx];

        double amount_out = get_amount_out(pool, src_token, current_amount);
        std::string source = pool.is_v3 ? "Uniswap_V3" : "Uniswap_V2";

        std::cout << "  Hop " << (i + 1) << ": " << src_token
                  << " -> " << dst_token
                  << " via " << pool.address
                  << " (" << source << ", fee="
                  << (pool.is_v3 ? pool.fee_v3 : pool.fee_v2) << ")"
                  << "  in=" << amount_to_string(current_amount)
                  << "  out=" << amount_to_string(amount_out) << std::endl;

        json fill;
        fill["from"] = src_token;
        fill["to"] = dst_token;
        fill["pool"] = pool.address;
        fill["source"] = source;
        fill["proportionBps"] = "10000";
        fill["expected_output"] = amount_to_string(amount_out);
        fills.push_back(fill);

        current_amount = amount_out;
    }

    double to_amount = current_amount;
    double profit = to_amount - from_amount;
    double profit_pct = (to_amount / from_amount - 1.0) * 100.0;

    std::cout << "\nEnd:   " << amount_to_string(to_amount) << " of " << start_token << std::endl;
    std::cout << "Profit: " << (profit >= 0 ? "+" : "") << std::fixed << std::setprecision(4) << profit_pct << "%" << std::endl;
    if (profit > 0) {
        std::cout << "*** ARBITRAGE OPPORTUNITY FOUND ***" << std::endl;
    }

    // Build route JSON
    result["blockNumber"] = block_number;
    result["from"] = start_token;
    result["to"] = start_token;
    result["fromAmount"] = amount_to_string(from_amount);
    result["toAmount"] = amount_to_string(to_amount);
    result["profit"] = amount_to_string(profit);
    result["profitPct"] = profit_pct;
    result["profitable"] = (profit > 0);
    result["route"]["fills"] = fills;

    return result;
}

// ──────────────────────────────────────────────
//  Recompute edges for a single pool (both directions)
//  Returns list of (src_id, dst_id, new_best_weight) for edges that changed
// ──────────────────────────────────────────────
std::vector<std::tuple<uint32_t, uint32_t, double>> recompute_pool_edges(
    size_t pool_idx,
    const std::vector<Pool>& all_pools,
    const std::unordered_map<std::string, uint32_t>& token_to_id,
    const std::unordered_map<std::string, double>& quote_sizes,
    EdgeMap& all_edges)
{
    std::vector<std::tuple<uint32_t, uint32_t, double>> changed;
    auto& pool = all_pools[pool_idx];

    auto it0 = quote_sizes.find(pool.token0);
    auto it1 = quote_sizes.find(pool.token1);
    if (it0 == quote_sizes.end() || it1 == quote_sizes.end()) return changed;

    auto id0_it = token_to_id.find(pool.token0);
    auto id1_it = token_to_id.find(pool.token1);
    if (id0_it == token_to_id.end() || id1_it == token_to_id.end()) return changed;

    uint32_t id0 = id0_it->second;
    uint32_t id1 = id1_it->second;
    double qs0 = it0->second;
    double qs1 = it1->second;

    // token0 -> token1
    bool valid;
    double w01 = compute_edge_weight(pool, pool.token0, qs0, qs1, valid);
    if (valid) {
        auto& ei = all_edges[id0][id1];
        double old_best = ei.best_weight;
        ei.pool_weights[pool_idx] = w01;
        ei.recompute_best();
        if (ei.best_weight != old_best) {
            changed.emplace_back(id0, id1, ei.best_weight);
        }
    }

    // token1 -> token0
    double w10 = compute_edge_weight(pool, pool.token1, qs1, qs0, valid);
    if (valid) {
        auto& ei = all_edges[id1][id0];
        double old_best = ei.best_weight;
        ei.pool_weights[pool_idx] = w10;
        ei.recompute_best();
        if (ei.best_weight != old_best) {
            changed.emplace_back(id1, id0, ei.best_weight);
        }
    }

    return changed;
}

inline double json_number_to_double(const json& value) {
    try {
        if (value.is_number_float()) return value.get<double>();
        if (value.is_number_integer()) return static_cast<double>(value.get<int64_t>());
        if (value.is_number_unsigned()) return static_cast<double>(value.get<uint64_t>());
        if (value.is_string()) return std::stod(value.get<std::string>());
    } catch (...) {
    }
    return 0.0;
}

std::vector<std::tuple<uint32_t, uint32_t, double>> apply_pool_state_and_recompute(
    size_t pool_idx,
    std::vector<Pool>& all_pools,
    const std::unordered_map<std::string, uint32_t>& token_to_id,
    const std::unordered_map<std::string, std::vector<size_t>>& token_to_pool_indices,
    const std::unordered_map<std::string, std::vector<size_t>>& weth_pool_indices,
    std::unordered_map<std::string, double>& quote_sizes,
    double quote_size_wei,
    EdgeMap& all_edges)
{
    std::vector<std::tuple<uint32_t, uint32_t, double>> edge_changes;
    auto& pool = all_pools[pool_idx];

    bool is_weth_pool = (pool.token0 == WETH || pool.token1 == WETH);
    std::string other_token = (pool.token0 == WETH) ? pool.token1 : pool.token0;

    if (is_weth_pool) {
        double best_output = 0;
        auto it = weth_pool_indices.find(other_token);
        if (it != weth_pool_indices.end()) {
            for (size_t idx : it->second) {
                double output = get_amount_out(all_pools[idx], WETH, quote_size_wei);
                if (output > best_output) best_output = output;
            }
        }
        if (best_output > 0) {
            quote_sizes[other_token] = best_output;
        }

        if (token_to_id.count(other_token) && token_to_pool_indices.count(other_token)) {
            std::unordered_set<size_t> pools_to_update;
            for (size_t pi : token_to_pool_indices.at(other_token)) {
                pools_to_update.insert(pi);
            }
            for (size_t pi : pools_to_update) {
                auto changes = recompute_pool_edges(pi, all_pools, token_to_id, quote_sizes, all_edges);
                edge_changes.insert(edge_changes.end(), changes.begin(), changes.end());
            }
        }
    } else {
        edge_changes = recompute_pool_edges(pool_idx, all_pools, token_to_id, quote_sizes, all_edges);
    }

    return edge_changes;
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <json_file> <seed> <quote_size_eth> [port] [k] [hp_threshold]"
                  << std::endl;
        return 1;
    }

    std::string json_file = argv[1];
    unsigned int seed = static_cast<unsigned int>(std::stoul(argv[2]));
    (void)seed;  // seed not used for HP-Index (deterministic)
    double quote_size_eth = std::stod(argv[3]);
    int port = argc > 4 ? std::stoi(argv[4]) : 0;
    int k = argc > 5 ? std::stoi(argv[5]) : 3;
    int hp_threshold = argc > 6 ? std::stoi(argv[6]) : 10;

    double quote_size_wei = quote_size_eth * 1e18;

    // ── 0. Load token ranking (look next to json_file, then cwd) ──
    {
        std::string dir = json_file.substr(0, json_file.find_last_of("/\\") + 1);
        std::string rank_path = dir + "token_ranking.txt";
        std::ifstream test(rank_path);
        if (!test.is_open()) {
            rank_path = "token_ranking.txt";  // fallback: cwd
        }
        load_token_ranking(rank_path);
    }

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
    int64_t block_number = 0;

    for (auto& entry : data) {
        try {
            all_pools.push_back(parse_pool(entry));
            if (block_number == 0 && entry.contains("block")) {
                block_number = entry["block"].get<int64_t>();
            }
        } catch (const std::exception& e) {
            continue;
        }
    }

    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start).count();
    std::cout << "Parsed " << all_pools.size() << " pools in " << load_ms << " ms" << std::endl;

    // ── 2. Build lookup indices ──────────────────────
    std::unordered_map<std::string, size_t> pool_address_to_idx;
    std::unordered_map<std::string, std::vector<size_t>> token_to_pool_indices;

    for (size_t i = 0; i < all_pools.size(); i++) {
        pool_address_to_idx[all_pools[i].address] = i;
        token_to_pool_indices[all_pools[i].token0].push_back(i);
        token_to_pool_indices[all_pools[i].token1].push_back(i);
    }

    // ── 3. Find WETH-connected tokens ─────────────────
    std::unordered_set<std::string> weth_connected;
    weth_connected.insert(WETH);

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

    // ── 4. Compute quote sizes ────────────────────────
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

    // ── 5. Assign integer IDs to tokens ───────────────
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

    // ── 6. Build graph with full edge tracking ────────
    EdgeMap all_edges;
    auto build_start = std::chrono::high_resolution_clock::now();

    DirectedGraph graph;

    for (size_t pi = 0; pi < all_pools.size(); pi++) {
        auto& pool = all_pools[pi];
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
            double w01 = -std::log(out01 / qs1);
            all_edges[id0][id1].pool_weights[pi] = w01;
        }

        // token1 -> token0
        double out10 = get_amount_out(pool, pool.token1, qs1);
        if (out10 > 0) {
            double w10 = -std::log(out10 / qs0);
            all_edges[id1][id0].pool_weights[pi] = w10;
        }
    }

    // Compute best for each edge and build DirectedGraph
    uint32_t total_edges = 0;
    for (auto& [src, dst_map] : all_edges) {
        for (auto& [dst, ei] : dst_map) {
            ei.recompute_best();
            graph.update_edge(src, dst, ei.best_weight);
            total_edges++;
        }
    }

    auto build_end = std::chrono::high_resolution_clock::now();
    auto build_ms = std::chrono::duration_cast<std::chrono::milliseconds>(build_end - build_start).count();
    std::cout << "Graph built: " << graph.vertices().size() << " vertices, "
              << total_edges << " edges in " << build_ms << " ms" << std::endl;

    // ── 7. Build HP-Index and find initial negative cycle ───────
    std::cout << "\n=== Building HP-Index (k=" << k << ", threshold=" << hp_threshold << ") ===" << std::endl;

    HotPointIndex hp_index(graph, static_cast<uint32_t>(k), static_cast<uint32_t>(hp_threshold));

    auto hp_build_start = std::chrono::high_resolution_clock::now();
    hp_index.build();
    auto hp_build_end = std::chrono::high_resolution_clock::now();
    auto hp_build_ms = std::chrono::duration_cast<std::chrono::milliseconds>(hp_build_end - hp_build_start).count();
    std::cout << "HP-Index built in " << hp_build_ms << " ms" << std::endl;

    // Full scan for initial best negative cycle
    std::cout << "\n=== Running initial full scan for negative cycles ===" << std::endl;
    auto scan_start = std::chrono::high_resolution_clock::now();
    hp_index.find_best_negative_cycle();
    auto scan_end = std::chrono::high_resolution_clock::now();
    auto scan_ms = std::chrono::duration_cast<std::chrono::milliseconds>(scan_end - scan_start).count();
    std::cout << "Full scan completed in " << scan_ms << " ms" << std::endl;

    // ── 8. Simulate best cycle ────────────────────────
    if (!hp_index.current_best_cycle_.empty()) {
        std::cout << "Best cycle: ";
        for (size_t i = 0; i < hp_index.current_best_cycle_.size(); i++) {
            std::cout << hp_index.current_best_cycle_[i];
            if (i < hp_index.current_best_cycle_.size() - 1) std::cout << " -> ";
        }
        std::cout << " (Weight: " << hp_index.current_best_weight_ << ")" << std::endl;

        json route = simulate_cycle(hp_index.current_best_cycle_, id_to_token, quote_sizes,
                                     all_edges, all_pools, block_number);
        std::ofstream out_file("route_output.json");
        out_file << std::setw(2) << route << std::endl;
        out_file.close();
        std::cout << "Route written to route_output.json" << std::endl;
    } else {
        std::cout << "\nNo negative cycles found." << std::endl;
    }

    // ── 9. Dynamic update server ──────────────────────
    if (port <= 0) {
        std::cout << "\nNo port specified, exiting." << std::endl;
        return 0;
    }

    std::cout << "\n=== Starting dynamic update server on port " << port << " ===" << std::endl;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket() failed" << std::endl;
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind() failed" << std::endl;
        close(server_fd);
        return 1;
    }

    if (listen(server_fd, 5) < 0) {
        std::cerr << "listen() failed" << std::endl;
        close(server_fd);
        return 1;
    }

    std::cout << "Listening on port " << port << "..." << std::endl;

    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            std::cerr << "accept() failed" << std::endl;
            continue;
        }
        std::cout << "Client connected." << std::endl;

        // Read newline-delimited JSON from client
        std::string buffer;
        char chunk[4096];
        while (true) {
            ssize_t n = recv(client_fd, chunk, sizeof(chunk) - 1, 0);
            if (n <= 0) break;
            chunk[n] = '\0';
            buffer += chunk;

            // Process complete lines
            size_t pos;
            while ((pos = buffer.find('\n')) != std::string::npos) {
                std::string line = buffer.substr(0, pos);
                buffer = buffer.substr(pos + 1);

                if (line.empty() || line[0] != '{') continue;

                auto update_start = std::chrono::high_resolution_clock::now();

                // Parse pool update
                json entry;
                try {
                    entry = json::parse(line);
                } catch (const std::exception& e) {
                    std::cerr << "JSON parse error: " << e.what() << std::endl;
                    continue;
                }

                std::vector<std::tuple<uint32_t, uint32_t, double>> edge_changes;
                size_t events_seen = 0;
                size_t events_applied = 0;

                // Update block number (legacy single-pool format)
                if (entry.contains("block")) {
                    block_number = entry["block"].get<int64_t>();
                }
                // Update block number (batch event format)
                if (entry.contains("block_info") && entry["block_info"].contains("block_number")) {
                    block_number = entry["block_info"]["block_number"].get<int64_t>();
                }

                if (entry.contains("events") && entry["events"].is_array()) {
                    // New dynamic format: a batch with multiple events.
                    for (const auto& ev : entry["events"]) {
                        events_seen++;
                        if (!ev.contains("parsed_event") || !ev["parsed_event"].is_object()) continue;
                        const auto& parsed_event = ev["parsed_event"];
                        std::string event_type = parsed_event.value("event_type", "");

                        std::string pool_address;
                        if (ev.contains("pair_address")) {
                            pool_address = str_to_lower(ev["pair_address"].get<std::string>());
                        } else if (ev.contains("pool_address")) {
                            pool_address = str_to_lower(ev["pool_address"].get<std::string>());
                        } else {
                            continue;
                        }

                        auto addr_it = pool_address_to_idx.find(pool_address);
                        if (addr_it == pool_address_to_idx.end()) continue;

                        if (ev.contains("block_number")) {
                            block_number = ev["block_number"].get<int64_t>();
                        }

                        size_t pool_idx = addr_it->second;
                        Pool& pool = all_pools[pool_idx];

                        if (!pool.is_v3) {
                            if (event_type != "Sync") continue;
                            if (!parsed_event.contains("reserve0") || !parsed_event.contains("reserve1")) continue;
                            pool.reserve0 = json_number_to_double(parsed_event["reserve0"]);
                            pool.reserve1 = json_number_to_double(parsed_event["reserve1"]);
                        } else {
                            if (!parsed_event.contains("sqrtPriceX96") ||
                                !parsed_event.contains("liquidity") ||
                                !parsed_event.contains("tick")) {
                                continue;
                            }
                            pool.sqrt_price_x96 = json_number_to_double(parsed_event["sqrtPriceX96"]);
                            pool.liquidity = json_number_to_double(parsed_event["liquidity"]);
                            pool.current_tick = static_cast<int>(json_number_to_double(parsed_event["tick"]));
                        }

                        auto changes = apply_pool_state_and_recompute(
                            pool_idx, all_pools, token_to_id, token_to_pool_indices,
                            weth_pool_indices, quote_sizes, quote_size_wei, all_edges);
                        edge_changes.insert(edge_changes.end(), changes.begin(), changes.end());
                        events_applied++;
                    }
                    std::cout << "Processed event batch: " << events_applied << "/" << events_seen
                              << " event(s) applied, " << edge_changes.size() << " edge(s) changed"
                              << std::endl;
                } else {
                    // Legacy format: single snapshot-style pool entry.
                    events_seen = 1;
                    Pool updated_pool;
                    try {
                        updated_pool = parse_pool(entry);
                    } catch (const std::exception& e) {
                        std::cerr << "Pool parse error: " << e.what() << std::endl;
                        continue;
                    }

                    auto addr_it = pool_address_to_idx.find(updated_pool.address);
                    if (addr_it == pool_address_to_idx.end()) {
                        std::cout << "Unknown pool " << updated_pool.address << ", skipping." << std::endl;
                        continue;
                    }
                    size_t pool_idx = addr_it->second;

                    Pool& pool = all_pools[pool_idx];
                    if (pool.is_v3) {
                        pool.sqrt_price_x96 = updated_pool.sqrt_price_x96;
                        pool.current_tick = updated_pool.current_tick;
                        pool.liquidity = updated_pool.liquidity;
                        pool.ticks = updated_pool.ticks;
                    } else {
                        pool.reserve0 = updated_pool.reserve0;
                        pool.reserve1 = updated_pool.reserve1;
                    }

                    edge_changes = apply_pool_state_and_recompute(
                        pool_idx, all_pools, token_to_id, token_to_pool_indices,
                        weth_pool_indices, quote_sizes, quote_size_wei, all_edges);
                    events_applied = 1;
                    std::cout << "Processed single pool update: " << edge_changes.size()
                              << " edge(s) changed" << std::endl;
                }

                // Feed edge updates to HP-Index
                for (auto& [src, dst, new_weight] : edge_changes) {
                    hp_index.update_edge(src, dst, new_weight);
                }

                auto update_end = std::chrono::high_resolution_clock::now();
                auto update_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    update_end - update_start).count();

                // Build response JSON
                json response;
                response["weight"] = hp_index.current_best_weight_;
                response["update_us"] = update_us;
                response["edges_changed"] = edge_changes.size();
                response["events_seen"] = events_seen;
                response["events_applied"] = events_applied;

                if (!hp_index.current_best_cycle_.empty()) {
                    std::cout << "  Best cycle: ";
                    for (size_t i = 0; i < hp_index.current_best_cycle_.size(); i++) {
                        std::cout << hp_index.current_best_cycle_[i];
                        if (i < hp_index.current_best_cycle_.size() - 1) std::cout << " -> ";
                    }
                    std::cout << " (weight=" << hp_index.current_best_weight_ << ")"
                              << " [" << update_us << " us]" << std::endl;

                    // Simulate and include route in response
                    json route = simulate_cycle(hp_index.current_best_cycle_, id_to_token,
                                                quote_sizes, all_edges, all_pools, block_number);
                    response["profitable"] = route.value("profitable", false);
                    response["route"] = route;
                } else {
                    response["profitable"] = false;
                }

                // Send response back to client (single line JSON + newline)
                std::string resp_str = response.dump() + "\n";
                send(client_fd, resp_str.c_str(), resp_str.size(), 0);
            }
        }

        close(client_fd);
        std::cout << "Client disconnected." << std::endl;
    }

    close(server_fd);
    return 0;
}
