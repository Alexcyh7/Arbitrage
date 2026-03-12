#pragma once

#include <string>
#include <vector>
#include <cmath>
#include <algorithm>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// ──────────────────────────────────────────────
//  Hex utilities
// ──────────────────────────────────────────────

inline int hex_char_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + c - 'a';
    if (c >= 'A' && c <= 'F') return 10 + c - 'A';
    return 0;
}

// Parse hex string to double (unsigned). Works for arbitrarily large hex values.
// Precision loss in low bits is acceptable for exchange rate computation.
inline double hex_to_double(const std::string& h) {
    size_t start = (h.size() >= 2 && h[0] == '0' && (h[1] == 'x' || h[1] == 'X')) ? 2 : 0;
    double result = 0.0;
    for (size_t i = start; i < h.size(); i++) {
        result = result * 16.0 + hex_char_val(h[i]);
    }
    return result;
}

// Parse hex string as signed integer (two's complement) to double.
inline double hex_to_signed_double(const std::string& h) {
    size_t start = (h.size() >= 2 && h[0] == '0' && (h[1] == 'x' || h[1] == 'X')) ? 2 : 0;
    std::string digits(h.begin() + start, h.end());
    if (digits.size() % 2 != 0) digits = "0" + digits;

    double raw = hex_to_double(h);
    if (hex_char_val(digits[0]) >= 8) {
        int byte_len = static_cast<int>(digits.size()) / 2;
        raw -= std::pow(2.0, byte_len * 8);
    }
    return raw;
}

inline std::string str_to_lower(const std::string& s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

// ──────────────────────────────────────────────
//  Pool data structure (unified V2 + V3)
// ──────────────────────────────────────────────

struct Pool {
    std::string address;
    std::string token0;
    std::string token1;
    bool is_v3;

    // V2 fields
    double reserve0 = 0;
    double reserve1 = 0;
    int fee_v2 = 0; // in 1/10000 units (e.g. 30 = 0.3%)

    // V3 fields
    double sqrt_price_x96 = 0;
    int current_tick = 0;
    double liquidity = 0;
    int fee_v3 = 0; // in 1/1000000 units (e.g. 3000 = 0.3%)
    int tick_spacing = 0;

    struct Tick {
        int idx;
        double net_liquidity; // signed
    };
    std::vector<Tick> ticks;
};

// ──────────────────────────────────────────────
//  JSON parsing
// ──────────────────────────────────────────────

inline Pool parse_pool(const json& entry) {
    Pool pool;
    if (entry.contains("component") &&
        entry["component"].contains("static_attributes") &&
        entry["component"]["static_attributes"].contains("pool_address")) {
        pool.address = str_to_lower(entry["component"]["static_attributes"]["pool_address"].get<std::string>());
    } else {
        pool.address = str_to_lower(entry["component"]["id"].get<std::string>());
    }

    auto& tokens = entry["component"]["tokens"];
    pool.token0 = str_to_lower(tokens[0].get<std::string>());
    pool.token1 = str_to_lower(tokens[1].get<std::string>());

    std::string protocol = entry["protocol_system"];
    auto& attrs = entry["state"]["attributes"];
    auto& static_attrs = entry["component"]["static_attributes"];

    if (protocol == "uniswap_v2") {
        pool.is_v3 = false;
        pool.reserve0 = hex_to_double(attrs["reserve0"].get<std::string>());
        pool.reserve1 = hex_to_double(attrs["reserve1"].get<std::string>());
        pool.fee_v2 = static_cast<int>(hex_to_double(static_attrs["fee"].get<std::string>()));
    } else {
        pool.is_v3 = true;
        pool.sqrt_price_x96 = hex_to_double(attrs["sqrt_price_x96"].get<std::string>());
        pool.current_tick = static_cast<int>(hex_to_signed_double(attrs["tick"].get<std::string>()));
        pool.liquidity = hex_to_double(attrs["liquidity"].get<std::string>());
        pool.fee_v3 = static_cast<int>(hex_to_double(static_attrs["fee"].get<std::string>()));
        pool.tick_spacing = static_cast<int>(hex_to_double(static_attrs["tick_spacing"].get<std::string>()));

        // Parse ticks from "ticks/{idx}/net-liquidity" keys
        for (auto& [key, val] : attrs.items()) {
            if (key.size() > 6 && key.compare(0, 6, "ticks/") == 0) {
                size_t second_slash = key.find('/', 6);
                if (second_slash != std::string::npos &&
                    key.compare(second_slash, std::string::npos, "/net-liquidity") == 0) {
                    std::string tick_str = key.substr(6, second_slash - 6);
                    int tick_idx = std::stoi(tick_str);
                    double net_liq = hex_to_signed_double(val.get<std::string>());
                    pool.ticks.push_back({tick_idx, net_liq});
                }
            }
        }
    }

    return pool;
}

// ──────────────────────────────────────────────
//  V2 swap simulation (double precision)
// ──────────────────────────────────────────────

inline double v2_get_amount_out(const Pool& pool, const std::string& input_token, double amount_in) {
    double reserve_in, reserve_out;
    if (input_token == pool.token0) {
        reserve_in = pool.reserve0;
        reserve_out = pool.reserve1;
    } else {
        reserve_in = pool.reserve1;
        reserve_out = pool.reserve0;
    }

    if (reserve_in <= 0 || reserve_out <= 0) return 0;

    double fee = pool.fee_v2; // e.g. 30
    double amount_in_with_fee = amount_in * (10000.0 - fee);
    return (amount_in_with_fee * reserve_out) / (reserve_in * 10000.0 + amount_in_with_fee);
}

// ──────────────────────────────────────────────
//  V3 swap simulation (double precision)
// ──────────────────────────────────────────────

static constexpr double Q96_DOUBLE = 79228162514264337593543950336.0; // 2^96

inline double v3_get_amount_out(const Pool& pool, const std::string& input_token, double amount_in) {
    bool zero_for_one = (input_token == pool.token0);
    double fee_rate = pool.fee_v3 / 1000000.0;
    double sp = pool.sqrt_price_x96 / Q96_DOUBLE;
    double L = pool.liquidity;
    double remaining = amount_in;
    double amount_out = 0;

    if (sp <= 1e-30 || L <= 0) return 0;

    if (zero_for_one) {
        // token0 -> token1: price decreases, walk ticks downward
        std::vector<Pool::Tick> sorted_ticks;
        for (auto& t : pool.ticks) {
            if (t.idx <= pool.current_tick) {
                sorted_ticks.push_back(t);
            }
        }
        std::sort(sorted_ticks.begin(), sorted_ticks.end(),
                  [](const auto& a, const auto& b) { return a.idx > b.idx; });

        for (auto& tick : sorted_ticks) {
            if (remaining <= 0) break;
            double sp_target = std::pow(1.0001, tick.idx / 2.0);
            if (sp_target >= sp) continue;
            if (L <= 1e-18) break;

            double dx_net = L * (sp - sp_target) / (sp * sp_target);
            double dx_gross = dx_net / (1.0 - fee_rate);

            if (remaining >= dx_gross) {
                amount_out += L * (sp - sp_target);
                remaining -= dx_gross;
                sp = sp_target;
                L -= tick.net_liquidity;
            } else {
                double dx_net_avail = remaining * (1.0 - fee_rate);
                double sp_new = (L * sp) / (L + dx_net_avail * sp);
                amount_out += L * (sp - sp_new);
                remaining = 0;
                sp = sp_new;
            }
        }

        if (remaining > 0 && L > 1e-18) {
            double dx_net_avail = remaining * (1.0 - fee_rate);
            double sp_new = (L * sp) / (L + dx_net_avail * sp);
            amount_out += L * (sp - sp_new);
        }
    } else {
        // token1 -> token0: price increases, walk ticks upward
        std::vector<Pool::Tick> sorted_ticks;
        for (auto& t : pool.ticks) {
            if (t.idx > pool.current_tick) {
                sorted_ticks.push_back(t);
            }
        }
        std::sort(sorted_ticks.begin(), sorted_ticks.end(),
                  [](const auto& a, const auto& b) { return a.idx < b.idx; });

        for (auto& tick : sorted_ticks) {
            if (remaining <= 0) break;
            double sp_target = std::pow(1.0001, tick.idx / 2.0);
            if (sp_target <= sp) continue;
            if (L <= 1e-18) break;

            double dy_net = L * (sp_target - sp);
            double dy_gross = dy_net / (1.0 - fee_rate);

            if (remaining >= dy_gross) {
                amount_out += L * (1.0 / sp - 1.0 / sp_target);
                remaining -= dy_gross;
                sp = sp_target;
                L += tick.net_liquidity;
            } else {
                double dy_net_avail = remaining * (1.0 - fee_rate);
                double sp_new = sp + dy_net_avail / L;
                amount_out += L * (1.0 / sp - 1.0 / sp_new);
                remaining = 0;
                sp = sp_new;
            }
        }

        if (remaining > 0 && L > 1e-18) {
            double dy_net_avail = remaining * (1.0 - fee_rate);
            double sp_new = sp + dy_net_avail / L;
            amount_out += L * (1.0 / sp - 1.0 / sp_new);
        }
    }

    return amount_out;
}

// ──────────────────────────────────────────────
//  Unified swap function
// ──────────────────────────────────────────────

inline double get_amount_out(const Pool& pool, const std::string& input_token, double amount_in) {
    if (amount_in <= 0) return 0;
    if (pool.is_v3) {
        return v3_get_amount_out(pool, input_token, amount_in);
    } else {
        return v2_get_amount_out(pool, input_token, amount_in);
    }
}
