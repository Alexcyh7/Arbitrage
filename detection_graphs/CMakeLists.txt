cmake_minimum_required(VERSION 3.16)
project(arbitrage_detection_graphs)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O3")

# Fetch nlohmann/json
include(FetchContent)
FetchContent_Declare(
    json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
FetchContent_MakeAvailable(json)

add_executable(detect_graphs
    main.cpp
    directed_graph.cpp
    hp_index.cpp
)
target_link_libraries(detect_graphs nlohmann_json::nlohmann_json)
