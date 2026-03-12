cmake_minimum_required(VERSION 3.16)
project(arbitrage_detection)

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

add_executable(detect
    main.cpp
    directed_graph.cpp
    cycle_detector.cpp
)
target_link_libraries(detect nlohmann_json::nlohmann_json)
