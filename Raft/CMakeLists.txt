set(RAFT_PORT 9999 CACHE STRING "raft port")
set(RAFT_INNER_PORT 8888 CACHE STRING "raft inner port")
set(RAFT_MAX_LISTEN_NUM 1024 CACHE STRING "max listen number of socket")
set(RAFT_WAIT_TIMEOUT -1 CACHE STRING "wait timeout(ms)")

configure_file(src/config.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/raft_config.hpp)

add_library(raft)
generate_export_header(raft EXPORT_FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/raft_export.hpp)
target_compile_definitions(raft PUBLIC SPDLOG_ACTIVE_LEVEL=DEBUG)
target_include_directories(raft PUBLIC src/ ${CMAKE_CURRENT_BINARY_DIR})
target_sources(raft PRIVATE src/raft.cpp)
target_link_libraries(
    raft
    PUBLIC
        tinyrpc::server
        tinyrpc::client
)

add_subdirectory(tests)
