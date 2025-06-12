add_executable(test_raft_node)
target_sources(test_raft_node PRIVATE raft_node.cpp)
target_link_libraries(
    test_raft_node
    PRIVATE
        raft
        CLI11::CLI11
)

add_executable(test_client)
target_sources(test_client PRIVATE client.cpp)
target_link_libraries(
    test_client
    PRIVATE
        raft
        CLI11::CLI11
)
