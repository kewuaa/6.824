set(targets main master map_worker reduce_worker)
foreach (target IN LISTS targets)
    add_executable(${target})
    target_sources(${target} PRIVATE src/${target}.cpp)
    target_link_libraries(
        ${target}
        PRIVATE
            CLI11::CLI11
            tinyrpc::client
            tinyrpc::server
    )
    target_include_directories(${target} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
endforeach()
