cmake_minimum_required(VERSION 3.15)

project(6.824 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAG_DEBUG "-O0 -Wall -g -ggdb")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN TRUE)


if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

include(cmake/CPM.cmake)

CPMAddPackage("gh:CLIUtils/CLI11@2.5.0")
CPMAddPackage(
    NAME tinyrpc
    GIT_REPOSITORY "https://github.com/kewuaa/tinyrpc.git"
    GIT_TAG master
    OPTIONS "BUILD_TESTS OFF"
)

add_subdirectory(MapReduce)
