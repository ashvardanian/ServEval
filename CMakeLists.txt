cmake_minimum_required(VERSION 3.28)
project(
    serveval
    LANGUAGES C CXX
    DESCRIPTION "Agentic-scale serving benchmarks, each framework driven by its own client"
)

include(FetchContent)
# UCall's main-dev, which `release-siblings` swaps for the `../UCall` checkout.
FetchContent_Declare(
    ucall
    GIT_REPOSITORY https://github.com/unum-cloud/UCall
    GIT_TAG 65fb9cc2c9275f43372d4889c2c2861a511e0055
    EXCLUDE_FROM_ALL SYSTEM
)
FetchContent_MakeAvailable(ucall)
include(${ucall_SOURCE_DIR}/cmake/yyjson.cmake)

add_executable(serv-eval-ucall-server ucall_server.cpp)
add_executable(serv-eval-ucall-client ucall_client.cpp)
target_link_libraries(serv-eval-ucall-server PRIVATE ucall::cpp yyjson::yyjson)
target_link_libraries(serv-eval-ucall-client PRIVATE ucall::cpp forkunion::static yyjson::yyjson)
