cmake_minimum_required(VERSION 3.12 FATAL_ERROR)

project(ORCCopy C CXX)

enable_testing()


set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O2")

find_package (Threads)

include_directories(SYSTEM
    ${PROJECT_SOURCE_DIR}/src
    /usr/local/include
)
link_directories(BEFORE
	/usr/local/lib
)

add_subdirectory(src)
