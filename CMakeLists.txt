cmake_minimum_required(VERSION 3.16)

project(DPHIM CXX)

set(CMAKE_CXX_STANDARD 20)

add_compile_options(-Wall -Wextra -Wpedantic)
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

find_package(Boost REQUIRED)
message("Boost version: ${Boost_VERSION}")
message("Boost include dir: ${Boost_INCLUDE_DIRS}")

file(GLOB DPHIM_SRC ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp)

message("DPHIM_SRC: ${DPHIM_SRC}")
if (DPHIM_SRC)
    set(PROP STATIC)
    set(PROP2 PUBLIC)
else ()
    set(PROP INTERFACE)
    set(PROP2 INTERFACE)
endif ()

add_subdirectory(nova)
add_library(dphim ${PROP} ${DPHIM_SRC})
target_include_directories(dphim ${PROP2}
        ${CMAKE_CURRENT_LIST_DIR}/include
        ${Boost_INCLUDE_DIRS}
        )

execute_process(
        COMMAND /bin/sh -c [[ ldconfig -p | grep libvmem ]]
        OUTPUT_VARIABLE LdconfigLibVmem
        RESULT_VARIABLE ExistLibVmem
)

execute_process(
        COMMAND /bin/sh -c [[ ldconfig -p | grep libpmem2 ]]
        OUTPUT_VARIABLE LdconfigLibPmem2
        RESULT_VARIABLE ExistLibPmem2
)

if ((${ExistLibVmem} EQUAL 0) AND (${ExistLibPmem2} EQUAL 0))
    message("link libvmem and libpmem2")
    target_link_libraries(dphim ${PROP2} nova vmem pmem2)
else ()
    message("libvmem and libpmem2 are not found")
    target_link_libraries(dphim ${PROP2} nova)
endif ()

add_executable(run main.cpp)
target_link_libraries(run dphim jemalloc)

file(GLOB TEST_SOURCES ${CMAKE_CURRENT_LIST_DIR}/tests/*.cpp)
foreach (TEST_SRC ${TEST_SOURCES})
    get_filename_component(TARGET ${TEST_SRC} NAME_WE)
    add_executable(test_${TARGET} ${TEST_SRC})
    target_link_libraries(test_${TARGET} dphim pmem pmem2 pmemobj vmem nova)
endforeach ()
