cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_COMPILER "g++-13" CACHE PATH "g++ compiler" FORCE)
set(USE_ADDRESS_SANITIZER OFF CACHE BOOL "Use address sanitizer" FORCE)
set(USE_JEMALLOC ON CACHE BOOL "Use jemalloc" FORCE)

project(DPHIM CXX)

set(CMAKE_CXX_STANDARD 20)

option(CMAKE_BUILD_TYPE "Build type" Release)
option(USE_ADDRESS_SANITIZER "Use address sanitizer" OFF)

if ("${USE_ADDRESS_SANITIZER}")
    message("Use address sanitizer")
    link_libraries(asan)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
endif ()

add_compile_options(-Wall -Wextra -Wpedantic)
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")
set(CMAKE_CXX_FLAGS_RELEASE "-Ofast -g0")
message("CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

find_package(Boost REQUIRED NO_MODULE)
message("Boost version: ${Boost_VERSION}")
message("Boost include dir: ${Boost_INCLUDE_DIRS}")

file(GLOB DPHIM_SRC ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp)

message("DPHIM_SRC: ${DPHIM_SRC}")
if (DPHIM_SRC)
    set(LIB_TYPE STATIC)
    set(LIB_VISIBILITY PUBLIC)
else ()
    set(LIB_TYPE INTERFACE)
    set(LIB_VISIBILITY INTERFACE)
endif ()

add_subdirectory(nova)
add_library(dphim ${LIB_TYPE} ${DPHIM_SRC})
target_include_directories(dphim ${LIB_VISIBILITY}
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

#execute_process(
#        COMMAND /bin/sh -c [[ ldconfig -p | grep libjemalloc ]]
#        OUTPUT_VARIABLE LdconfigLibJemalloc
#        RESULT_VARIABLE ExistLibJemalloc
#)

if ((${ExistLibVmem} EQUAL 0) AND (${ExistLibPmem2} EQUAL 0))
    message("link libvmem and libpmem2")
    target_link_libraries(dphim ${LIB_VISIBILITY} nova vmem pmem2)
else ()
    message("libvmem and libpmem2 are not found")
    target_link_libraries(dphim ${LIB_VISIBILITY} nova)
endif ()

target_link_libraries(dphim ${LIB_VISIBILITY} papi)

add_executable(run main.cpp)
target_link_libraries(run PRIVATE dphim)

#add_custom_target(sync
#        COMMAND rsync -avz
#        --exclude-from=${CMAKE_CURRENT_LIST_DIR}/.gitignore
#        --exclude='${CMAKE_CURRENT_LIST_DIR}/.git'
#        ${CMAKE_CURRENT_LIST_DIR}
#        kimura-g@katsunuma2-tiny:/opt/sandbox/
#        COMMAND rsync -chavzP
#        kimura-g@katsunuma2-tiny:/opt/sandbox/dphim/log
#        ${CMAKE_CURRENT_LIST_DIR}/
#)

#file(GLOB TEST_SOURCES ${CMAKE_CURRENT_LIST_DIR}/tests/*.cpp)
#foreach (TEST_SRC ${TEST_SOURCES})
#    get_filename_component(TARGET ${TEST_SRC} NAME_WE)
#    message("dphim test: dphim_test_${TARGET}")
#    add_executable(dphim_test_${TARGET} ${TEST_SRC})
#    target_link_libraries(dphim_test_${TARGET} PRIVATE dphim)
#endforeach ()

