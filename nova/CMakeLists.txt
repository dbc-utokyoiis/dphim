cmake_minimum_required(VERSION 3.10)

option(USE_JEMALLOC "Use jemalloc" ON)
execute_process(
        COMMAND /bin/sh -c [[ ldconfig -p | grep libjemalloc ]]
        OUTPUT_VARIABLE LdconfigLibJemalloc
        RESULT_VARIABLE ExistLibJemalloc
)

add_compile_options(-Wall -Wextra -Wpedantic)

set(NOVA_SRC
        ${CMAKE_CURRENT_LIST_DIR}/src/jemalloc.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/numa_aware_scheduler.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/simple_scheduler.cpp
        ${CMAKE_CURRENT_LIST_DIR}/src/single_queue_scheduler.cpp
)

if (NOVA_SRC)
    add_library(nova STATIC ${NOVA_SRC})
    message("NOVA_SRC: ${NOVA_SRC}")
    set(PROP PUBLIC)
else ()
    add_library(nova INTERFACE ${NOVA_SRC})
    set(PROP INTERFACE)
endif ()

set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g3")

target_include_directories(nova ${PROP} ${CMAKE_CURRENT_LIST_DIR}/include)
target_compile_options(nova ${PROP}
        -std=c++20
        -Wall
        -Wextra
        -Wpedantic
)
target_link_libraries(nova ${PROP} pthread numa)

message("CMAKE_CXX_COMPILER_ID: ${CMAKE_CXX_COMPILER_ID}")
message("CMAKE_CXX_COMPILER_VERSION: ${CMAKE_CXX_COMPILER_VERSION}")

if ((CMAKE_CXX_COMPILER_ID STREQUAL "Clang") OR (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"))
    target_link_options(nova ${PROP} -stdlib=libc++)
    target_compile_options(nova ${PROP}
            -fcoroutines-ts
            -stdlib=libc++
    )
elseif ((CMAKE_CXX_COMPILER_ID STREQUAL "GNU"))
    target_compile_options(nova ${PROP}
            -fcoroutines
    )
endif ()

if ((${ExistLibJemalloc} EQUAL 0) AND ${USE_JEMALLOC})
    message("use libjemalloc")
    target_compile_options(nova PUBLIC -DUSE_JEMALLOC)
    target_link_libraries(nova PUBLIC jemalloc)
else ()
    message("not use libjemalloc")
endif ()

target_link_libraries(nova PUBLIC papi)

#file(GLOB TEST_SOURCES ${CMAKE_CURRENT_LIST_DIR}/tests/*.cpp)
#foreach (TEST_SRC ${TEST_SOURCES})
#    get_filename_component(TARGET ${TEST_SRC} NAME_WE)
#    message("nova test: nova_test_${TARGET}")
#    add_executable(nova_test_${TARGET} ${TEST_SRC})
#    target_link_libraries(nova_test_${TARGET} PRIVATE nova)
#endforeach ()
