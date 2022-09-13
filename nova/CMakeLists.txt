cmake_minimum_required(VERSION 3.0)

add_compile_options(-Wall -Wextra -Wpedantic)

file(GLOB NOVA_SRC ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp)

if (NOVA_SRC)
    add_library(nova STATIC ${NOVA_SRC})
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

message("${CMAKE_CXX_COMPILER_ID}")
if ((CMAKE_CXX_COMPILER_ID STREQUAL "Clang") OR (CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"))
    message("Clang")
    target_link_options(nova ${PROP} -stdlib=libc++)
    target_compile_options(nova ${PROP}
            -fcoroutines-ts
            -stdlib=libc++
            )
elseif ((CMAKE_CXX_COMPILER_ID STREQUAL "GNU"))
    message("GNU")
    target_compile_options(nova ${PROP}
            -fcoroutines
            )
endif ()

file(GLOB NOVA_TEST_SRC ${CMAKE_CURRENT_LIST_DIR}/tests/*.cpp)
foreach (TEST_SRC ${NOVA_TEST_SRC})
    get_filename_component(TARGET ${TEST_SRC} NAME_WE)
    add_executable(test_${TARGET} ${TEST_SRC})
    target_link_libraries(test_${TARGET} nova)
endforeach ()
