#pragma once

#include <cstddef>

#if defined(USE_JEMALLOC)
#define HAS_JEMALLOC
#include <jemalloc/jemalloc.h>
#else
#warning "NO JEMALLOC"
#endif

namespace nova {
inline namespace memory {

void *malloc_on_thread(std::size_t size, std::size_t thread);

void setup(std::size_t thread_num);

void setup_thread(std::size_t thread_id, int node);

}// namespace memory
}// namespace nova
