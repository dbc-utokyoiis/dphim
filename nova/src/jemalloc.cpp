#include <nova/jemalloc.hpp>

#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>
#include <unistd.h>

#include <array>
#include <iostream>
#include <mutex>
#include <sstream>
#include <vector>

namespace nova {
inline namespace memory {
#ifdef HAS_JEMALLOC
namespace {
std::vector<unsigned> thread_arenas;
std::mutex mtx;

template<std::size_t Node>
void *extent_alloc_node_(extent_hooks_t * /*extent_hooks*/, void *new_addr, size_t size, size_t /*alignment*/, bool *zero, bool *commit, unsigned /*arena_ind*/) {
    void *ptr = mmap(new_addr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        return nullptr;
    }

    unsigned long nodemask = 1 << Node;
    if (mbind(ptr, size, MPOL_BIND, &nodemask, sizeof(nodemask) * 8, 0) != 0) {
        perror("mbind failed");
        munmap(ptr, size);
        return nullptr;
    }
    if (*zero) {
        memset(ptr, 0, size);
    }
    if (*commit) {
        for (size_t i = 0; i < size; i += sysconf(_SC_PAGESIZE)) {
            volatile char *temp = (char *) ptr + i;
            *temp = 0;
        }
    }
    return ptr;
}

std::array<extent_alloc_t *, 8> extent_alloc_node = {
        extent_alloc_node_<0>,
        extent_alloc_node_<1>,
        extent_alloc_node_<2>,
        extent_alloc_node_<3>,
        extent_alloc_node_<4>,
        extent_alloc_node_<5>,
        extent_alloc_node_<6>,
        extent_alloc_node_<7>,
};
}// namespace

template<typename T, typename... Names>
T read_mallctl(Names &&...names) {
    std::lock_guard guard(mtx);
    T val;
    std::size_t size = sizeof(T);
    std::stringstream ss;
    ((ss << names), ...);
    mallctl(ss.str().c_str(), &val, &size, NULL, 0);
    return val;
}

template<typename T, typename... Names>
void write_mallctl(T val, Names &&...names) {
    std::lock_guard guard(mtx);
    std::stringstream ss;
    ((ss << names), ...);
    mallctl(ss.str().c_str(), nullptr, nullptr, &val, sizeof(T));
}

void setup(std::size_t thread_num) {
    thread_arenas.resize(thread_num, MALLOCX_ARENA(0));
}

void setup_thread(std::size_t thread_id, int node) {
    auto corresponding_arena = read_mallctl<unsigned>("thread.arena");
    thread_arenas[thread_id] = corresponding_arena;
    extent_hooks_t hooks = {};
    hooks.alloc = extent_alloc_node[node],
    write_mallctl(hooks, "arena.", corresponding_arena, ".extent_hooks");
}

void *malloc_on_thread(std::size_t n, std::size_t thread) {
    if (thread >= thread_arenas.size()) {
        std::cerr << "bad alloc" << std::endl;
        std::abort();
    } else {
        return mallocx(n, MALLOCX_ARENA(thread_arenas[thread]));
    }
}

#else

void *malloc_on_node(std::size_t n, int) {
    return std::malloc(n);
}

void setup_node_spec_arena() {
    throw std::runtime_error("jemalloc is not available");
}

void set_numa_binding([[maybe_unused]] int node) {
    throw std::runtime_error("jemalloc is not available");
}

#endif

}// namespace memory
}// namespace nova
