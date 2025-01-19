#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#if __has_include(<libvmem.h> )

#define DPHIM_PMEM

#include <libpmem2.h>
#include <libvmem.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <unordered_map>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace dphim {

struct pmem_allocator {

    struct header_t {
        VMEM *vmem;
        alignas(std::max_align_t) char data[0];

        void *get_region() {
            return reinterpret_cast<char *>(this) + sizeof(header_t);
        }
    };

    explicit pmem_allocator(const char *path_to_devdax)
        : vmem(vmem_create(path_to_devdax, get_devdax_size(path_to_devdax))) {
        if (!vmem) {
            throw std::runtime_error(std::string("error: ") + vmem_errormsg());
        }
    }

    pmem_allocator(const pmem_allocator &) = delete;
    pmem_allocator(pmem_allocator &&) = delete;
    pmem_allocator &operator=(const pmem_allocator &) = delete;
    pmem_allocator &operator=(pmem_allocator &&) = delete;

    ~pmem_allocator() {
        if (vmem)
            vmem_delete(vmem);
    }

    void *alloc(std::size_t n) {
        auto *h = reinterpret_cast<header_t *>(vmem_malloc(vmem, sizeof(header_t) + n));
        if (!h)
            throw std::bad_alloc();
        h->vmem = vmem;
        return h->get_region();
    }

    static void dealloc(void *p) noexcept {
        auto *h = reinterpret_cast<header_t *>(reinterpret_cast<char *>(p) - sizeof(header_t));
        vmem_free(h->vmem, h);
    }

private:
    std::size_t get_devdax_size(const char *path) const {
        int fd = open(path, O_RDONLY);
        if (fd < 0)
            throw std::runtime_error(strerror(errno));

        struct pmem2_source *src;
        if (pmem2_source_from_fd(&src, fd) != 0)
            throw std::runtime_error(pmem2_errormsg());

        std::size_t size;
        if (pmem2_source_size(src, &size) != 0)
            throw std::runtime_error(pmem2_errormsg());
        close(fd);
        return size;
    }

    VMEM *vmem = nullptr;
};

}// namespace dphim
#endif

namespace dphim {

struct pmem_allocate_trait {

protected:
    bool is_debug_mode = false;

#ifdef DPHIM_PMEM
    inline static std::vector<std::shared_ptr<pmem_allocator>> pmem_allocators;

public:
    template<typename T>
    struct local_pmem_allocator {
        using value_type = T;
        using pointer = T *;
        pointer allocate(std::size_t n) {
            static thread_local auto p = [] {
                unsigned int cpu, node;
                getcpu(&cpu, &node);
                return std::make_pair(cpu, node);
            }();
            return reinterpret_cast<T *>(pmem_allocators.at(p.second)->alloc(n * sizeof(T)));
        }

        void deallocate(pointer p, [[maybe_unused]] std::size_t n) {
            dphim::pmem_allocator::dealloc(p);
        }
    };
#endif

public:
    void set_pmem_devdax_path([[maybe_unused]] int node, [[maybe_unused]] const std::string &path) {
#ifdef DPHIM_PMEM
        if (is_debug_mode) {
            std::cerr << "set pmem path: node=" << node << "path=" << path << std::endl;
        }
        if (pmem_allocators.size() <= std::size_t(node)) {
            pmem_allocators.resize(node + 1);
        }
        pmem_allocators[node] = std::make_shared<pmem_allocator>(path.c_str());
#else
        if (is_debug_mode) {
            std::cerr << "WARN: set_pmem_path() is called but DPHIM_PMEM is not defined." << std::endl;
        }
#endif
    }

    auto
    get_pmem_allocator([[maybe_unused]] std::optional<int> node = std::nullopt) {
#ifdef DPHIM_PMEM
        if (pmem_allocators.size() == 1) {
            return pmem_allocators.front();
        } else if (!pmem_allocators.empty()) {
            if (node) {
                return pmem_allocators.at(*node);
            } else {
                thread_local auto [cpu, node] = [] {
                    unsigned int cpu, node;
                    getcpu(&cpu, &node);
                    return std::make_pair(cpu, node);
                }();
                return pmem_allocators.at(node);
            }
        }
#endif
        throw std::runtime_error("pmem is unsupported");
    }
};

std::vector<std::string> get_pmem_devdax(std::optional<int> numa_node);
}// namespace dphim
