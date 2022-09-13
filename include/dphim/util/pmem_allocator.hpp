#pragma once

#if __has_include(<libvmem.h>)

#define DPHIM_PMEM

#include <libpmem2.h>
#include <libvmem.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

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