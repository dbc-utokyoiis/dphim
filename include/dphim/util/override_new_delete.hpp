#pragma once

#include <cstdlib>

#define OVERRIDE_NEW_DELETE

namespace dphim {
inline namespace util {
using malloc_t = void *(*) (std::size_t);
using free_t = void (*)(void *);

inline malloc_t malloc_func = std::malloc;
inline free_t free_func = std::free;
}// namespace util
}// namespace dphim

inline void *operator new(std::size_t n) {
    return dphim::util::malloc_func(n);
}

inline void operator delete(void *p) noexcept {
    dphim::util::free_func(p);
}

inline void operator delete(void *p, [[maybe_unused]] std::size_t n) noexcept {
    dphim::util::free_func(p);
}

inline void *operator new[](std::size_t n) {
    return dphim::util::malloc_func(n);
}

inline void operator delete[](void *p) noexcept {
    dphim::util::free_func(p);
}

inline void operator delete[](void *p, [[maybe_unused]] std::size_t n) noexcept {
    dphim::util::free_func(p);
}
