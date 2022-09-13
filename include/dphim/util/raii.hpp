#pragma once

#include <type_traits>
#include <utility>

namespace dphim {
template<class F>
struct RAII {

    explicit RAII(F &&f) noexcept
        : func(std::forward<F>(f)) {}

    ~RAII() {
        func();
    }

private:
    std::remove_reference_t<F> func;
};
}// namespace dphim