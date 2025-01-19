#pragma once

#include <type_traits>
#include <utility>

namespace nova {

template<typename F>
struct Raii {
    template<typename F2>
    Raii(F2 &&callback)
        : callback(std::forward<F2>(callback)) {}

    ~Raii() {
        callback();
    }

private:
    F callback;
};

template<typename F>
auto make_raii(F &&func) {
    return Raii<std::remove_reference_t<F>>(std::forward<F>(func));
}

}// namespace nova