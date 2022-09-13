#pragma once

#include <tuple>

namespace nova {
inline namespace util {
template<typename T>
concept like_tuple = requires(T) {
    std::get<0>(std::declval<T>());
    std::tuple_size_v<std::remove_reference_t<T>>;
};

template<typename T>
concept like_vec = requires(T) {
    std::begin(std::declval<T>());
    std::end(std::declval<T>());
};

template<typename F, typename T>
void for_each(F &&f, T &&xs) requires like_tuple<T> {
    std::apply([&f](auto &&...xs) mutable { (f(std::forward<decltype(xs)>(xs)), ...); }, std::forward<T>(xs));
}

template<typename F, typename T>
void for_each(F &&f, T &&xs) requires like_vec<T> {
    std::for_each(std::begin(std::forward<T>(xs)), std::end(std::forward<T>(xs)), std::forward<F>(f));
}

}// namespace util
}// namespace nova