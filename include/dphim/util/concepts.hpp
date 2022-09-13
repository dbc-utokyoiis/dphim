#pragma once

#if __has_include(<concepts>)
#include <concepts>
#elif __has_include(<experimental/concepts>)
#include <experimental/concepts>
namespace std {
using namespace std::experimental;
}
#else
#error no include file for concepts
#endif

#include <type_traits>
#include <utility>

namespace FHMUtil {

template<class T>
concept LikeMap = requires(T v) {
    typename T::key_type;
    typename T::mapped_type;
    //    typename T::iterator;
    //    typename T::const_iterator;

    //    { v.empty() } -> std::same_as<bool>;
    //    { v.size() } -> std::same_as<std::size_t>;
    //    { v.begin() } -> std::same_as<typename T::iterator>;
    //    { v.end() } -> std::same_as<typename T::iterator>;
    //    { v.cbegin() } -> std::same_as<typename T::const_iterator>;
    //    { v.cend() } -> std::same_as<typename T::const_iterator>;
    //
    //    { v.find(std::declval<typename T::key_type>()) } -> std::same_as<typename T::iterator>;
    //
    //    std::declval<typename T::iterator>()->first;
    //    std::declval<typename T::iterator>()->second;
};

namespace detail {
template<typename T>
struct is_pair : std::false_type {};
template<typename T1, typename T2>
struct is_pair<std::pair<T1, T2>> : std::true_type {};
}// namespace detail

template<class T>
concept KeyIsPair = LikeMap<T> && requires(T v) {
    requires detail::is_pair<typename T::key_type>::value;
    requires std::is_same_v<typename T::key_type::first_type, typename T::key_type::second_type>;
};

}// namespace FHMUtil