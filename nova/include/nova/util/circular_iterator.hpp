#pragma once

#include <iterator>

namespace nova {
inline namespace util {
template<typename ForwardIterator>
class CircularIterator {
    const ForwardIterator begin, end;
    ForwardIterator current;

public:
    using iterator_category = std::forward_iterator_tag;

    using value_type = typename std::iterator_traits<ForwardIterator>::value_type;

    using reference = typename std::add_lvalue_reference<value_type>::type;

    using const_reference = typename std::add_const<reference>::type;

    using pointer = typename std::iterator_traits<ForwardIterator>::pointer;

    using difference_type = typename std::iterator_traits<ForwardIterator>::difference_type;

    explicit CircularIterator(ForwardIterator begin, ForwardIterator end, ForwardIterator current)
        : begin(begin), end(end), current(current) {
    }

    CircularIterator &operator=(const ForwardIterator &iterator) {
        current = iterator;
        return *this;
    }

    operator ForwardIterator() { return current; }

    decltype(auto) operator*() { return *current; }
    decltype(auto) operator*() const { return *current; }

    auto &operator++() {
        if (current == end) {
            current = begin;
        } else if (++current == end) {
            current = begin;
        }

        return *this;
    }

    auto operator++(int) {
        auto copy = *this;
        operator++();
        return copy;
    }

    friend inline bool operator==(const CircularIterator &lhs, const CircularIterator &rhs) {
        return lhs.begin == rhs.begin && lhs.end == rhs.end && lhs.current == rhs.current;
    }
};

}// namespace util
}// namespace nova