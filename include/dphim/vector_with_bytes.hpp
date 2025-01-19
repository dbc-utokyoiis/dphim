#pragma once

#include <exception>
#include <utility>
#include <vector>

namespace dphim {

template<typename Container, bool is_const>
struct PrefixSumContainerIterator {
    using value_type = typename Container::value_type;
    using difference_type = std::ptrdiff_t;
    using pointer = std::conditional_t<is_const, const value_type *, value_type *>;
    using reference = std::conditional_t<is_const, const value_type &, value_type &>;
    using iterator_category = std::random_access_iterator_tag;

    PrefixSumContainerIterator(
            std::conditional_t<is_const, const Container *, Container *> vec,
            std::size_t idx) : vec(vec), idx(idx) {}
    PrefixSumContainerIterator(const PrefixSumContainerIterator &other)
        : vec(other.vec), idx(other.idx) {}
    PrefixSumContainerIterator &operator=(const PrefixSumContainerIterator &other) = default;
    PrefixSumContainerIterator &operator++() {
        ++idx;
        return *this;
    }
    PrefixSumContainerIterator operator++(int) {
        auto tmp = *this;
        ++idx;
        return tmp;
    }
    PrefixSumContainerIterator &operator--() {
        --idx;
        return *this;
    }
    PrefixSumContainerIterator operator--(int) {
        auto tmp = *this;
        --idx;
        return tmp;
    }

    auto &operator*() {
        return vec->storage[idx].first;
    }
    const auto &operator*() const {
        return vec->storage[idx].first;
    }
    pointer operator->() {
        return &vec->storage[idx].first;
    }
    const pointer operator->() const {
        return &vec->storage[idx].first;
    }
    bool operator==(const PrefixSumContainerIterator &rhs) const {
        return vec == rhs.vec && idx == rhs.idx;
    }
    bool operator!=(const PrefixSumContainerIterator &rhs) const {
        return !(*this == rhs);
    }
    bool operator<(const PrefixSumContainerIterator &rhs) const {
        return idx < rhs.idx;
    }
    bool operator>(const PrefixSumContainerIterator &rhs) const {
        return idx > rhs.idx;
    }
    bool operator<=(const PrefixSumContainerIterator &rhs) const {
        return idx <= rhs.idx;
    }
    bool operator>=(const PrefixSumContainerIterator &rhs) const {
        return idx >= rhs.idx;
    }
    PrefixSumContainerIterator &operator+=(std::size_t n) {
        idx += n;
        return *this;
    }
    PrefixSumContainerIterator &operator-=(std::size_t n) {
        idx -= n;
        return *this;
    }
    PrefixSumContainerIterator operator+(std::size_t n) const {
        return PrefixSumContainerIterator(vec, idx + n);
    }
    PrefixSumContainerIterator operator-(std::size_t n) const {
        return PrefixSumContainerIterator(vec, idx - n);
    }
    std::ptrdiff_t operator-(const PrefixSumContainerIterator &rhs) const {
        return idx - rhs.idx;
    }

    std::conditional_t<is_const, const Container *, Container *> vec;
    std::size_t idx;
};

template<typename Container, bool is_const>
struct PrefixSumRange {
    using iterator = PrefixSumContainerIterator<Container, is_const>;
    using const_iterator = PrefixSumContainerIterator<Container, true>;
    using value_type = typename Container::value_type;

    PrefixSumRange(Container *vec, std::size_t bg, std::size_t ed)
        : vec(vec), bg(bg), ed(ed) {}
    PrefixSumRange(iterator bg, iterator ed)
        : vec(bg.vec), bg(bg.idx), ed(ed.idx) {
        if (bg.vec != ed.vec) {
            throw std::runtime_error("different vector");
        }
    }

    auto begin() {
        return iterator(vec, bg);
    }
    auto end() {
        return iterator(vec, ed);
    }
    auto begin() const {
        return const_iterator(vec, bg);
    }
    auto end() const {
        return const_iterator(vec, ed);
    }
    auto size() const {
        return ed - bg;
    }
    auto empty() const {
        return bg == ed;
    }
    auto get_sum_value() const {
        if (ed < vec->size()) {
            return vec->storage[ed].second - vec->storage[bg].second;
        } else {
            return vec->sum_value - vec->storage[bg].second;
        }
    }
    auto calc_sum_value() const {
        std::size_t sum_bytes = 0;
        for (std::size_t i = bg; i < ed; ++i)
            sum_bytes += vec->storage[i].first.get_sum_value();
        return sum_bytes;
    }

private:
    Container *vec;
    std::size_t bg, ed;
};

template<typename T,
         typename SumT,
         SumT (T::*GetValue)() const,
         template<typename> typename Container = std::vector>
struct PrefixSumContainer {
    using iterator = PrefixSumContainerIterator<PrefixSumContainer, false>;
    using const_iterator = PrefixSumContainerIterator<PrefixSumContainer, true>;
    using value_type = T;

    PrefixSumContainer() = default;
    PrefixSumContainer(const PrefixSumContainer &) = default;
    PrefixSumContainer(PrefixSumContainer &&) noexcept = default;
    PrefixSumContainer &operator=(const PrefixSumContainer &) = default;
    PrefixSumContainer &operator=(PrefixSumContainer &&) noexcept = default;

    auto begin() {
        return iterator(this, 0);
    }
    auto end() {
        return iterator(this, storage.size());
    }
    auto begin() const {
        return const_iterator(this, 0);
    }
    auto end() const {
        return const_iterator(this, storage.size());
    }
    auto rbegin() {
        return std::make_reverse_iterator(end());
    }
    auto rend() {
        return std::make_reverse_iterator(begin());
    }
    std::size_t size() const {
        return storage.size();
    }
    auto sub_range(std::size_t bg, std::size_t ed) {
        return PrefixSumRange<T, false>(this, bg, ed);
    }
    auto sub_range(std::size_t bg, std::size_t ed) const {
        return PrefixSumRange<T, true>(this, bg, ed);
    }
    bool empty() const {
        return storage.empty();
    }
    void reserve(std::size_t n) {
        storage.reserve(n);
    }
    template<typename U, typename = std::enable_if_t<std::is_convertible_v<U, T>>>
    void push_back(U &&obj) {
        if (storage.empty()) {
            storage.emplace_back(std::forward<U>(obj), SumT(0));
            sum_value += (storage.back().first.*GetValue)();
        } else {
            storage.emplace_back(std::forward<U>(obj), sum_value);
            sum_value += (storage.back().first.*GetValue)();
        }
    }
    template<typename Pred>
    friend void erase_if(PrefixSumContainer &vec, Pred &&pred) {
        std::erase_if(vec.storage, [&pred](auto &p) { return pred(p.first); });
        vec.recalc();
    }
    auto get_sum_value() const {
        return sum_value;
    }
    void recalc() {
        SumT sum = 0;
        for (auto &[obj, b]: storage) {
            b = sum;
            sum += (obj.*GetValue)();
        }
        sum_value = sum;
    }
    template<typename Iter>
    auto erase(Iter bg, Iter ed) {
        auto ret = storage.erase(storage.begin() + bg.idx, storage.begin() + ed.idx);
        recalc();
        return ret;
    }

private:
    friend PrefixSumContainerIterator<PrefixSumContainer, true>;
    friend PrefixSumContainerIterator<PrefixSumContainer, false>;
    friend PrefixSumRange<PrefixSumContainer, true>;
    friend PrefixSumRange<PrefixSumContainer, false>;
    Container<std::pair<T, SumT>> storage;
    SumT sum_value = 0;
};


}// namespace dphim