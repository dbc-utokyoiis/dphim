#pragma once

#include <iostream>
#include <memory>
#include <optional>
#include <vector>

#include "zero_is_option.hpp"

namespace dphim {

template<typename T,
         typename Alloc = std::conditional_t<std::is_integral_v<T>, std::allocator<ZeroIsOption<T>>, std::allocator<std::optional<T>>>>
struct IndexMap {
    using allocator_type = Alloc;
    using element_type = typename allocator_type::value_type;
    using BufferType = std::vector<element_type, allocator_type>;
    using key_type = std::size_t;
    using value_type = std::pair<std::size_t, T>;
    using mapped_type = T;

    explicit IndexMap(const allocator_type &a = allocator_type()) : buffer(a), size_(0) {}
    explicit IndexMap(std::size_t n, const allocator_type &a = allocator_type()) : buffer(n, std::nullopt, a), size_(n) {}
    IndexMap(const IndexMap &) = default;
    IndexMap(IndexMap &&) noexcept = default;
    IndexMap &operator=(const IndexMap &) = default;
    IndexMap &operator=(IndexMap &&) noexcept = default;

    struct const_pair {
        const std::size_t first;
        const mapped_type &second;
        const const_pair *operator->() const {
            return this;
        }
    };

    struct pair {
        const std::size_t first;
        mapped_type &second;
        operator const_pair() {
            return {first, second};
        }
        pair *operator->() {
            return this;
        }
        const pair *operator->() const {
            return this;
        }
    };

    struct iterator {
        explicit iterator() = default;
        explicit iterator(typename BufferType::iterator iter, BufferType &buffer)
            : iter(iter), ref(&buffer) {}

        iterator(const iterator &) = default;
        iterator &operator=(const iterator &other) = default;

        pair operator*() {
            return {static_cast<std::size_t>(iter - ref->begin()), **iter};
        }
        const_pair operator*() const {
            return {static_cast<std::size_t>(iter - ref->begin()), **iter};
        }
        pair operator->() {
            return {static_cast<std::size_t>(iter - ref->begin()), **iter};
        }
        const_pair operator->() const {
            return {static_cast<std::size_t>(iter - ref->begin()), **iter};
        }

        iterator &operator++() {
            do {
                ++iter;
            } while (iter < ref->end() && !iter->has_value());
            return *this;
        }

        iterator &operator--() {
            do {
                --iter;
            } while (ref->begin() < iter && !iter->has_value());
            return *this;
        }

        iterator operator++(int) {
            auto it = *iter;
            do {
                ++iter;
            } while (iter < ref->end() && !iter->has_value());
            return it;
        }

        iterator operator--(int) {
            auto it = *iter;
            do {
                --iter;
            } while (ref->begin() < iter && !iter->has_value());
            return it;
        }


        friend bool operator==(const iterator &lhs, const iterator &rhs) {
            return lhs.iter == rhs.iter;
        }

    private:
        typename BufferType::iterator iter;
        BufferType *ref;
    };

    struct const_iterator {
        explicit const_iterator(typename BufferType::const_iterator iter, const BufferType &buffer)
            : iter(iter), ref(&buffer) {}

        const_iterator(const const_iterator &) = default;
        const_iterator &operator=(const const_iterator &) = default;

        const_pair operator*() {
            return {static_cast<std::size_t>(iter - ref->begin()), **iter};
        }
        const_pair operator*() const {
            return {static_cast<std::size_t>(iter - ref->begin()), **iter};
        }
        const_pair operator->() const {
            return {static_cast<std::size_t>(iter - ref->begin()), **iter};
        }

        const_iterator &operator++() {
            do {
                ++iter;
            } while (iter < ref->end() && !iter->has_value());
            return *this;
        }

        const_iterator &operator--() {
            do {
                --iter;
            } while (ref->begin() < iter && !iter->has_value());
            return *this;
        }

        const_iterator operator++(int) {
            auto it = *iter;
            do {
                ++iter;
            } while (iter < ref->end() && !iter->has_value());
            return it;
        }

        const_iterator operator--(int) {
            auto it = *iter;
            do {
                --iter;
            } while (ref->begin() < iter && !iter->has_value());
            return it;
        }

        friend bool operator==(const const_iterator &lhs, const const_iterator &rhs) {
            return lhs.iter == rhs.iter;
        }

    private:
        typename BufferType::const_iterator iter;
        const BufferType *ref;
    };


    iterator begin() {
        auto it = buffer.begin();
        for (; it != buffer.end(); ++it)
            if (it->has_value())
                break;
        return iterator(it, buffer);
    }
    iterator end() { return iterator(buffer.end(), buffer); }
    const_iterator begin() const {
        auto it = buffer.begin();
        for (; it != buffer.end(); ++it)
            if (it->has_value())
                break;
        return const_iterator(it, buffer);
    }
    const_iterator end() const { return const_iterator(buffer.end(), buffer); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    T &at(std::size_t idx) {
        return *buffer[idx];
    }
    const T &at(std::size_t idx) const {
        return *buffer[idx];
    }

    std::pair<iterator, bool> insert(const value_type &v) {
        if (buffer.size() <= v.first) {
            buffer.resize(v.first + 1);
        }

        auto it = buffer.begin() + v.first;
        if (it->has_value()) {
            return {iterator(it, buffer), false};
        } else {
            *it = v;
            size_++;
            return {iterator(it, buffer), true};
        }
    }

    std::pair<iterator, bool> insert(value_type &&v) {
        if (buffer.size() <= v.first) {
            buffer.resize(v.first + 1);
        }

        auto it = buffer.begin() + v.first;
        if (it->has_value()) {
            return {iterator(it, buffer), false};
        } else {
            *it = std::move(v.second);
            size_++;
            return {iterator(it, buffer), true};
        }
    }

    template<typename... Args>
    auto emplace(Args &&...args) { return insert(value_type{std::forward<Args>(args)...}); }

    template<typename... Args>
    auto emplace(Args &&...args) const { return insert(value_type{std::forward<Args>(args)...}); }

    template<class M>
    std::pair<iterator, bool> insert_or_assign(const key_type &k, M &&obj) {
        if (buffer.size() <= k) {
            buffer.resize(k + 1);
        }

        auto it = buffer.begin() + k;
        if (it->has_value()) {
            it->template emplace(std::forward<M>(obj));
            return {iterator(it, buffer), false};
        } else {
            it->template emplace(std::forward<M>(obj));
            size_++;
            return {iterator(it, buffer), true};
        }
    }

    iterator find(const key_type &key) {
        if (buffer.size() < key)
            return end();

        auto it = buffer.begin() + key;
        if (it->has_value()) {
            return iterator(it, buffer);
        } else {
            return end();
        }
    }

    const_iterator find(const key_type &key) const {
        if (buffer.size() < key)
            return end();

        auto it = buffer.begin() + key;
        if (it->has_value()) {
            return iterator(it, buffer);
        } else {
            return end();
        }
    }

    void reserve(std::size_t n) {
        buffer.reserve(n);
    }

    std::size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    auto get_allocator() const {
        return buffer.get_allocator();
    }

private:
    BufferType buffer;
    std::size_t size_ = 0;
};
}// namespace dphim