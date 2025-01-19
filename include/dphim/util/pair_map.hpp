#pragma once

#include <cassert>
#include <iostream>
#include <optional>
#include <utility>
#include <vector>

#include "zero_is_option.hpp"

#include <nova/task.hpp>
#include <nova/when_all.hpp>

namespace dphim {

template<typename T>
struct PairMap {
    using element_type = std::conditional_t<std::is_integral_v<T>, ZeroIsOption<T>, std::optional<T>>;
    using key_type = std::pair<std::size_t, std::size_t>;
    using mapped_type = T;

    using buffer_type = std::shared_ptr<element_type[]>;

    explicit PairMap(std::size_t part_num = 1)
        : m_buffers(part_num), m_size(0) {}

    explicit PairMap(PairMap &&other)
        : m_buffers(std::move(other.m_buffers)), m_size(other.m_size) {
        other.m_size = 0;
    }

    PairMap &operator=(const PairMap &other) {
        m_buffers = other.m_buffers;
        m_size = other.m_size;
        return *this;
    }

    void set_size(std::size_t size) {
        assert(m_size == 0);
        m_size = size;
    }

    void reserve(std::size_t pid = 0) {
        m_buffers[pid].reset(new element_type[sizeof(element_type) * part_size()]);
    }

    template<typename A, typename D>
    void reserve(std::size_t pid, A &&alloc_func, D &&deleter_factory) {
        auto *p = reinterpret_cast<element_type *>(alloc_func(sizeof(element_type) * part_size()));
        m_buffers[pid].reset(p, deleter_factory(sizeof(element_type) * part_size()));
    }

    void clear() {
        for (auto &m_buf: m_buffers) {
            for (std::size_t i = 0; i < part_size(); ++i)
                m_buf[i] = 0;
        }
    }

    void clear(std::size_t part_id) {
        for (std::size_t i = 0; i < part_size(); ++i)
            m_buffers[part_id][i] = 0;
    }

    T &at(const key_type &key) { return *at_raw(key); }

    const T &at(const key_type &key) const { return *at_raw(key); }

    [[nodiscard]] std::optional<std::pair<key_type, T &>> find(const key_type &key) {
        auto &ret = at_raw(key);
        return ret ? std::make_optional(std::pair<key_type, T &>{key, *ret}) : std::nullopt;
    }
    [[nodiscard]] std::optional<std::pair<key_type, const T &>> find(const key_type &key) const {
        auto &ret = at_raw(key);
        return ret ? std::make_optional(std::pair<key_type, const T &>{key, *ret}) : std::nullopt;
    }
    [[nodiscard]] constexpr static inline std::nullopt_t end() { return std::nullopt; }

    auto insert(const std::pair<key_type, mapped_type> &value) {
        auto &elm = at_raw(value.first);
        if (!elm.has_value())
            elm = value.second;
    }

    //    const buffer_type &data() const { return m_buffer; }
    //    buffer_type &data() { return m_buffer; }
    std::size_t size() const { return m_size; }
    std::size_t raw_size() const { return m_size * (m_size - 1) / 2; }
    std::size_t part_size() const { return (raw_size() - 1) / m_buffers.size() + 1; }
    std::size_t part_num() const { return m_buffers.size(); }

    const element_type &at_raw(const key_type &key) const {
        if (key.first == key.second) {
            throw std::out_of_range("key.first(" + std::to_string(key.first) + ") == key.second(" + std::to_string(key.second) + ")");
        } else if (key.first >= m_size || key.second >= m_size) {
            throw std::out_of_range("key.first(" + std::to_string(key.first) + ") or key.second(" + std::to_string(key.second) + ") is larger than m_size(" + std::to_string(m_size) + ")");
        }
        const auto &x = std::min(key.first, key.second);
        const auto &y = std::max(key.first, key.second);
        auto id = x * m_size - x * (x + 1) / 2 + y - x - 1;
        std::size_t pid = id / part_size();
        std::size_t id_in_p = id % part_size();
        if (id_in_p >= part_size())
            throw std::out_of_range("id in part");
        return m_buffers.at(pid)[id_in_p];
    }

    std::size_t get_pid(const key_type &key) {
        auto x = std::min(key.first, key.second);
        auto y = std::max(key.first, key.second);
        auto id = x * m_size - x * (x + 1) / 2 + y - x - 1;
        return id / part_size();
    }

    element_type &at_raw(const key_type &key) {
        return const_cast<element_type &>(static_cast<const PairMap *>(this)->at_raw(key));
    }

private:
    std::vector<buffer_type> m_buffers;
    std::size_t m_size = 0;
};

}// namespace dphim