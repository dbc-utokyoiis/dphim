#pragma once

#include <atomic>
#include <iostream>
#include <string>

namespace nova {

template<typename Item, Item *Item::*Next>
struct atomic_intrusive_list {

    atomic_intrusive_list() noexcept : m_head(nullptr) {}
    atomic_intrusive_list(const atomic_intrusive_list &other) = delete;
    atomic_intrusive_list(atomic_intrusive_list &&other) = delete;
    atomic_intrusive_list &operator=(const atomic_intrusive_list &other) = delete;
    atomic_intrusive_list &operator=(atomic_intrusive_list &&other) = delete;

    using size_type = long;

    void push_front(Item *item) noexcept {
        auto current_head = m_head.load(std::memory_order_relaxed);
        do {
            item->*Next = static_cast<Item *>(current_head);
        } while (not m_head.compare_exchange_weak(
                current_head, item,
                std::memory_order_release,
                std::memory_order_acquire));
        m_size.fetch_add(1, std::memory_order_release);
    }

    Item *pop_front() noexcept {
        auto current_head = m_head.load(std::memory_order_relaxed);
        Item *next = nullptr;
        do {
            if (!current_head) {
                return nullptr;
            } else {
                next = current_head->*Next;
            }
        } while (not m_head.compare_exchange_weak(
                current_head,
                next,
                std::memory_order_release,
                std::memory_order_acquire));
        m_size.fetch_sub(1, std::memory_order_release);
        return current_head;
    }

    std::size_t approx_size(std::memory_order m = std::memory_order_acquire) const noexcept {
        return m_size.load(m);
    }

    //    void merge_front(intrusive_forward_list<Item, Next> list) {
    //        m_approx_size.fetch_add(list.m_size, std::memory_order_relaxed);
    //        auto current_head = m_head.load(std::memory_order_relaxed);
    //        do {
    //            list.m_tail->*Next = current_head;
    //        } while (not m_head.compare_exchange_weak(
    //                current_head,
    //                list.m_head,
    //                std::memory_order_release,
    //                std::memory_order_acquire));
    //    }
    //
    //    intrusive_forward_list<Item, Next> pop_all_forward() {
    //        m_approx_size.store(0, std::memory_order_relaxed);
    //
    //        auto old_head = m_head.load(std::memory_order_relaxed);
    //        if (old_head == nullptr)
    //            return {};
    //        old_head = m_head.exchange(nullptr, std::memory_order_acquire);
    //        return intrusive_forward_list<Item, Next>::make_forward(static_cast<Item *>(old_head));
    //    }
    //
    //    intrusive_forward_list<Item, Next> pop_all_reversed() {
    //        m_approx_size.store(0, std::memory_order_relaxed);
    //
    //        auto old_head = m_head.load(std::memory_order_relaxed);
    //        if (old_head == nullptr)
    //            return {};
    //        old_head = m_head.exchange(nullptr, std::memory_order_acquire);
    //        return intrusive_forward_list<Item, Next>::make_reversed(static_cast<Item *>(old_head));
    //    }

    [[nodiscard]] bool empty(std::memory_order order = std::memory_order_acquire) const noexcept {
        return m_head.load(order) == nullptr;
    }

private:
    explicit atomic_intrusive_list(void *head) noexcept
        : m_head(head), m_size(0) {}
    std::atomic<Item *> m_head;
    std::atomic<std::size_t> m_size;
};

}// namespace nova
