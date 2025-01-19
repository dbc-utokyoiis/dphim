#pragma once


#include <atomic>
#include <mutex>
#include <vector>

#include <nova/config.hpp>

#define USE_NONBLOCKING_LIST 1

namespace nova {

template<typename Item, Item *Item::*Next>
struct nonblocking_intrusive_list {

    nonblocking_intrusive_list() noexcept : m_head(nullptr) {}
    nonblocking_intrusive_list(const nonblocking_intrusive_list &other) = delete;
    nonblocking_intrusive_list(nonblocking_intrusive_list &&other) = delete;
    nonblocking_intrusive_list &operator=(const nonblocking_intrusive_list &other) = delete;
    nonblocking_intrusive_list &operator=(nonblocking_intrusive_list &&other) = delete;

    using size_type = long;

    void push_front(Item *item) noexcept {
        while (true) {
            auto old_head = m_head.load();
            item->*Next = old_head;
            if (m_head.compare_exchange_strong(old_head, item)) {
                break;
            }
        }
    }

    template<typename F>
    std::size_t consume_all(F &&func) {
        auto head = m_head.load();
        std::size_t consume_count = 0;
        if (!head) {
            return consume_count;
        }
        auto new_head = nullptr;
        if (m_head.compare_exchange_strong(head, new_head)) {
            while (auto p = head) {
                head = head->*Next;
                func(p);
                ++consume_count;
            }
        }
        return consume_count;
    }

    template<typename F>
    std::size_t consume_once(F &&func) {
        if (auto p = pop_front()) {
            func(p);
            return 1;
        } else {
            return 0;
        }
    }

    Item *pop_front() noexcept {
        while (true) {
            auto old_head = m_head.load(std::memory_order_relaxed);
            if (old_head == nullptr)
                return nullptr;
            auto new_head = old_head->*Next;// old_head can be released here.
            if (m_head.compare_exchange_weak(old_head, new_head)) {
                return old_head;
            }
        }
    }

    [[nodiscard]] bool empty(std::memory_order order = MEM_ORDER_ACQ) const noexcept {
        return m_head.load(order) == nullptr;
    }

private:
    explicit nonblocking_intrusive_list(void *head) noexcept
        : m_head(head) {}
    std::atomic<Item *> m_head;
};

template<typename Item, Item *Item::*Next>
struct blocking_intrusive_list {
    blocking_intrusive_list() = default;
    blocking_intrusive_list(const blocking_intrusive_list &) = delete;
    blocking_intrusive_list(blocking_intrusive_list &&) noexcept = delete;
    blocking_intrusive_list &operator=(const blocking_intrusive_list &) = delete;
    blocking_intrusive_list &operator=(blocking_intrusive_list &&) noexcept = delete;

    // void push_front(Item *item) noexcept {
    //     std::lock_guard<std::mutex> lk(mtx);
    //     if (stack.empty()) {
    //         item->*Next = nullptr;
    //     } else {
    //         item->*Next = stack.back();
    //     }
    //     stack.push_back(item);
    // }
    //
    // Item *pop_front() noexcept {
    //     std::lock_guard<std::mutex> lk(mtx);
    //     if (stack.empty()) {
    //         return nullptr;
    //     } else {
    //         auto ret = stack.back();
    //         stack.pop_back();
    //         return ret;
    //     }
    // }
    //
    // [[nodiscard]] bool empty(std::memory_order = std::memory_order_acquire) {
    //     std::lock_guard<std::mutex> lk(m_mtx);
    //     return stack.empty();
    // }

    void push_front(Item *item) noexcept {
        if (!item)
            return;
        std::lock_guard lk(m_mtx);
        auto old_head = m_head;
        auto new_head = item;
        new_head->*Next = old_head;
        m_head = new_head;
        m_size += 1;
    }

    Item *pop_front() {
        std::lock_guard lk(m_mtx);
        auto old_head = m_head;
        if (!old_head)
            return nullptr;
        m_head = old_head->*Next;
        m_size -= 1;
        return old_head;
    }

    std::size_t approx_size() {
        std::lock_guard lk(m_mtx);
        return m_size;
    }

    bool empty() {
        std::lock_guard lk(m_mtx);
        return m_head == nullptr;
    }

private:
    // std::vector<Item *> stack;
    Item *m_head = nullptr;
    std::size_t m_size = 0;
    std::mutex m_mtx;
};


#if USE_NONBLOCKING_LIST
template<typename Item, Item *Item::*Next>
using parallel_intrusive_list = nonblocking_intrusive_list<Item, Next>;
#else
template<typename Item, Item *Item::*Next>
using parallel_intrusive_list = blocking_intrusive_list<Item, Next>;
#endif

}// namespace nova
