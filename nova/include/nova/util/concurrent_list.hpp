#pragma once

#include <memory>
#include <optional>

#include <boost/lockfree/stack.hpp>

namespace nova {

namespace detail {
template<typename T>
struct linked_list_concurrent_stack {

    struct node_t;

    using node_ptr_t = std::shared_ptr<node_t>;

    struct node_t {
        T data;
        node_ptr_t next;
    };

private:
    explicit linked_list_concurrent_stack(void *head) noexcept
        : m_head(head) {}
    node_ptr_t m_head;
    std::atomic<int> m_size;

public:
    linked_list_concurrent_stack() noexcept : m_head(nullptr) {}
    linked_list_concurrent_stack(const linked_list_concurrent_stack &other) = delete;
    linked_list_concurrent_stack(linked_list_concurrent_stack &&other) = delete;
    linked_list_concurrent_stack &operator=(const linked_list_concurrent_stack &other) = delete;
    linked_list_concurrent_stack &operator=(linked_list_concurrent_stack &&other) = delete;

    using size_type = long;

    void push_front(T data) noexcept {
        auto new_node = node_ptr_t(new node_t{std::move(data), nullptr});
        while (true) {
            auto old_head = std::atomic_load_explicit(&m_head, std::memory_order_relaxed);
            new_node->next = old_head;
            if (/*m_head.compare_exchange_weak(old_head, new_node)*/
                std::atomic_compare_exchange_weak_explicit(
                        &m_head, &old_head, new_node,
                        std::memory_order_release, std::memory_order_relaxed)) {
                break;
            }
        }
        m_size.fetch_add(1, std::memory_order_relaxed);
    }

    template<typename F>
    std::size_t consume_once(F &&func) {
        if (auto p = pop_front()) {
            func(*p);
            return 1;
        } else {
            return 0;
        }
    }

    std::optional<T> pop_front() noexcept {
        while (true) {
            auto old_head = std::atomic_load_explicit(&m_head, std::memory_order_relaxed);
            if (old_head == nullptr)
                return std::nullopt;
            auto new_head = old_head->next;// old_head can be released here.
            if (std::atomic_compare_exchange_weak_explicit(
                        &m_head, &old_head, new_head,
                        std::memory_order_release, std::memory_order_relaxed)) {
                auto data = std::move(old_head->data);
                m_size.fetch_sub(1, std::memory_order_relaxed);
                return std::make_optional(std::move(data));
            }
        }
    }

    [[nodiscard]] bool empty(std::memory_order order = MEM_ORDER_ACQ) const noexcept {
        return std::atomic_load_explicit(&m_head, order) == nullptr;
    }

    std::size_t approx_size() {
        return m_size.load(std::memory_order_relaxed);
    }
};
}// namespace detail

// template<typename T>
// using concurrent_stack = detail::linked_list_concurrent_stack<T>;

template<typename T>
struct concurrent_stack {

    inline static constexpr std::size_t DEFAULT_MAIN_STACK_SIZE = 1024;

    explicit concurrent_stack(std::size_t main_stack_size = DEFAULT_MAIN_STACK_SIZE)
        : main_stack(main_stack_size) {}

    void push_front(const T &val) {
        if (main_stack.push(val))
            return;
        sub_stack.push_front(val);
        std::cout << "push sub" << std::endl;
    }

    std::optional<T> pop_front() {
        T ret;
        if (auto p = main_stack.pop(ret))
            return std::make_optional(std::move(ret));
        return sub_stack.pop_front();
    }


    template<typename F>
    std::size_t consume_once(F &&func) {
        if (auto v = pop_front()) {
            func(*v);
            return 1;
        } else {
            return 0;
        }
    }

    [[nodiscard]] bool empty(std::memory_order order = MEM_ORDER_ACQ) const noexcept {
        return main_stack.empty() && sub_stack.empty();
    }

private:
    boost::lockfree::stack<T> main_stack;
    detail::linked_list_concurrent_stack<T> sub_stack;
};

}// namespace nova