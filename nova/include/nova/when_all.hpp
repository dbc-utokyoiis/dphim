#pragma once

#include "when_all.hpp"


#include <nova/config.hpp>
#include <nova/type_traits.hpp>
#include <nova/util/for_each.hpp>
#include <nova/util/return_value_or_void.hpp>
#include <nova/wait_group.hpp>

#include <future>
#include <vector>

namespace nova {

/// All of the tasks are wrapped by when_all_task.

template<typename T>
struct when_all_task;

struct when_all_done_notifier {

    auto await_ready() const noexcept { return false; }

    template<typename P>
    auto await_suspend(coro::coroutine_handle<P> h) const noexcept {
        h.promise().wg->done();
    }

    auto await_resume() const noexcept {}
};

template<typename T>
struct when_all_promise : return_value_or_void<T> {

    auto initial_suspend() const noexcept -> coro::suspend_always { return {}; }

    auto final_suspend() const noexcept -> when_all_done_notifier { return {}; }

    auto get_return_object() noexcept {
        return when_all_task<T>{coro::coroutine_handle<when_all_promise>::from_promise(*this)};
    }

    wait_group *wg = nullptr;
};

template<typename T>
struct when_all_task : coroutine_base<when_all_promise<T>> {

    using promise_type = when_all_promise<T>;

    friend promise_type;

    using coroutine_base<promise_type>::coroutine_base;

    bool is_done() const {
        return !this->coro || this->coro.done();
    }

    void start(wait_group &wg, bool count_up = true) {
        if (count_up) { wg.add(); }
        this->get_promise().wg = &wg;
        this->coro.resume();
    }

    auto result() & -> std::add_lvalue_reference_t<T> {
        return this->get_promise().result();
    }

    auto result() const & -> std::add_const_t<std::add_lvalue_reference_t<T>> {
        return this->get_promise().result();
    }

    auto result() && -> std::add_rvalue_reference_t<T> {
        return std::move(std::move(this->get_promise()).result());
    }
};

template<typename Awaitable>
auto make_when_all_task(Awaitable &&awaitable) {
    using R = typename awaitable_traits<Awaitable>::awaiter_result_t;
    using result_type = std::conditional_t<std::is_rvalue_reference_v<R>, std::remove_reference_t<R>, R>;
    //    using result_type = typename awaitable_traits<Awaitable>::awaiter_result_t;

    // NOTE:
    //  This lambda becomes return value of when_all() and refers to when_all()'s parameter ('awaitable').
    //  If the parameter is rvalue and the return value is extended lifetime beyond when_all(), 'awaitable' becomes a dangling reference.
    //  So, the type of 'awaitable' is not rvalue reference type.
    if constexpr (std::is_lvalue_reference_v<Awaitable &&>) {
        return [](auto &awaitable) -> when_all_task<result_type> {
            if constexpr (std::is_same_v<result_type, void>) {
                co_await awaitable;
            } else {
                co_return co_await awaitable;
            }
        }(awaitable);
    } else {
        return [](auto awaitable) -> when_all_task<result_type> {
            if constexpr (std::is_same_v<result_type, void>) {
                co_await std::move(awaitable);
            } else {
                co_return co_await std::move(awaitable);
            }
        }(std::move(awaitable));
    }
}

enum class launch {
    immediate,
    defer
};

template<typename TaskContainer>
struct [[nodiscard]] when_all_awaitable : TaskContainer {

    template<typename Tasks>
    explicit when_all_awaitable(Tasks &&tasks) requires(!std::is_same_v<std::remove_cvref_t<Tasks>, when_all_awaitable>)
        : TaskContainer(std::forward<Tasks>(tasks)) {}

    when_all_awaitable(const when_all_awaitable &) = delete;
    when_all_awaitable &operator=(const when_all_awaitable &) = delete;

    when_all_awaitable(when_all_awaitable &&) = default;
    when_all_awaitable &operator=(when_all_awaitable &&) = default;

    friend auto operator co_await(when_all_awaitable &awaitable) {
        struct awaiter {
            auto await_ready() const noexcept { return self->is_ready(); }
            auto await_suspend(coro::coroutine_handle<> cont) {
                self->start();
                self->try_await(cont);
            }
            auto await_resume() -> decltype(std::declval<when_all_awaitable &>().result()) {
                return self->result();
            }
            when_all_awaitable *self;
        };
        return awaiter{&awaitable};
    }

    friend auto operator co_await(when_all_awaitable &&awaitable) {
        struct awaiter {
            explicit awaiter(when_all_awaitable &&awaitable)
                : self(std::move(awaitable)) {}
            auto await_ready() const noexcept { return self.is_ready(); }
            auto await_suspend(coro::coroutine_handle<> cont) {
                self.start();
                self.try_await(cont);
            }
            auto await_resume() -> std::remove_reference_t<decltype(std::declval<when_all_awaitable &&>().result())> {
                return std::move(self).result();
            }

        private:
            when_all_awaitable self;
        };
        return awaiter(std::move(awaitable));
    }

    template<typename Task>
    void add_task(Task &&task, launch l = launch::defer) {
        if (l == launch::defer) {
            this->add_defer(make_when_all_task(std::forward<Task>(task)));
        } else if (l == launch::immediate) {
            this->add_immediate(make_when_all_task(std::forward<Task>(task)));
        } else {
            throw std::runtime_error("unsupported launch parameter");
        }
    }
};

template<typename R, typename C = std::vector<when_all_task<R>>>
struct VecTaskContainer {

    explicit VecTaskContainer(C &&tasks)
        : wg(std::make_unique<wait_group>()), defer_tasks(std::move(tasks)) {}

    VecTaskContainer(const VecTaskContainer &) = delete;
    VecTaskContainer &operator=(const VecTaskContainer &) = delete;
    VecTaskContainer(VecTaskContainer &&other) noexcept
        : wg(std::move(other.wg)),
          immediate_tasks(std::move(other.immediate_tasks)),
          defer_tasks(std::move(other.defer_tasks)) {
        other.wg = nullptr;
    }

protected:
    void add_immediate(when_all_task<R> &&task) {
        immediate_tasks.push_back(std::move(task));
        immediate_tasks.back().start(*wg);
    }

    void add_defer(when_all_task<R> &&task) {
        defer_tasks.push_back(std::move(task));
    }

    auto is_ready() const noexcept { return wg->is_ready(); }

    // return true if c is resumed immediately
    auto try_await(coro::coroutine_handle<> c) {
        return wg->try_await(c);
    }

    auto start() {
        if (!defer_tasks.empty()) {
            wg->add(defer_tasks.size());
            util::for_each([this](auto &&t) mutable { t.start(*wg, /* count_up= */ false); }, defer_tasks);
        }
    }

    using result_element_type = std::conditional_t<
            std::is_lvalue_reference_v<R>,
            std::reference_wrapper<std::remove_reference_t<R>>,
            std::remove_reference_t<R>>;

    auto result() & {
        if constexpr (!std::is_void_v<R>) {
            std::vector<std::reference_wrapper<std::remove_reference_t<R>>> res;
            res.reserve(immediate_tasks.size() + defer_tasks.size());
            for (auto &t: immediate_tasks)
                res.push_back(std::ref(t.result()));
            for (auto &t: defer_tasks)
                res.push_back(std::ref(t.result()));
            return res;
        }
    }

    auto result() const & {
        if constexpr (!std::is_void_v<R>) {
            std::vector<std::reference_wrapper<std::remove_reference_t<const R>>> res;
            res.reserve(immediate_tasks.size() + defer_tasks.size());
            for (auto &t: immediate_tasks)
                res.push_back(std::cref(t.result()));
            for (auto &t: defer_tasks)
                res.push_back(std::cref(t.result()));
            return res;
        }
    }

    auto result() && {
        if constexpr (std::is_lvalue_reference_v<R>) {
            std::vector<result_element_type> res;
            res.reserve(immediate_tasks.size() + defer_tasks.size());
            for (auto &&t: immediate_tasks)
                res.push_back(t.result());
            for (auto &&t: defer_tasks)
                res.push_back(t.result());
            return res;
        } else if constexpr (!std::is_void_v<R>) {
            std::vector<result_element_type> res;
            res.reserve(immediate_tasks.size() + defer_tasks.size());
            for (auto &&t: immediate_tasks)
                res.push_back(std::move(std::move(t).result()));
            for (auto &&t: defer_tasks)
                res.push_back(std::move(std::move(t).result()));
            return res;
        } else {
            static_assert(std::is_same_v<R, void>);
        }
    }

private:
    std::unique_ptr<wait_group> wg;
    C immediate_tasks;
    C defer_tasks;
};


template<typename... Tasks>
struct TupleTaskContainer {

    explicit TupleTaskContainer(std::tuple<Tasks...> &&tasks)
        : wg(std::make_unique<wait_group>()), tasks(std::move(tasks)) {}

    TupleTaskContainer(const TupleTaskContainer &) = delete;
    TupleTaskContainer(TupleTaskContainer &&) = default;

    auto is_ready() const noexcept { return wg->is_ready(); }

    auto try_await(coro::coroutine_handle<> c) const {
        wg->try_await(c);
    }

    auto start() {
        wg->add(std::tuple_size_v<decltype(tasks)>);
        util::for_each([this](auto &&t) mutable { t.start(*wg, /* count_up= */ false); }, tasks);
    }

    auto result() const &noexcept {
        return std::apply([](auto &&...t) {
            return std::forward_as_tuple(get_result_or_ignore(std::forward<decltype(t)>(t))...);
        },
                          tasks);
    }

    auto result() const &&noexcept {
        return std::apply([](auto &&...t) {
            return std::forward_as_tuple(get_result_or_ignore(std::forward<decltype(t)>(t))...);
        },
                          std::move(tasks));
    }

private:
    template<typename T>
    static auto get_result_or_ignore(T &&t) -> decltype(auto) {
        if constexpr (std::is_void_v<decltype(std::declval<T &&>().result())>) {
            return std::ignore;
        } else {
            return std::forward<T>(t).result();
        }
    }

    std::unique_ptr<wait_group> wg;
    std::tuple<Tasks...> tasks;
};

template<typename... Awaitable>
[[nodiscard]] auto when_all(Awaitable &&...awaitable) {
    using TaskContainer = TupleTaskContainer<decltype(make_when_all_task(std::declval<Awaitable>()))...>;
    return when_all_awaitable<TaskContainer>(std::make_tuple(make_when_all_task(std::forward<Awaitable>(awaitable))...));
}

template<typename Awaitable>
[[nodiscard]] auto when_all(std::vector<Awaitable> &&awaitable) {
    using R = typename awaitable_traits<Awaitable>::awaiter_result_t;
    using result_type = std::conditional_t<std::is_rvalue_reference_v<R>, std::remove_reference_t<R>, R>;
    using TaskContainer = VecTaskContainer<result_type>;
    std::vector<when_all_task<result_type>> tasks;
    tasks.reserve(awaitable.size());
    std::transform(
            std::make_move_iterator(awaitable.begin()),
            std::make_move_iterator(awaitable.end()),
            std::back_inserter(tasks),
            [](auto &&t) { return make_when_all_task(std::forward<decltype(t)>(t)); });
    return when_all_awaitable<TaskContainer>(std::move(tasks));
}

}// namespace nova