#pragma once

#include <nova/config.hpp>
#include <nova/type_traits.hpp>

#include <variant>

namespace nova {

template<typename... Awaiter>
struct awaiter_variant {

    template<typename... Args>
    awaiter_variant(Args &&...args) : entity(std::forward<Args>(args)...) {}

    auto await_ready() const noexcept {
        return std::visit([](auto &&e) { return e.await_ready(); }, entity);
    }

    auto await_suspend(coro::coroutine_handle<> awaiting) noexcept {
        return std::visit([&](auto &&e) -> coro::coroutine_handle<> { return e.await_suspend(awaiting); }, entity);
    }

    auto await_resume() -> decltype(auto) {
        return std::visit([](auto &&e) { return e.await_resume(); }, entity);
    }

private:
    std::variant<Awaiter...> entity;
};

template<typename... Awaitable>
struct awaitable_variant : std::variant<Awaitable...> {

    template<typename... Args>
    awaitable_variant(Args &&...args) : entity(std::forward<Args>(args)...) {}

    using awaiter = awaiter_variant<std::remove_reference_t<typename nova::awaitable_traits<Awaitable>::awaiter_t>...>;

    auto operator co_await() & {
        return std::visit([](auto &&e) -> awaiter { return awaiter{nova::detail::get_awaiter(std::forward<decltype(e)>(e))}; }, entity);
    }

    auto operator co_await() && {
        return std::visit([](auto &&e) -> awaiter { return awaiter{nova::detail::get_awaiter(std::forward<decltype(e)>(e))}; }, std::move(entity));
    }

private:
    std::variant<Awaitable...> entity;
};


}// namespace nova