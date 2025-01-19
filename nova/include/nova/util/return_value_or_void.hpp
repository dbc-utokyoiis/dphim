#pragma once

#include <cstddef>
#include <utility>
#include <variant>

#include <iostream>

namespace nova {

struct result_is_empty : std::exception {
    [[nodiscard]] const char *what() const noexcept override {
        return "result is empty";
    }
};

struct coro_is_destructed : std::exception {
    [[nodiscard]] const char *what() const noexcept override {
        return "coroutine is already destructed";
    }
};

enum class result_state {
    empty,
    value,
    exception,
    destructed,
};

template<typename T>
struct return_value_or_void {

    return_value_or_void()
        : exception{nullptr} {}

    return_value_or_void(const return_value_or_void &) = delete;
    return_value_or_void &operator=(const return_value_or_void &) = delete;

    ~return_value_or_void() {
        if (state == result_state::exception) {
            exception.~exception_ptr();
        } else if (state == result_state::value) {
            data.~T();
        }
        state = result_state::destructed;
    }

    template<typename U>
    requires std::is_convertible_v<U, T> && std::is_constructible_v<T, U>
    void return_value(U &&v) noexcept(std::is_nothrow_constructible_v<T, U>) {
        ::new (std::addressof(data)) T(std::forward<U>(v));
        state = result_state::value;
    }

    void unhandled_exception() noexcept {
        ::new (std::addressof(exception)) std::exception_ptr(std::current_exception());
        state = result_state::exception;
    }

    auto result() & -> T & {
        check();
        return data;
    }

    auto result() const & -> const T & {
        check();
        return data;
    }

    auto result() && -> T && {
        check();
        return std::move(data);
    }

    auto current_state() const { return state; }

private:
    void check() const {
        switch (state) {
            case result_state::empty:
                throw result_is_empty{};
            case result_state::exception:
                std::rethrow_exception(exception);
            case result_state::destructed:
                throw coro_is_destructed{};
            case result_state::value:
                break;
        }
    }

    union {
        std::exception_ptr exception;
        T data;
    };

    result_state state = result_state::empty;
};

template<typename T>
struct return_value_or_void<T &> {

    return_value_or_void()
        : exception{nullptr} {}

    ~return_value_or_void() {
        if (state == result_state::exception) {
            exception.~exception_ptr();
        }
    }

    auto return_value(T &v) -> void {
        pointer = std::addressof(v);
        state = result_state::value;
    }

    void unhandled_exception() noexcept {
        ::new (std::addressof(exception)) std::exception_ptr(std::current_exception());
        state = result_state::exception;
    }

    auto result() -> T & {
        check();
        return *pointer;
    }

    auto result() const -> const T & {
        check();
        return *pointer;
    }

    auto current_state() const { return state; }

private:
    void check() const {
        switch (state) {
            case result_state::empty:
                throw result_is_empty{};
            case result_state::exception:
                std::rethrow_exception(exception);
            case result_state::destructed:
                throw coro_is_destructed{};
            case result_state::value:
                break;
        }
    }

    union {
        std::exception_ptr exception;
        T *pointer;
    };
    result_state state = result_state::empty;
};

template<typename T>
struct return_value_or_void<T &&> : return_value_or_void<std::remove_reference_t<T>> {};

//template<typename T>
//struct return_value_or_void<T &&> {
//
//    return_value_or_void()
//        : exception{nullptr} {}
//
//    ~return_value_or_void() {
//        if (state == result_state::exception) {
//            exception.~exception_ptr();
//        }
//    }
//
//    auto return_value(T &&v) -> void {
//        pointer = std::addressof(v);
//        state = result_state::value;
//    }
//
//    void unhandled_exception() noexcept {
//        ::new (std::addressof(exception)) std::exception_ptr(std::current_exception());
//        state = result_state::exception;
//    }
//
//    auto result() const -> T && {
//        check();
//        return std::move(*pointer);
//    }
//
//    auto current_state() const { return state; }
//
//private:
//    void check() const {
//        if (state == result_state::empty) {
//            throw result_is_empty{};
//        } else if (state == result_state::exception) {
//            std::rethrow_exception(exception);
//        }
//    }
//    union {
//        std::exception_ptr exception;
//        T *pointer;
//    };
//    result_state state = result_state::empty;
//};

template<>
struct return_value_or_void<void> {
    using return_type = void;

    return_value_or_void() = default;

    auto return_void() -> void {
        state = result_state::value;
    }

    void unhandled_exception() noexcept {
        ::new (std::addressof(exception)) std::exception_ptr(std::current_exception());
        state = result_state::exception;
    }

    auto result() const -> void {
        if (state == result_state::empty) {
            throw result_is_empty{};
        } else if (state == result_state::exception) {
            std::rethrow_exception(exception);
        }
    }

    auto current_state() const { return state; }

private:
    std::exception_ptr exception = nullptr;
    result_state state = result_state::empty;
};

}// namespace nova