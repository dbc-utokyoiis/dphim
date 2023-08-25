#pragma once

#include <memory>
#include <numeric>
#include <vector>

#include <nova/immediate.hpp>
#include <nova/task.hpp>
#include <nova/variant.hpp>
#include <nova/when_all.hpp>

namespace dphim {

template<typename T, typename A = std::allocator<T>>
struct dynamic_array {
    explicit dynamic_array(std::size_t n, A alloc = A())
        : data(nullptr), len(n), alloc(alloc) {
        if (len > 0) {
            data = std::allocator_traits<A>::allocate(alloc, len * sizeof(T));
            for (std::size_t i = 0; i < len; ++i)
                std::allocator_traits<A>::construct(alloc, data + i);
        }
    }

    explicit dynamic_array(std::size_t n, const T &init, A alloc = A())
        : data(nullptr), len(n), alloc(alloc) {
        if (len > 0) {
            data = std::allocator_traits<A>::allocate(alloc, len * sizeof(T));
            for (std::size_t i = 0; i < len; ++i)
                std::allocator_traits<A>::construct(alloc, data + i, init);
        }
    }

    dynamic_array(dynamic_array &&other) noexcept
        : data(std::exchange(other.data, nullptr)),
          len(std::exchange(other.len, 0)),
          alloc(std::move(other.alloc)) {}

    ~dynamic_array() {
        if constexpr (!std::is_trivially_destructible_v<T>) {
            for (std::size_t i = 0; i < len; ++i)
                std::allocator_traits<A>::destroy(alloc, data + i);
        }
        std::allocator_traits<A>::deallocate(alloc, data, len * sizeof(T));
    }

    T *begin() { return data; }
    T *end() { return data + len; }
    const T *begin() const { return data; }
    const T *end() const { return data + len; }
    const T *cbegin() { return data; }
    const T *cend() { return data + len; }

    T &front() { return data[0]; }
    T &back() { return data[len - 1]; }
    const T &front() const { return data[0]; }
    const T &back() const { return data[len - 1]; }

    T &operator[](std::size_t n) { return data[n]; }
    const T &operator[](std::size_t n) const { return data[n]; }

    [[nodiscard]] std::size_t size() const noexcept { return len; }

private:
    T *data;
    std::size_t len;
    [[no_unique_address]] A alloc;
};

template<typename Iter, typename Partitions>
struct parted_iter {
private:
    decltype(auto) part(std::ptrdiff_t i) { return (*partitions)[i]; }
    decltype(auto) part(std::ptrdiff_t i) const { return (*partitions)[i]; }

    decltype(auto) part() { return part(partition_id); }
    decltype(auto) part() const { return part(partition_id); }

public:
    using difference_type = typename Iter::difference_type;
    using value_type = typename Iter::value_type;
    using pointer = typename Iter::pointer;
    using reference = typename Iter::reference;
    using iterator_category = typename std::random_access_iterator_tag;

    parted_iter(Iter current, std::size_t partition_id, Partitions &partitions)
        : current(current), partition_id(static_cast<std::ptrdiff_t>(partition_id)), partitions(&partitions) {}

    parted_iter(const parted_iter &other) = default;
    parted_iter &operator=(const parted_iter &other) = default;

    decltype(auto) operator*() { return *current; }
    decltype(auto) operator*() const { return *current; }

    parted_iter operator++() {
        if (++current == part().end()) {
            while (partition_id < static_cast<std::ptrdiff_t>(partitions->size()) && (*partitions)[++partition_id].empty())
                ;
            current = (partition_id < static_cast<std::ptrdiff_t>(partitions->size()))
                              ? (*partitions)[partition_id].begin()
                              : partitions->back().end();
        }
        return *this;
    }

    parted_iter operator++(int) {
        auto tmp = *this;
        ++(*this);
        return tmp;
    }

    parted_iter operator--() {
        if (current-- == part().begin()) {
            while (partition_id >= 0 && part(--partition_id).empty())
                ;
            current = (partition_id >= 0)
                              ? std::prev(part().end())
                              : partitions->front().begin();
        }
        return *this;
    }

    parted_iter operator--(int) {
        auto tmp = *this;
        --(*this);
        return tmp;
    }

    parted_iter &operator+=(std::ptrdiff_t d) {
        if (d > 0) {
            while (d >= std::distance(current, part().end())) {
                d -= std::distance(current, part().end());
                ++partition_id;
                current = part().begin();
            }
            current += d;
        } else if (d < 0) {
            (*this) -= -d;
        }
        return *this;
    }

    parted_iter &operator-=(std::ptrdiff_t d) {
        if (d > 0) {
            while (d > std::distance(part().begin(), current)) {
                d -= std::distance(part().begin(), current);// d > 0
                --partition_id;
                current = part().end();
            }
            current -= d;
        } else if (d < 0) {
            (*this) += -d;
        }
        return *this;
    }

    parted_iter operator+(std::ptrdiff_t d) const {
        auto tmp = *this;
        return tmp += d;
    }

    parted_iter operator-(std::ptrdiff_t d) const {
        auto tmp = *this;
        return tmp -= d;
    }

    friend std::ptrdiff_t operator-(const parted_iter &lhs, const parted_iter &rhs) {
        if (lhs.partition_id < rhs.partition_id) {
            return -(rhs - lhs);
        } else if (rhs.partition_id < lhs.partition_id) {
            auto &partitions = *lhs.partitions;
            auto &part_l = lhs.part();
            auto &part_r = rhs.part();
            std::ptrdiff_t diff = 0;
            diff += (part_r.end() - rhs.current);
            for (auto i = rhs.partition_id + 1; i < lhs.partition_id - 1; ++i)
                diff += partitions[i].size();
            diff += (lhs.current - part_l.begin());
            return diff;
        } else {
            return lhs.current - rhs.current;
        }
    }


    friend bool operator==(const parted_iter &lhs, const parted_iter &rhs) {
        return lhs.current == rhs.current;
    }

    friend bool operator!=(const parted_iter &lhs, const parted_iter &rhs) {
        return !(lhs == rhs);
    }

    friend bool operator<(const parted_iter &lhs, const parted_iter &rhs) {
        if (lhs.partition_id == rhs.partition_id) {
            return lhs.current < rhs.current;
        } else {
            return lhs.partition_id < rhs.partition_id;
        }
    }

    friend bool operator>(const parted_iter &lhs, const parted_iter &rhs) {
        return rhs < lhs;
    }

    friend bool operator>=(const parted_iter &lhs, const parted_iter &rhs) {
        return !(lhs < rhs);
    }

    friend bool operator<=(const parted_iter &lhs, const parted_iter &rhs) {
        return rhs >= lhs;
    }

    friend std::ostream &operator<<(std::ostream &os, parted_iter const &v) {
        return os << v.partition_id << ":" << std::distance(v.part().begin(), v.current);
    }

private:
    Iter current;
    std::ptrdiff_t partition_id;
    Partitions *partitions;
};

template<typename T, typename A = std::allocator<T>>
struct parted_vec {
    using value_type = T;
    using reference = T &;
    using const_reference = const T &;

    using partition_type = std::vector<T, A>;
    using partitions_type = dynamic_array<partition_type>;

    using iterator = parted_iter<typename std::vector<T, A>::iterator, partitions_type>;
    using const_iterator = parted_iter<typename std::vector<T, A>::const_iterator, const partitions_type>;

    explicit parted_vec(std::size_t n) : data(n) {}

    parted_vec(const parted_vec &) = delete;
    parted_vec(parted_vec &&) noexcept = default;
    parted_vec &operator=(const parted_vec &) = delete;
    parted_vec &operator=(parted_vec &&) noexcept = default;

    void push_back(const T &val, std::size_t partition_id) {
        data[partition_id].push_back(val);
    }

    void push_back(T &&val, std::size_t partition_id) {
        data[partition_id].push_back(std::move(val));
    }

    iterator begin() { return iterator(data.front().begin(), 0, data); }
    iterator end() { return iterator(data.back().end(), data.size() - 1, data); }
    const_iterator begin() const { return const_iterator(data.front().begin(), 0, data); }
    const_iterator end() const { return const_iterator(data.back().end(), data.size() - 1, data); }
    const_iterator cbegin() { return const_iterator(data.front().cbegin(), 0, data); }
    const_iterator cend() { return const_iterator(data.back().cend(), data.size() - 1, data); }

    partition_type &get(std::size_t i) { return data[i]; }
    const partition_type &get(std::size_t i) const { return data[i]; }

    decltype(auto) operator[](std::size_t i) { return *(begin() + i); }
    decltype(auto) operator[](std::size_t i) const { return *(begin() + i); }

    std::size_t size() const {
        return std::accumulate(std::begin(data), std::end(data), 0, [](std::size_t v, auto &&p) { return v + p.size(); });
    }

    std::size_t partition_size() const { return data.size(); }

    partitions_type &partitions() { return data; }
    const partitions_type &partitions() const { return data; }

    auto part_begin() { return data.begin(); }
    auto part_end() { return data.end(); }
    auto part_begin() const { return data.begin(); }
    auto part_end() const { return data.end(); }

    template<typename Comp>
    friend void erase_if(parted_vec<T, A> &vec, Comp &&comp) {
        using std::erase_if;
        for (auto &part: vec.partitions())
            erase_if(part, comp);
    }

private:
    partitions_type data;
};

template<typename F, typename... Ts, typename Arg0, typename... Args>
decltype(auto) invocable_invoke_impl(F &&f, std::tuple<Ts...> &&t, Arg0 &&arg0, Args &&...args) {
    if constexpr (std::is_invocable_v<F, Ts...>) {
        return std::apply(std::forward<F>(f), std::move(t));
    } else {
        return invocable_invoke_impl(std::forward<F>(f),
                                     std::tuple_cat(std::move(t), std::forward_as_tuple(std::forward<Arg0>(arg0))),
                                     std::forward<Args>(args)...);
    }
}

template<typename F, typename... Ts>
decltype(auto) invocable_invoke_impl(F &&f, std::tuple<Ts...> &&t) {
    return std::apply(std::forward<F>(f), std::move(t));
}

template<typename F, typename... Args>
decltype(auto) invocable_invoke(F &&f, Args &&...args) {
    return invocable_invoke_impl(std::forward<F>(f), std::tuple<>{}, std::forward<Args>(args)...);
}

template<typename F, typename... Args>
using invocable_invoke_result_t = decltype(invocable_invoke(std::declval<F>(), std::declval<Args>()...));

template<typename T, typename A, typename F, typename Sched>
auto for_each_batched(parted_vec<T, A> &vec, F &&f, Sched &&sched, std::size_t batch_size)
    requires std::is_same_v<std::invoke_result_t<F, T &>, void>
{
    std::vector<nova::task<>> tasks;
    std::size_t part_id = 0;
    for (auto &part: vec.partitions()) {
        auto bg = part.begin();
        for (std::size_t j = 0; j < part.size(); j += batch_size) {
            tasks.emplace_back([](auto bg, auto ed, auto part_id, auto &&f, auto &&sched) -> nova::task<> {
                co_await invocable_invoke(sched, part_id);
                for (auto it = bg; it < ed; ++it)
                    invocable_invoke(f, *it, part_id);
            }(bg + j, bg + std::min(j + batch_size, part.size()), part_id, f, sched));
            //            std::cout << part_id << " [" << j << ", " << std::min(j + batch_size, part.size()) << ")" << std::endl;
        }
        ++part_id;
    }
    //    std::cout << __PRETTY_FUNCTION__ << ": " << tasks.size() << std::endl;
    return nova::when_all(std::move(tasks));
}

template<typename T, typename A, typename F, typename Sched>
auto for_each_batched(const parted_vec<T, A> &vec, F &&f, Sched &&sched, std::size_t batch_size)
    requires std::is_same_v<std::invoke_result_t<F, const T &>, void>
{
    std::vector<nova::task<>> tasks;
    for (std::size_t part_id = 0; part_id < vec.partitions().size(); ++part_id) {
        auto &part = vec.partitions()[part_id];
        auto bg = part.begin();
        for (std::size_t j = 0; j < part.size(); j += batch_size)
            tasks.emplace_back([](auto bg, auto ed, auto part_id, auto &&f, auto &&sched) -> nova::task<> {
                co_await invocable_invoke(sched, part_id);
                for (auto it = bg; it < ed; ++it)
                    invocable_invoke(f, *it, part_id);
            }(bg + j, bg + std::min(j + batch_size, part.size()), part_id, f, sched));
    }
    return nova::when_all(std::move(tasks));
}

template<typename T, typename A, typename F, typename Sched>
auto partition_map(parted_vec<T, A> &vec, F &&f, Sched &&sched) {
    using R = invocable_invoke_result_t<F, typename parted_vec<T, A>::partition_type &, std::size_t>;
    std::vector<nova::task<R>> tasks;
    for (std::size_t i = 0; i < vec.partitions().size(); ++i) {
        auto &part = vec.partitions()[i];
        tasks.emplace_back([](auto &part, auto part_id, auto &&f, auto &&sched) -> nova::task<R> {
            co_await invocable_invoke(sched, part_id);
            co_return invocable_invoke(f, part, part_id);
        }(part, i, f, sched));
    }
    return nova::when_all(std::move(tasks));
}

template<typename T, typename A, typename F, typename Sched>
auto partition_map(const parted_vec<T, A> &vec, F &&f, Sched &&sched) {
    using R = invocable_invoke_result_t<F, const typename parted_vec<T, A>::partition_type &, std::size_t>;
    std::vector<nova::task<R>> tasks;
    for (std::size_t i = 0; i < vec.partitions().size(); ++i) {
        auto &part = vec.partitions()[i];
        tasks.emplace_back([](auto &part, auto part_id, auto &&f, auto &&sched) -> nova::task<R> {
            co_await invocable_invoke(sched, part_id);
            co_return invocable_invoke(f, part, part_id);
        }(part, i, f, sched));
    }
    return nova::when_all(std::move(tasks));
}

template<typename T, typename A, typename F, typename Sched, typename Cond>
auto partition_map(const parted_vec<T, A> &vec, F &&f, Sched &&sched, Cond &&cond) {
    using R = invocable_invoke_result_t<F, const typename parted_vec<T, A>::partition_type &, std::size_t>;
    std::vector<nova::awaitable_variant<nova::task<R>, nova::immediate<R>>> tasks;
    for (std::size_t i = 0; i < vec.partitions().size(); ++i) {
        auto &part = vec.partitions()[i];
        if (cond(part, i)) {
            tasks.emplace_back([](auto &part, auto part_id, auto &&f, auto &&sched) -> nova::task<R> {
                co_await invocable_invoke(sched, part_id);
                co_return invocable_invoke(f, part, part_id);
            }(part, i, f, sched));
        } else {
            tasks.emplace_back(nova::immediate<R>{f(part)});
        }
    }
    return nova::when_all(std::move(tasks));
}

}// namespace dphim