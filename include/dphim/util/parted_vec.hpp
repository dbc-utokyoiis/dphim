#pragma once

#include <memory>
#include <numeric>
#include <sstream>
#include <vector>

#include <nova/immediate.hpp>
#include <nova/task.hpp>
#include <nova/variant.hpp>
#include <nova/when_all.hpp>

namespace dphim {

// 添え字を表すカスタムイテレーター
class IndexIterator {
public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = int;
    using difference_type = int;
    using pointer = int *;
    using reference = int &;

    explicit IndexIterator(int index) : index(index) {}

    int operator*() const { return index; }
    IndexIterator &operator++() {
        ++index;
        return *this;
    }
    IndexIterator operator++(int) {
        IndexIterator tmp = *this;
        ++(*this);
        return tmp;
    }
    IndexIterator &operator--() {
        --index;
        return *this;
    }
    IndexIterator operator--(int) {
        IndexIterator tmp = *this;
        --(*this);
        return tmp;
    }
    IndexIterator &operator+=(difference_type n) {
        index += n;
        return *this;
    }
    IndexIterator &operator-=(difference_type n) {
        index -= n;
        return *this;
    }
    IndexIterator operator+(difference_type n) const { return IndexIterator(index + n); }
    IndexIterator operator-(difference_type n) const { return IndexIterator(index - n); }
    difference_type operator-(const IndexIterator &other) const { return index - other.index; }
    bool operator==(const IndexIterator &other) const { return index == other.index; }
    bool operator!=(const IndexIterator &other) const { return index != other.index; }
    bool operator<(const IndexIterator &other) const { return index < other.index; }
    bool operator>(const IndexIterator &other) const { return index > other.index; }
    bool operator<=(const IndexIterator &other) const { return index <= other.index; }
    bool operator>=(const IndexIterator &other) const { return index >= other.index; }

private:
    int index;
};


// 範囲を N 個に分割する関数
template<typename RandomIt, typename F>
std::vector<std::pair<RandomIt, RandomIt>> split_range(RandomIt bg, RandomIt ed, size_t N, F &&get_range_size) {
    size_t total_size = get_range_size(bg, ed);

    if (N == 0 || total_size == 0) {
        throw std::invalid_argument("N must be greater than 0 and range must not be empty");
    }

    size_t base_size = total_size / N;

    std::vector<std::pair<RandomIt, RandomIt>> result;
    result.reserve(N);
    int cur_idx = 0;
    int ed_idx = std::distance(bg, ed);

    for (size_t i = 0; i < N - 1; ++i) {
        auto next = std::partition_point(
                IndexIterator(cur_idx), IndexIterator(ed_idx),
                [&](auto idx) {
                    return get_range_size(bg + cur_idx, bg + idx) < base_size;
                });
        result.emplace_back(bg + cur_idx, bg + *next);
        cur_idx = *next;
    }
    result.emplace_back(bg + cur_idx, bg + ed_idx);

    return result;
}

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

    dynamic_array &operator=(dynamic_array &&other) noexcept {
        data = std::exchange(other.data, nullptr);
        len = std::exchange(other.len, 0);
        alloc = std::move(other.alloc);
        return *this;
    }

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

    T &at(std::size_t n) {
        if (n >= len) {
            std::stringstream ss;
            ss << __PRETTY_FUNCTION__ << ": n=" << n << ", len=" << len;
            throw std::out_of_range(ss.str());
        }
        return data[n];
    }

    const T &at(std::size_t n) const {
        if (n >= len) {
            std::stringstream ss;
            ss << __PRETTY_FUNCTION__ << ": n=" << n << ", len=" << len;
            throw std::out_of_range(ss.str());
        }
        return data[n];
    }

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
                if (partition_id + 1 >= static_cast<std::ptrdiff_t>(partitions->size())) {
                    current = partitions->back().end();
                    return *this;
                }
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
                if (partition_id == 0) {
                    current = partitions->front().begin();
                    return *this;
                }
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

    Iter get_iter() const { return current; }
    std::ptrdiff_t get_partition_id() const { return partition_id; }
    auto &get_partition() { return (*partitions)[partition_id]; }
    const auto &get_partition() const { return (*partitions)[partition_id]; }

private:
    Iter current;
    std::ptrdiff_t partition_id;
    Partitions *partitions;
};

template<typename T, typename Partition = std::vector<T>>
struct parted_vec {
    using value_type = T;
    using reference = T &;
    using const_reference = const T &;

    using partition_type = Partition;
    using partitions_type = dynamic_array<partition_type>;

    using iterator = parted_iter<typename partition_type::iterator, partitions_type>;
    using const_iterator = parted_iter<typename partition_type::const_iterator, const partitions_type>;

    explicit parted_vec(std::size_t n) : data(n) {}

    parted_vec(const parted_vec &) = delete;
    parted_vec &operator=(const parted_vec &) = delete;
    parted_vec(parted_vec &&) noexcept = default;
    parted_vec &operator=(parted_vec &&) noexcept = default;

    void push_back(const T &val, std::size_t partition_id) {
        data[partition_id].push_back(val);
    }

    void push_back(T &&val, std::size_t partition_id) {
        data[partition_id].push_back(std::move(val));
    }

    iterator begin() {
        for (std::size_t i = 0; i < partition_num(); ++i) {
            if (!data[i].empty())
                return iterator(data[i].begin(), 0, data);
        }
        return end();
    }
    iterator end() { return iterator(data.back().end(), data.size() - 1, data); }
    const_iterator begin() const { return const_iterator(data.front().begin(), 0, data); }
    const_iterator end() const { return const_iterator(data.back().end(), data.size() - 1, data); }
    const_iterator cbegin() { return const_iterator(data.front().cbegin(), 0, data); }
    const_iterator cend() { return const_iterator(data.back().cend(), data.size() - 1, data); }

    partition_type &get(std::size_t i) { return data.at(i); }
    const partition_type &get(std::size_t i) const { return data.at(i); }

    decltype(auto) operator[](std::size_t i) { return *(begin() + i); }
    decltype(auto) operator[](std::size_t i) const { return *(begin() + i); }

    std::size_t size() const {
        return std::accumulate(std::begin(data), std::end(data), 0, [](std::size_t v, auto &&p) { return v + p.size(); });
    }

    std::size_t partition_num() const { return data.size(); }

    partitions_type &partitions() { return data; }
    const partitions_type &partitions() const { return data; }

    auto part_begin() { return data.begin(); }
    auto part_end() { return data.end(); }
    auto part_begin() const { return data.begin(); }
    auto part_end() const { return data.end(); }

    template<typename Comp>
    friend void erase_if(parted_vec<T, Partition> &vec, Comp &&comp) {
        using std::erase_if;
        for (auto &part: vec.partitions())
            erase_if(part, comp);
    }

    template<typename P2>
    void merge(parted_vec<T, P2> &&vec) {
        if (partition_num() != vec.partition_num()) {
            throw std::runtime_error(__PRETTY_FUNCTION__);
        }
        for (std::size_t p = 0; p < partition_num(); ++p) {
            auto &self = get(p);
            auto &other = vec.get(p);
            if (self.empty()) {
                self = std::move(other);
            } else {
                self.reserve(self.size() + other.size());
                for (auto &&o: other) {
                    self.push_back(std::move(o));
                }
            }
        }
    }

    void shrink(std::size_t new_part_num) {
        if (new_part_num > data.size())
            return;
        partitions_type new_data(new_part_num);
        for (std::size_t i = 0; i < data.size(); ++i) {
            new_data[i % new_part_num].insert(new_data[i % new_part_num].end(),
                                              std::make_move_iterator(data[i].begin()),
                                              std::make_move_iterator(data[i].end()));
        }
        data = std::move(new_data);
    }

    template<typename F>
    void repartition2(const std::vector<std::pair<iterator, iterator>> &parts,
                      F &&push_back_fn) {
        partitions_type new_data(parts.size());
        for (std::size_t i = 0; i < parts.size(); ++i) {
            auto [bg, ed] = parts[i];
            assert(bg.get_iter() == data[i].begin());
            for (auto it = bg; it != ed; ++it) {
                push_back_fn(data[i], std::move(*it), it.get_partition_id());
            }
        }
    }

    template<typename Iter, typename F>
    void repartition(const std::vector<std::pair<Iter, Iter>> &parts,
                     F &&push_back_fn) {
        partitions_type new_data(parts.size());
        for (std::size_t i = 0; i < parts.size(); ++i) {
            auto [bg, ed] = parts[i];
            new_data[i].reserve(std::distance(bg, ed));
            for (auto it = bg; it != ed; ++it) {
                push_back_fn(new_data[i], std::move(*it), it.get_partition_id(), i);
            }
        }
        data = std::move(new_data);
    }

    template<typename F>
    auto balanced_partitions(F &&get_range_size) -> std::vector<std::pair<iterator, iterator>> {
        auto get_size = [this, &get_range_size](auto bg, auto ed) {
            if (bg.get_partition_id() == ed.get_partition_id()) {
                return get_range_size(bg.get_iter(), ed.get_iter());
            } else {
                auto total_size = get_range_size(bg.get_iter(), bg.get_partition().end());
                for (auto i = bg.get_partition_id() + 1; i < ed.get_partition_id(); ++i) {
                    total_size += this->get(i).get_sum_value();
                }
                total_size += get_range_size(ed.get_partition().begin(), ed.get_iter());
                return total_size;
            }
        };
        return split_range(this->begin(), this->end(), this->partition_num(), get_size);
    }

    template<typename F>
    auto balanced_partitions(F &&get_range_size) const -> std::vector<std::pair<const_iterator, const_iterator>> {
        auto get_size = [this, &get_range_size](auto bg, auto ed) {
            if (bg.get_partition_id() == ed.get_partition_id()) {
                return get_range_size(bg.get_iter(), ed.get_iter());
            } else {
                auto total_size = get_range_size(bg.get_iter(), bg.get_partition().end());
                for (auto i = bg.get_partition_id() + 1; i < ed.get_partition_id(); ++i) {
                    total_size += this->get(i).get_sum_value();
                }
                total_size += get_range_size(ed.get_partition().begin(), ed.get_iter());
                return total_size;
            }
        };
        return split_range(this->begin(), this->end(), this->partition_num(), get_size);
    }

private:
    partitions_type data;
};


#if 1
// #define WHEN_ALL_RETURN_TYPE(E) nova::when_all_awaitable<nova::VecTaskContainer<E>>
// #define WHEN_ALL_RETURN_VOID_TYPE nova::when_all_awaitable<nova::VecTaskContainer<void>>
#define WHEN_ALL_RETURN_TYPE(E) auto
#define WHEN_ALL_RETURN_VOID_TYPE auto
#define WHEN_ALL_RETURN return
#define WHEN_ALL_RETURN_VOID return
#else
#define WHEN_ALL_RETURN_TYPE(E) nova::task<std::vector<E>>
#define WHEN_ALL_RETURN_VOID_TYPE nova::task<>
#define WHEN_ALL_RETURN co_return co_await
#define WHEN_ALL_RETURN_VOID co_await
#endif

template<typename T, typename A, typename F, typename Sched>
        auto for_each_batched(parted_vec<T, A> &vec, F &&f, Sched &&sched, std::size_t batch_size)
                -> WHEN_ALL_RETURN_VOID_TYPE
        requires std::is_same_v < std::invoke_result_t<F, T &, std::size_t>,
void >
{
    std::vector<nova::task<>> tasks;
    std::size_t part_id = 0;
    for (auto &part: vec.partitions()) {
        auto bg = part.begin();
        for (std::size_t j = 0; j < part.size(); j += batch_size) {
            tasks.emplace_back([](auto bg, auto ed, auto part_id, auto &&f, auto &&sched) -> nova::task<> {
                co_await sched(part_id, bg, ed);
                for (auto it = bg; it < ed; ++it)
                    f(*it, part_id);
            }(bg + j, bg + std::min(j + batch_size, part.size()), part_id, f, sched));
        }
        ++part_id;
    }
    WHEN_ALL_RETURN_VOID nova::when_all(std::move(tasks));
}

template<typename T, typename A, typename F, typename Sched>
        auto for_each_batched(const parted_vec<T, A> &vec, F &&f, Sched &&sched, std::size_t batch_size)
                -> WHEN_ALL_RETURN_VOID_TYPE
        requires std::is_same_v < std::invoke_result_t<F, const T &, std::size_t>,
void >
{
    std::vector<nova::task<>> tasks;
    for (std::size_t part_id = 0; part_id < vec.partitions().size(); ++part_id) {
        auto &part = vec.partitions()[part_id];
        auto bg = part.begin();
        for (std::size_t j = 0; j < part.size(); j += batch_size)
            tasks.emplace_back([](auto bg, auto ed, auto part_id, auto &&f, auto &&sched) -> nova::task<> {
                co_await sched(part_id, bg, ed);
                for (auto it = bg; it < ed; ++it)
                    f(it, part_id);
            }(bg + j, bg + std::min(j + batch_size, part.size()), part_id, f, sched));
    }
    WHEN_ALL_RETURN_VOID nova::when_all(std::move(tasks));
}

template<typename T, typename A, typename F, typename Sched, typename Cond = nova::always_true,
         typename R = std::invoke_result_t<F, typename parted_vec<T, A>::partition_type &, std::size_t>>
auto partition_map(parted_vec<T, A> &vec, F &&f, Sched &&sched, Cond &&cond = {}) -> WHEN_ALL_RETURN_TYPE(R) {
    std::vector<nova::awaitable_variant<nova::task<R>, nova::immediate<R>>> tasks;
    for (std::size_t i = 0; i < vec.partitions().size(); ++i) {
        auto &part = vec.partitions().at(i);
        if (cond(part, i)) {
            tasks.emplace_back([](auto &part, auto part_id, auto &&f, auto &sched) -> nova::task<R> {
                co_await sched(part, part_id);
                co_return f(part, part_id);
            }(part, i, f, sched));
        } else {
            if constexpr (std::is_void_v<R>) {
                f(part, i);
                tasks.emplace_back(nova::immediate<void>{});
            } else {
                tasks.emplace_back(nova::immediate<R>(f(part, i)));
            }
        }
    }
    WHEN_ALL_RETURN nova::when_all(std::move(tasks));
}

template<typename T, typename A, typename F, typename Sched, typename Cond = nova::always_true,
         typename R = std::invoke_result_t<F, const typename parted_vec<T, A>::partition_type &, std::size_t>>
auto partition_map(const parted_vec<T, A> &vec, F &&f, Sched &&sched, Cond &&cond = {}) -> WHEN_ALL_RETURN_TYPE(R) {
    std::vector<nova::awaitable_variant<nova::task<R>, nova::immediate<R>>> tasks;
    for (std::size_t i = 0; i < vec.partitions().size(); ++i) {
        auto &part = vec.partitions()[i];
        if (cond(part, i)) {
            tasks.emplace_back([](auto &part, auto part_id, auto &f, auto &&sched) -> nova::task<R> {
                co_await sched(part, part_id);
                co_return f(part, part_id);
            }(part, i, f, sched));
        } else {
            tasks.emplace_back(nova::immediate<R>(f(part, i)));
        }
    }
    WHEN_ALL_RETURN nova::when_all(std::move(tasks));
}


}// namespace dphim
