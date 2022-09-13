#pragma once

#include <dphim/logger.hpp>
#include <dphim/util/pmem_allocator.hpp>

#include <nova/parallel_sort.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/task.hpp>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace dphim {

struct dphim_base : ConcurrentLogger {

    std::shared_ptr<nova::scheduler_base> sched;
    std::string input_path;
    bool sched_no_await = false;

    enum class PmemAllocType {
        None,
        AEK,
        Elems,
    } pmem_alloc_type = PmemAllocType::None;

    using Database = std::vector<Transaction>;

    dphim_base(std::shared_ptr<nova::scheduler_base> sched, std::string input_path, std::string output_path, Utility minutil, int th_num)
        : ConcurrentLogger(std::move(output_path), minutil, th_num), sched(std::move(sched)), input_path(std::move(input_path)) {
    }

    auto schedule(int option = -1) {
        if (sched_no_await) {
            return sched->schedule(nova::scheduler_base::operation::OPTION_NO_AWAIT);
        } else {
            return sched->schedule(option);
        }
    }

    std::pair<Transaction, Item> parseOneLine(std::string line, [[maybe_unused]] int node);

    template<bool do_partitioning>
    auto parseTransactions() -> nova::task<std::pair<std::conditional_t<do_partitioning, std::vector<Database>, Database>, Item>>;

    auto parseFileRange(const char *pathname, off_t bg, off_t ed, int node) -> nova::task<std::pair<Database, Item>>;

    template<typename I, typename D>
    auto calcTWU(D &database, Item max_item) -> nova::task<std::pair<std::vector<Utility>, I>> {
        std::vector<Utility> itemTWU;
        itemTWU.resize(max_item + 1, 0);

        for_each_part(database, [&](auto /*node*/, auto &database) {
            for (auto &transaction: database) {
                for (auto &[item, utility]: transaction) {
                    itemTWU[item] += transaction.transaction_utility;
                }
            }
        });

        I items;
        items.reserve(itemTWU.size());
        for (int i = 1; i < int(itemTWU.size()); ++i) {
            if (itemTWU[i] >= min_util)
                items.push_back(i);
        }
        co_await nova::parallel_sort(
                items.begin(), items.end(),
                [&](auto l, auto r) { return itemTWU[l] < itemTWU[r]; }, [this] { return schedule(); });

        co_return std::make_pair(std::move(itemTWU), std::move(items));
    }

    template<typename D, typename F>
    auto for_each_part(D &db, F &&func) {
        if constexpr (std::is_same_v<std::remove_cvref_t<D>, std::vector<Database>>) {
            using R = std::invoke_result_t<decltype(func), int, decltype(db[0])>;
            if constexpr (std::is_void_v<R>) {
                for (int i = 0; i < int(db.size()); ++i) {
                    func(i, db[i]);
                }
            } else {
                std::vector<R> ret;
                for (int i = 0; i < int(db.size()); ++i) {
                    ret.push_back(func(i, db[i]));
                }
                return ret;
            }
        } else {
            return func(-1, db);
        }
    }

    template<typename T, typename F, typename R = std::invoke_result_t<F, T &, int>>
    auto for_each_dp(std::vector<T> &db, F &&func, int p_size = 1000) {
        std::vector<nova::task<R>> tasks;
        static_assert(std::is_void_v<R>);

        auto f = [](auto self, auto &&func, auto bg, auto ed) -> nova::task<R> {
            co_await self->schedule();
            for (auto it = bg; it != ed; ++it)
                func(*it, -1);
        };

        for (auto it = db.begin(); it < db.end(); it += p_size) {
            auto ed = it + std::min<std::size_t>(p_size, std::distance(it, db.end()));
            tasks.push_back(f(this, func, it, ed));
        }
        return nova::when_all(std::move(tasks));
    }

    template<typename T, typename F, typename R = std::invoke_result_t<F, T &, int>>
    auto for_each_dp(std::vector<std::vector<T>> &parted_db, F &&func, int p_size = 1000) {
        std::vector<nova::task<>> tasks;
        for (auto node = 0ul; node < parted_db.size(); ++node) {
            auto &db = parted_db[node];
            for (auto it = db.begin(); it < db.end(); it += p_size) {
                auto ed = it + std::min<std::size_t>(p_size, std::distance(it, db.end()));
                tasks.push_back([](auto self, auto &&func, auto bg, auto ed, auto node) -> nova::task<> {
                    co_await self->schedule(node);
                    for (auto it = bg; it != ed; ++it)
                        func(*it, node);
                }(this, func, it, ed, node));
            }
        }
        return nova::when_all(std::move(tasks));
    }

    template<typename M, int no_sched_limit = 10>
    auto part_map(std::vector<Database> &parted_db, M &&map_func) {
        using R = std::invoke_result_t<decltype(map_func), decltype(parted_db[0]), int>;
        using ret_type = std::conditional_t<std::is_void_v<R>, void, std::pair<int, R>>;

        std::vector<nova::task<ret_type>> tasks;
        for (auto node = 0ul; node < parted_db.size(); ++node) {
            tasks.emplace_back([](auto self, auto node, auto &db, auto &map) -> nova::task<ret_type> {
                if (db.size() > no_sched_limit)
                    co_await self->schedule(node);
                if constexpr (std::is_void_v<ret_type>) {
                    map(db, node);
                } else {
                    co_return std::make_pair(node, map(db, node));
                }
            }(this, node, parted_db[node], map_func));
        }
        return nova::when_all(std::move(tasks));
    }

    template<typename M>
    auto part_map_awaitable(std::vector<Database> &parted_db, M &&map_func, std::size_t no_sched_limit = 10) {
        std::vector<nova::task<>> tasks;
        for (auto node = 0ul; node < parted_db.size(); ++node) {
            tasks.emplace_back([](auto self, auto node, auto &db, auto &map, auto no_sched_limit) -> nova::task<> {
                if (db.size() > no_sched_limit)
                    co_await self->schedule(node);
                co_await map(db, node);
            }(this, node, parted_db[node], map_func, no_sched_limit));
        }
        return nova::when_all(std::move(tasks));
    }

    template<typename M, int no_sched_limit = 10>
    auto part_map(const std::vector<Database> &parted_db, M &&map_func) {
        using R = std::invoke_result_t<decltype(map_func), decltype(parted_db[0]), int>;
        using ret_type = std::conditional_t<std::is_void_v<R>, void, std::pair<int, R>>;

        std::vector<nova::task<ret_type>> tasks;
        for (auto node = 0ul; node < parted_db.size(); ++node) {
            tasks.emplace_back([](auto self, auto node, auto &db, auto &map) -> nova::task<ret_type> {
                if (db.size() > no_sched_limit)
                    co_await self->schedule(node);
                if constexpr (std::is_void_v<ret_type>) {
                    map(db, node);
                } else {
                    co_return std::make_pair(node, map(db, node));
                }
            }(this, node, parted_db[node], map_func));
        }
        return nova::when_all(std::move(tasks));
    }

    template<typename M, int no_sched_limit = 10>
    auto part_map(Database &db, M &&map_func) {
        using R = std::invoke_result_t<decltype(map_func), decltype(db), int>;
        using ret_type = std::conditional_t<std::is_void_v<R>, void, std::pair<int, R>>;
        return [](auto self, auto &db, auto &map) -> nova::task<ret_type> {
            if (db.size() > no_sched_limit)
                co_await self->schedule();
            if constexpr (std::is_void_v<ret_type>) {
                map(db, -1);
            } else {
                co_return std::make_pair(-1, map(db, -1));
            }
        }(this, db, map_func);
    }

    template<typename M>
    auto part_map_awaitable(Database &db, M &&map_func, std::size_t no_sched_limit = 10) {
        return [](auto self, auto &db, auto &map, auto no_sched_limit) -> nova::task<> {
            if (db.size() > no_sched_limit)
                co_await self->schedule();
            co_await map(db, -1);
        }(this, db, map_func, no_sched_limit);
    }

    template<typename M, int no_sched_limit = 10>
    auto part_map(const Database &db, M &&map_func) {
        using R = std::invoke_result_t<decltype(map_func), decltype(db), int>;
        using ret_type = std::conditional_t<std::is_void_v<R>, void, std::pair<int, R>>;
        return [](auto self, auto &db, auto &map) -> nova::task<ret_type> {
            if (db.size() > no_sched_limit)
                co_await self->schedule();
            if constexpr (std::is_void_v<ret_type>) {
                map(db, -1);
            } else {
                co_return std::make_pair(-1, map(db, -1));
            }
        }(this, db, map_func);
    }

#ifdef DPHIM_PMEM
    inline static std::vector<std::shared_ptr<pmem_allocator>> pmem_allocators;

    template<typename T>
    struct local_pmem_allocator {
        using value_type = T;
        using pointer = T *;
        pointer allocate(std::size_t n) {
            static thread_local auto p = [] {
                unsigned int cpu, node;
                getcpu(&cpu, &node);
                return std::make_pair(cpu, node);
            }();
            return reinterpret_cast<T *>(pmem_allocators.at(p.second)->alloc(n * sizeof(T)));
        }

        void deallocate(pointer p, [[maybe_unused]] std::size_t n) {
            dphim::pmem_allocator::dealloc(p);
        }
    };
#endif

    static void set_pmem_path([[maybe_unused]] std::vector<std::string> paths) {
#ifdef DPHIM_PMEM
        try {
            pmem_allocators.resize(paths.size());
            for (auto i = 0ul; i < pmem_allocators.size(); ++i) {
                pmem_allocators[i] = std::make_shared<dphim::pmem_allocator>(paths[i].c_str());
            }
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }
#endif
    }

    auto get_pmem_allocator([[maybe_unused]] std::optional<int> node = std::nullopt) {
#ifdef DPHIM_PMEM
        if (pmem_allocators.size() == 1) {
            return pmem_allocators.front();
        } else if (!pmem_allocators.empty()) {
            if (node) {
                return pmem_allocators.at(*node);
            } else {
                thread_local auto [cpu, node] = [] {
                    unsigned int cpu, node;
                    getcpu(&cpu, &node);
                    return std::make_pair(cpu, node);
                }();
                return pmem_allocators.at(node);
            }
        }
#endif
        throw std::runtime_error("pmem is unsupported");
    }
};


template<bool do_partitioning>
auto dphim_base::parseTransactions() -> nova::task<std::pair<std::conditional_t<do_partitioning, std::vector<Database>, Database>, Item>> {

    struct stat st;
    if (stat(input_path.c_str(), &st) == -1)
        throw std::runtime_error(strerror(errno));

    if constexpr (do_partitioning) {
        std::vector<nova::task<std::pair<Database, Item>>> tasks;

        auto max_node = sched->get_max_node_id().value();
        auto fsize = st.st_size;
        auto diff = (fsize - 1) / (max_node + 1) + 1;
        for (int i = 0; i <= max_node; ++i) {
            auto bg = diff * i;
            auto ed = std::min(diff * (i + 1), fsize + 1);
            tasks.push_back(parseFileRange(input_path.c_str(), bg, ed, i));
        }

        std::vector<Database> ret;
        Item maxItem = 0;
        for (auto &&[db, mI]: co_await nova::when_all(std::move(tasks))) {
            maxItem = std::max(mI, maxItem);
            ret.push_back(std::move(db));
        }

        co_return std::make_pair(std::move(ret), maxItem);
    } else {
        auto [ret, mI] = co_await parseFileRange(input_path.c_str(), 0, st.st_size, -1);
        co_return std::make_pair(std::move(ret), mI);
    }
}

}// namespace dphim