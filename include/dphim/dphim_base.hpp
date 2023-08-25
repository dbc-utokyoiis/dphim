#pragma once

#include <dphim/logger.hpp>
#include <dphim/util/parted_vec.hpp>
#include <dphim/util/pmem_allocator.hpp>

#include <nova/immediate.hpp>
#include <nova/parallel_sort.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/task.hpp>
#include <nova/variant.hpp>

#include <concepts>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace dphim {

using Database = parted_vec<Transaction>;

struct DefaultCond {
    bool await_flag(const Database & /*db*/, std::size_t /*node_id*/) const { return true; }
    int sched_opt(const Database & /*db*/, std::size_t /*node_id*/) const { return nova::OPTION_DEFAULT; }
};

struct DBSizeLimit {
    bool await_flag(const Database &db, std::size_t /*node_id*/) const {
        return db.size() > no_await_limit_size;
    }
    int sched_opt(const Database &db, std::size_t node_id) const {
        return db.size() > move_limit_size ? int(node_id) : nova::OPTION_DEFAULT;
    }
    std::size_t move_limit_size = 10;
    std::size_t no_await_limit_size = 10;
};

//struct DBBytesLimit {
//    bool await_flag(const Database &db, std::size_t /*node_id*/) const {
//        return db.bytes() > limit_bytes;
//    }
//    int sched_opt(const Database &db, std::size_t node_id) const {
//        return db.bytes() > limit_bytes ? int(node_id) : nova::OPTION_DEFAULT;
//    }
//    std::size_t limit_bytes;
//};

struct dphim_base : ConcurrentLogger {

    std::shared_ptr<nova::scheduler_base> sched;
    std::string input_path;
    bool sched_no_await = false;

    using Transactions = std::vector<Transaction>;

    enum class PmemAllocType { None,
                               AEK,
                               Elems,
    } pmem_alloc_type = PmemAllocType::None;

    dphim_base(std::shared_ptr<nova::scheduler_base> sched, std::string input_path, std::string output_path, Utility minutil, int th_num)
        : ConcurrentLogger(std::move(output_path), minutil, th_num), sched(std::move(sched)), input_path(std::move(input_path)) {}

    auto schedule(int option = -1) {
        if (sched_no_await) {
            return sched->schedule(nova::OPTION_NO_AWAIT);
        } else {
            return sched->schedule(option);
        }
    }

    std::pair<Transaction, Item> parseOneLine(std::string line, [[maybe_unused]] int node);

    auto parseTransactions() -> nova::task<std::pair<Database, Item>>;

    auto parseFileRange(const char *pathname, off_t bg, off_t ed, int node) -> nova::task<std::pair<Transactions, Item>>;

    template<typename I, typename D>
    auto calcTWU(D &database, Item max_item) -> nova::task<std::pair<std::vector<Utility>, I>> {

        auto itemTWU = [max_item](auto &&ret) {
            if constexpr (std::is_same_v<std::remove_cvref_t<decltype(ret)>, std::vector<Utility>>) {
                return ret;
            } else {
                for (std::size_t i = 1; i < ret.size(); ++i)
                    for (std::size_t j = 0; j < max_item + 1; ++j)
                        ret[0][j] += ret[i][j];
                return ret[0];
            }
        }(co_await partition_map(
                               database, [&](auto &pdb) {
                    std::vector<Utility> itemTWU(max_item + 1, 0);
                    for (auto &transaction: pdb)
                        for (auto &[item, utility]: transaction)
                            itemTWU[item] += transaction.transaction_utility;
                    return itemTWU; }, [this](auto part_id) { return schedule(part_id); }));

        I items;
        items.reserve(itemTWU.size());
        for (int i = 1; i < int(itemTWU.size()); ++i)
            if (itemTWU[i] >= min_util)
                items.push_back(i);

        co_await nova::parallel_sort(
                items.begin(), items.end(),
                [&](auto l, auto r) { return itemTWU[l] < itemTWU[r]; }, [this] { return schedule(); });

        co_return std::make_pair(std::move(itemTWU), std::move(items));
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


}// namespace dphim