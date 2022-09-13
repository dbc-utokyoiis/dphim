#pragma once

#include <nova/config.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/task.hpp>
#include <nova/when_all.hpp>
#include <nova/worker.hpp>

#include <dphim/dphim_base.hpp>
#include <dphim/logger.hpp>
#include <dphim/util/pmem_allocator.hpp>
#include <dphim/utility_bin_array.hpp>

//#define NOINLINE __attribute__((noinline))
#define NOINLINE

namespace dphim {

struct DPEFIM : dphim_base {

    bool use_parallel_sort = true;
    bool use_parted_database = false;


private:
    std::vector<Item> oldNameToNewNames;
    std::vector<Item> newNameToOldNames;

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

    Utility min_util;
    Item maxItem;

public:
    inline static long MAXIMUM_SIZE_MERGING = 1000;

private:
#define SCHEDULE co_await schedule()

public:
    //    using Itemset = std::vector<Item>;
    using Database = std::vector<Transaction>;

    DPEFIM(std::shared_ptr<nova::scheduler_base> sched, const std::string &input_path, const std::string &output_path, Utility minutil, int th_num)
        : dphim_base(std::move(sched), input_path, output_path, minutil, th_num),
          min_util(minutil) {}

    auto run() -> nova::task<>;

    template<bool do_partitioning, typename I>
    auto run_impl() -> nova::task<>;

    template<typename D>
    auto calcFirstSU(D &database) -> nova::task<std::vector<Utility>>;

    NOINLINE auto calcUtilityAndNextDB(Item x, const std::pair<DPEFIM::Database::const_iterator, DPEFIM::Database::const_iterator> transactionRange, int node = -1)
            -> std::pair<Utility, DPEFIM::Database>;

    template<typename D, typename I>
    auto search(const I &prefix, const D &transactionsOfP, const I &itemsToKeep, const I &itemsToExplore) {
        incCandidateCount(itemsToExplore.size());
        std::vector<nova::task<>> tasks;
        tasks.reserve(itemsToExplore.size());
        for (int j = 0; j < int(itemsToExplore.size()); ++j) {
            tasks.push_back(searchX(j, prefix, transactionsOfP, itemsToKeep, itemsToExplore));
        }
        return nova::when_all(std::move(tasks));
    }

    template<typename D, typename I>
    auto searchX(int j, const I &prefix, const D &transactionsOfP, const I &itemsToKeep, const I &itemsToExplore) -> nova::task<>;

    template<bool do_partitioning, typename D, typename I>
    auto calcUpperBounds(std::size_t j, const D &transactionsPx, const I &itemsToKeep) const
            -> std::conditional_t<do_partitioning, UtilityBinArray, const UtilityBinArray &>;
};

}// namespace dphim
