#pragma once

#include <nova/scheduler_base.hpp>
#include <nova/task.hpp>
#include <nova/when_all.hpp>
#include <nova/worker.hpp>

#include <dphim/dphim_base.hpp>
#include <dphim/logger.hpp>
#include <dphim/util/pmem_allocator.hpp>
#include <dphim/utility_bin_array.hpp>
#include <nova/jemalloc.hpp>

// #define NOINLINE __attribute__((noinline))
#define NOINLINE

namespace dphim {

struct DPEFIM : DphimBase {

    bool use_parallel_sort = true;
    enum ScatterType {
        None,
        Best,
        All,
    } scatter_type = ScatterType::Best;

    void set_scatter_type(const std::string &str) {
        if (str == "none") {
            scatter_type = ScatterType::None;
        } else if (str == "best") {
            scatter_type = ScatterType::Best;
        } else if (str == "all") {
            scatter_type = ScatterType::All;
        } else {
            throw std::runtime_error("unknown scatter type: " + str);
        }
    }

    friend std::ostream &operator<<(std::ostream &os, const ScatterType &st) {
        switch (st) {
            case ScatterType::None:
                return os << "none";
            case ScatterType::Best:
                return os << "best";
            case ScatterType::All:
                return os << "all";
            default:
                return os << "unknown";
        }
    }

private:
    std::vector<Item> oldNameToNewNames;
    std::vector<Item> newNameToOldNames;

    Utility min_util;
    Item maxItem = 0;
    std::size_t partition_num = 1;


public:
    struct SpeculationThresholds {
        std::size_t step1_scatter_alloc_threshold = 20000000;// 20MB
        std::size_t step1_task_migration_threshold = 100000;
        std::size_t step2_scatter_alloc_threshold = 100000;
        std::size_t step2_task_migration_threshold = 100000;
        std::size_t step3_scatter_alloc_threshold = 100;
        std::size_t step3_task_migration_threshold = 20000;
        std::size_t step3_stop_scatter_alloc_depth = 1000;
        std::size_t step3_stop_task_migration_depth = 1000;
    } thresholds;

    void set_speculation_thresholds(const SpeculationThresholds &thresholds) {
        this->thresholds = thresholds;
    }

    DPEFIM(std::shared_ptr<nova::scheduler_base> sched, const std::string &input_path, const std::string &output_path, Utility minutil, int th_num)
        : DphimBase(std::move(sched), input_path, output_path, minutil, th_num),
          min_util(minutil) {}

    auto run() -> nova::task<>;

    template<typename I>
    auto run_impl() -> nova::task<>;

    template<typename D>
    auto calcFirstSU(D &database) -> nova::task<std::vector<Utility>>;

    template<typename T>
    auto calcUtilityAndNextDB(Item x, T &&db, int node = -1, bool allow_scatter = false) -> std::pair<Utility, Database>;

    template<typename D, typename I>
    auto search(const I &prefix, const D &transactionsOfP, I &&itemsToKeep, I &&itemsToExplore) {
        incCandidateCount(itemsToExplore.size());
        std::vector<nova::task<>> tasks;
        tasks.reserve(itemsToExplore.size());
        for (int j = 0; j < int(itemsToExplore.size()); ++j) {
            tasks.emplace_back(searchX(j, prefix, transactionsOfP, itemsToKeep, itemsToExplore));
        }
        return nova::when_all(std::move(tasks));
    }

    template<typename D, typename I, typename I2>
    auto searchX(int j, I &&prefix, const D &transactionsOfP, I2 &&itemsToKeep, I2 &&itemsToExplore) -> nova::task<>;

    template<typename D, typename I>
    void calcUpperBoundsImpl(UtilityBinArray &ub, std::size_t j, const D &db, const I &itemsToKeep) const;

    template<bool no_use_thread_local, typename D, typename I>
    NOINLINE auto calcUpperBounds(std::size_t j, const D &transactionsPx, const I &itemsToKeep) const
            -> std::conditional_t<no_use_thread_local, UtilityBinArray, UtilityBinArray &>;

    auto cloneTransaction(const Transaction &tra, std::optional<int> node) {
        if (pmem_alloc_type != PmemAllocType::None) {
#ifdef DPHIM_PMEM
            auto pmem_allocator = get_pmem_allocator(node);
            return tra.clone(
                    [=](auto size) { return pmem_allocator->alloc(size); },
                    [=]([[maybe_unused]] auto size) {
                        return [=](auto *p) {
                            using U = std::remove_pointer_t<std::remove_cvref_t<decltype(p)>>;
                            p->~U();
                            pmem_allocator->dealloc(p);
                        };
                    });
#else
            throw std::runtime_error("pmem is unsupported ");
#endif
        } else {
            if (node) {
                auto cpu = sched->get_corresponding_cpu_id(*node);
                if (cpu) {
                    return tra.clone([cpu](std::size_t sz) {
                        return nova::malloc_on_thread(sz, *cpu);
                    });
                } else {
                    std::cerr << "failed to find corresponding cpu" << std::endl;
                    std::abort();
                }
            } else {
                return tra.clone();
            }
        }
    }

    void repartition(Database &database);
    double balanceCheck(const Database &database) const;
};

}// namespace dphim
