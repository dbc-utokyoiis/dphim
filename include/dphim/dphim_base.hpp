#pragma once

#include <dphim/efim.hpp>
#include <dphim/logger.hpp>
#include <dphim/util/parted_vec.hpp>
#include <dphim/util/pmem_allocator.hpp>
#include <dphim/vector_with_bytes.hpp>

#include <nova/parallel_sort.hpp>
#include <nova/scheduler_base.hpp>
#include <nova/task.hpp>

#include <deque>
#include <sys/types.h>

namespace dphim {

using Transactions = PrefixSumContainer<Transaction, std::size_t, &Transaction::bytes>;
using Database = parted_vec<Transaction, Transactions>;

struct DphimBase : ConcurrentLogger, pmem_allocate_trait {

protected:
    std::shared_ptr<nova::scheduler_base> sched;
    std::string input_path;
    bool sched_no_await = false;
    PmemAllocType pmem_alloc_type = PmemAllocType::None;

public:
    void set_sched_no_await(bool flag) {
        sched_no_await = flag;
    }

    void set_debug_mode(bool debug_mode = true) {
        pmem_allocate_trait::is_debug_mode = debug_mode;
        ConcurrentLogger::is_debug = debug_mode;
    }

    bool is_debug_mode() const {
        return is_debug;
    }

    void set_pmem_alloc_type(const std::string &typ) {
        if (typ == "aek") {
            pmem_alloc_type = PmemAllocType::AEK;
        } else if (typ == "elems") {
            pmem_alloc_type = PmemAllocType::Elems;
        } else if (typ.empty() || typ == "none") {
            pmem_alloc_type = PmemAllocType::None;
        } else {
            throw std::runtime_error("unknown pmem alloc type: " + typ);
        }
    }

    DphimBase(std::shared_ptr<nova::scheduler_base> sched, std::string input_path, const std::string &output_path, Utility minutil, int th_num)
        : ConcurrentLogger(output_path, minutil, th_num),
          sched(std::move(sched)), input_path(std::move(input_path)) {}

    auto schedule(int option = -1) const -> nova::scheduler_base::operation {
        if (sched_no_await) {
            return sched->schedule(nova::OPTION_NO_AWAIT);
        } else {
            return sched->schedule(option);
        }
    }

    std::pair<Transaction, Item> parseOneLine(std::string line, [[maybe_unused]] int node);

    auto parseTransactions(std::function<std::size_t(std::size_t)> get_partition_num = nullptr)
            -> nova::task<std::pair<Database, Item>>;

    auto parseFileRange(const char *pathname, off_t bg, off_t ed, int node) -> nova::task<std::pair<Transactions, Item>>;

    template<typename I, typename D>
    auto calcTWU(D &database, Item max_item, std::size_t threshold = 0) -> nova::task<std::pair<std::vector<Utility>, I>> {
        if (is_debug_mode()) {
            std::cerr << "calcTWU" << std::endl;
            std::cerr << "  scatter threshold: " << threshold << std::endl;
        }
        auto partedItemTWU = co_await partition_map(
                database,
                [max_item](auto &pdb, auto /*part_id*/) {
                    std::vector<Utility> itemTWU(max_item + 1, 0);
                    for (auto &transaction: pdb)
                        for (auto &[item, utility]: transaction)
                            itemTWU[item] += transaction.transaction_utility;
                    return itemTWU;
                },
                [this, threshold](auto &part, auto part_id) {
                    if (is_debug_mode()) {
                        std::cerr << "  database@node" << part_id << ": "
                                  << (part.get_sum_value() > threshold ? "scatter" : "no scatter")
                                  << std::endl;
                    }
                    if (part.get_sum_value() > threshold) {
                        return schedule(part_id);
                    } else {
                        return schedule();
                    }
                });

        auto itemTWU = [max_item](auto &&ret) {
            if constexpr (std::is_same_v<std::remove_cvref_t<decltype(ret)>, std::vector<Utility>>) {
                return ret;
            } else {
                for (std::size_t i = 1; i < ret.size(); ++i)
                    for (std::size_t j = 0; j < max_item + 1; ++j)
                        ret[0][j] += ret[i][j];
                return ret[0];
            }
        }(partedItemTWU);

        I items;
        items.reserve(itemTWU.size());
        for (int i = 1; i < int(itemTWU.size()); ++i)
            if (itemTWU[i] >= min_util)
                items.push_back(i);
        co_await nova::parallel_sort(
                items.begin(), items.end(),
                [&](auto l, auto r) { return itemTWU[l] < itemTWU[r]; },
                [this] { return schedule(); });
        co_return std::make_pair(std::move(itemTWU), std::move(items));
    }
};


}// namespace dphim