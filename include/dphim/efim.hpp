#pragma once

#include <optional>

#include <dphim/logger.hpp>
#include <dphim/parse.hpp>
#include <dphim/util/pmem_allocator.hpp>
#include <dphim/util/raii.hpp>
#include <dphim/utility_bin_array.hpp>

namespace dphim {

struct EFIM : ConcurrentLogger {
    using Database = std::vector<Transaction>;
    using Itemset = std::vector<Item>;

    EFIM(const std::string &input_path, const std::string &output_path, Utility minutil, [[maybe_unused]] int th_num)
        : ConcurrentLogger(output_path, minutil, th_num), input_path(input_path), min_util(minutil) {}

    void run();

    std::pair<Transaction, Item> parseTransactionOneLine(std::string line);
    std::pair<std::vector<Transaction>, Item> parseTransactions(const std::string &input_path);

    void searchX(int j, const std::vector<Transaction> &transactionsOfP, const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix);
    void search(const std::vector<Transaction> &transactionsOfP, const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix);
    const UtilityBinArray &calcUpperBounds(const std::vector<Transaction> &transactionsPx, std::size_t j, const std::vector<Item> &itemsToKeep);

    static std::pair<std::vector<Utility>, std::vector<Item>> calcTWU(const Database &database, Item maxItem, Utility min_util);
    static std::vector<Utility> calcFirstSU(const Database &database, std::size_t maxItem);

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
        return std::shared_ptr<dphim::pmem_allocator>();
#else
        return nullptr;
#endif
        //        throw std::runtime_error("pmem is unsupported");
    }

    template<typename F, typename T>
    auto map_sp(F &&func, const std::vector<T> &args) -> std::vector<std::invoke_result_t<F, const T &>> {
        using R = std::invoke_result_t<F, T>;
        std::vector<std::thread> threads;
        std::vector<R> ret(args.size());
        auto diff = (args.size() - 1) / thread_num + 1;
        for (auto i = 0ul; i < thread_num; ++i) {
            threads.emplace_back([&ret, &args, &func](auto bg, auto ed) {
                for (auto idx = bg; idx < ed; ++idx)
                    ret[idx] = func(args[idx]);
            },
                                 i * diff, std::min<int>((i + 1) * diff, args.size()));
        }
        for (auto &th: threads)
            if (th.joinable())
                th.join();
        return ret;
    }

    template<typename F, typename T>
    auto map_sp(F &&func, std::vector<T> &&args) -> std::vector<std::invoke_result_t<F, T &&>> {
        using R = std::invoke_result_t<F, T>;
        std::vector<std::thread> threads;
        std::vector<R> ret(args.size());
        std::ptrdiff_t diff = (args.size() - 1) / thread_num + 1;
        for (std::ptrdiff_t i = 0; i < std::ptrdiff_t(thread_num); ++i) {
            threads.emplace_back([&ret, &args, &func](auto bg, auto ed) {
                for (auto idx = bg; idx < ed; ++idx)
                    ret[idx] = func(std::move(args[idx]));
            },
                                 i * diff, std::min<std::ptrdiff_t>((i + 1) * diff, args.size()));
        }
        for (auto &th: threads)
            if (th.joinable())
                th.join();
        return ret;
    }

    template<typename F, typename T>
    auto part_map(F &&func, const std::vector<T> &arg_vec) -> std::vector<std::invoke_result_t<F, T>> {
        using R = std::invoke_result_t<F, T>;
        std::vector<R> ret(arg_vec.size());
        std::vector<std::thread> threads;
        for (auto i = 0ul; i < arg_vec.size(); ++i) {
            threads.emplace_back([&] {
                ret[i] = func(arg_vec[i]);
            });
        }
        for (auto &th: threads)
            if (th.joinable())
                th.join();
        return ret;
    }

private:
    std::vector<Utility> utilityBinArraySU, utilityBinArrayLU;
    std::vector<Item> oldNameToNewNames, newNameToOldNames;

#ifdef DPHIM_PMEM
    std::vector<std::shared_ptr<pmem_allocator>> pmem_allocators;
#endif

    std::string input_path;
    Utility min_util;
    Item maxItem;

public:
    bool activateTransactionMerging = true;
    bool activateSubtreeUtilityPruning = true;
    long MAXIMUM_SIZE_MERGING = 1000;
    bool use_parallel_sort = true;

    void set_pmem_path([[maybe_unused]] std::vector<std::string> paths) {
#ifdef DPHIM_PMEM
        try {
            pmem_allocators.resize(paths.size());
            for (auto i = 0ul; i < pmem_allocators.size(); ++i) {
                pmem_allocators[i] = std::make_shared<pmem_allocator>(paths[i].c_str());
            }
        } catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }
#endif
    }
};

}// namespace dphim