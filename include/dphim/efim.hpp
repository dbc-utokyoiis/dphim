#pragma once

#include <optional>
#include <thread>

#include <dphim/logger.hpp>
#include <dphim/parse.hpp>
#include <dphim/util/pmem_allocator.hpp>
#include <dphim/util/raii.hpp>
#include <dphim/utility_bin_array.hpp>

namespace dphim {

enum class PmemAllocType { None,
                           AEK,
                           Elems,
};

enum class PartStrategy {
    Normal,
    Rnd,
    Weighted,
    TwoLenPrefixPart,
};

inline std::ostream &operator<<(std::ostream &os, const PartStrategy &strategy) {
    switch (strategy) {
        case PartStrategy::Normal:
            os << "Normal";
            break;
        case PartStrategy::Rnd:
            os << "Rnd";
            break;
        case PartStrategy::Weighted:
            os << "Weighted";
            break;
        case PartStrategy::TwoLenPrefixPart:
            os << "TwoLenPrefixPart";
            break;
    }
    return os;
}


struct EFIM : ConcurrentLogger, pmem_allocate_trait {
    using Itemset = std::vector<Item>;
    using Database = std::vector<Transaction>;

private:
    std::vector<Utility> utilityBinArraySU, utilityBinArrayLU;
    std::vector<Item> oldNameToNewNames, newNameToOldNames;

    std::string input_path;
    Utility min_util;
    Item maxItem;
    PartStrategy partitioning_strategy;
    PmemAllocType pmem_alloc_type = PmemAllocType::None;

public:
    explicit EFIM(const std::string &input_path, const std::string &output_path, Utility minutil, [[maybe_unused]] int th_num)
        : ConcurrentLogger(output_path, minutil, th_num), input_path(input_path), min_util(minutil) {}

    void set_partition_strategy(const std::string &strategy) {
        if (strategy == "rnd") {
            partitioning_strategy = PartStrategy::Rnd;
        } else if (strategy == "weighted") {
            partitioning_strategy = PartStrategy::Weighted;
        } else if (strategy == "twolen") {
            partitioning_strategy = PartStrategy::TwoLenPrefixPart;
        } else {
            partitioning_strategy = PartStrategy::Normal;
        }
        if (is_debug_mode) {
            std::cerr << "Current partitioning strategy: ";
            switch (partitioning_strategy) {
                case PartStrategy::Normal:
                    std::cerr << "Normal" << std::endl;
                    break;
                case PartStrategy::Rnd:
                    std::cerr << "Rnd" << std::endl;
                    break;
                case PartStrategy::Weighted:
                    std::cerr << "Weighted" << std::endl;
                    break;
                case PartStrategy::TwoLenPrefixPart:
                    std::cerr << "TwoLenPrefixPart" << std::endl;
                    break;
            }
        }
    }

    void set_debug_mode(bool flag) {
        is_debug_mode = flag;
    }

    void run();

    template<typename I>
    void run_impl();

    std::pair<Transaction, Item> parseTransactionOneLine(std::string line);
    std::pair<Database, Item> parseTransactions(const std::string &input_path);

    struct SearchXRet {
        Database projectedDB;
        Itemset itemsToKeep;
        Itemset itemsToExplore;
        std::vector<Item> prefix;
        Utility utility;
    };

    auto searchXImpl(int j, const Database &transactionsOfP,
                     const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix)
            -> SearchXRet;

    void searchX(int j, const Database &transactionsOfP,
                 const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix) {
        auto ret = searchXImpl(j, transactionsOfP, itemsToKeep, itemsToExplore, std::move(prefix));
        if (activateSubtreeUtilityPruning) {
            if (!ret.itemsToExplore.empty()) {
                search(ret.projectedDB,
                       ret.itemsToKeep,
                       ret.itemsToExplore,
                       std::move(ret.prefix));
            }
        } else {
            search(ret.projectedDB,
                   ret.itemsToKeep,
                   ret.itemsToExplore,
                   std::move(ret.prefix));
        }
    }

    void search(const Database &transactionsOfP,
                const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix) {
        incCandidateCount(itemsToExplore.size());
        for (int j = 0; j < int(itemsToExplore.size()); ++j) {
            searchX(j, transactionsOfP, itemsToKeep, itemsToExplore, prefix);
        }
    }

    void search(const Database &transactionsOfP,
                const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix,
                std::vector<int> xs) {
        incCandidateCount(xs.size());
        for (auto j: xs) {
            searchX(j, transactionsOfP, itemsToKeep, itemsToExplore, prefix);
        }
    }

    // void search(const Database &transactionsOfP,
    //             const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix,
    //             std::pair<int, int> range) {
    //     if (range.first > range.second) {
    //         throw std::runtime_error("hoge");
    //     }
    //     std::cout << range.second - range.first << std::endl;
    //     incCandidateCount(range.second - range.first);
    //     for (int j = range.first; j < range.second; ++j) {
    //         searchX(j, transactionsOfP, itemsToKeep, itemsToExplore, prefix);
    //     }
    // }

    const UtilityBinArray &calcUpperBounds(const Database &transactionsPx, std::size_t j, const std::vector<Item> &itemsToKeep);

    template<typename I = std::vector<Item>>
    static std::pair<std::vector<Utility>, I> calcTWU(const Database &database, Item maxItem, Utility min_util);

    static std::vector<Utility> calcFirstSU(const Database &database, std::size_t maxItem);

    template<typename F, typename T, typename R = std::invoke_result_t<F, T>>
    auto map_sp(F &&func, const std::vector<T> &args) -> std::enable_if_t<!std::is_same_v<R, void>, std::vector<R>> {
        std::vector<std::thread> threads;
        std::vector<R> ret(args.size());
        auto diff = (args.size() - 1) / thread_num + 1;
        for (auto i = 0ul; i < thread_num; ++i) {
            threads.emplace_back([&ret, &args, &func](auto bg, auto ed) {
                for (auto idx = bg; idx < ed; ++idx) {
                    ret[idx] = func(args[idx]);
                }
            },
                                 i * diff, std::min<int>((i + 1) * diff, args.size()));
        }
        for (auto &th: threads)
            if (th.joinable())
                th.join();
        return ret;
    }

    template<typename F, typename T, typename R = std::invoke_result_t<F, T>>
    auto map_sp(F &&func, std::vector<T> &&args) -> std::enable_if_t<!std::is_same_v<R, void>, std::vector<R>> {
        std::vector<std::thread> threads;
        std::vector<R> ret(args.size());
        auto diff = (args.size() - 1) / thread_num + 1;
        for (auto i = 0ul; i < thread_num; ++i) {
            threads.emplace_back([&ret, &args, &func](auto bg, auto ed) {
                for (auto idx = bg; idx < ed; ++idx) {
                    ret[idx] = func(std::move(args[idx]));
                }
            },
                                 i * diff, std::min<unsigned long>((i + 1) * diff, args.size()));
        }
        for (auto &th: threads)
            if (th.joinable())
                th.join();
        return ret;
    }

    template<typename F, typename T, typename R = std::invoke_result_t<F, T>>
    auto map_sp(F &&func, const std::vector<T> &args) -> std::enable_if_t<std::is_same_v<R, void>, void> {
        std::vector<std::thread> threads;
        auto diff = (args.size() - 1) / thread_num + 1;
        for (auto i = 0ul; i < thread_num; ++i) {
            threads.emplace_back([&args, &func](auto bg, auto ed) {
                for (auto idx = bg; idx < ed; ++idx) {
                    func(args[idx]);
                }
            },
                                 i * diff, std::min<unsigned long>((i + 1) * diff, args.size()));
        }
        for (auto &th: threads)
            if (th.joinable())
                th.join();
    }

    template<typename F, typename T, typename R = std::invoke_result_t<F, T>>
    auto map_sp(F &&func, std::vector<T> &&args) -> std::enable_if_t<std::is_same_v<R, void>, void> {
        std::vector<std::thread> threads;
        auto diff = (args.size() - 1) / thread_num + 1;
        for (auto i = 0ul; i < thread_num; ++i) {
            threads.emplace_back([&args, &func](auto bg, auto ed) {
                for (auto idx = bg; idx < ed; ++idx) {
                    func(std::move(args[idx]));
                }
            },
                                 i * diff, std::min<unsigned long>((i + 1) * diff, args.size()));
        }
        for (auto &th: threads)
            if (th.joinable())
                th.join();
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

public:
    bool activateTransactionMerging = true;
    bool activateSubtreeUtilityPruning = true;
    long MAXIMUM_SIZE_MERGING = 1000;
    bool use_parallel_sort = true;
};


}// namespace dphim
