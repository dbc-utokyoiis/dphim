#include <dphim/dpefim.hpp>
#include <dphim/parse.hpp>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <nova/numa_aware_scheduler.hpp>
#include <nova/parallel_sort.hpp>

namespace dphim {

auto DPEFIM::run() -> nova::task<> {
    co_await schedule(0);

    if (pmem_alloc_type == PmemAllocType::AEK) {
#ifdef DPHIM_PMEM
        if (use_parted_database) {
            co_await run_impl<true, std::vector<Item, local_pmem_allocator<Item>>>();
        } else {
            co_await run_impl<false, std::vector<Item, local_pmem_allocator<Item>>>();
        }
#else
        throw std::runtime_error("pmem is unsupported ");
#endif
    } else {
        if (use_parted_database) {
            co_await run_impl<true, std::vector<Item>>();
        } else {
            co_await run_impl<false, std::vector<Item>>();
        }
    }
}

template<bool do_partitioning, typename I>
auto DPEFIM::run_impl() -> nova::task<> {

    timer_start();

    auto [database, mI] = co_await parseTransactions<do_partitioning>();
    maxItem = mI;

    std::cout << "Transactions: " << database.size() << std::endl;
    std::cout << "maxItem: " << maxItem << std::endl;
    time_point("parse");

    auto [LU, itemsToKeep] = co_await calcTWU<I>(database, maxItem);
    std::cout << "itemsToKeep.size(): " << itemsToKeep.size() << std::endl;
    time_point("calcTWU");

    // set new name
    oldNameToNewNames.resize(maxItem + 1, 0);
    newNameToOldNames.resize(maxItem + 1, 0);
    Item currentName = 1;
    for (auto &item: itemsToKeep) {
        oldNameToNewNames[item] = currentName;
        newNameToOldNames[currentName] = item;
        item = currentName;
        currentName++;
    }
    maxItem = currentName;

    // remove unpromising elems and rename elems from old names to new names
    co_await for_each_dp(database, [this](Transaction &transaction, [[maybe_unused]] int node) {
        for (auto &[item, util]: transaction)
            item = oldNameToNewNames[item];
        transaction.erase_if([&](const auto &p) { return p.first == 0; });// remove invalid name
        std::sort(transaction.begin(), transaction.end(),
                  [&](const auto &l, const auto &r) { return l.first < r.first; });
    });

    co_await part_map_awaitable(
            database, [this](auto &db, auto /*node*/) -> nova::task<> {
                std::erase_if(db, [](const Transaction &t) { return t.empty(); });
                auto comp = [](const Transaction &l, const Transaction &r) {
                    return std::lexicographical_compare(
                            r.rbegin(), r.rend(), l.rbegin(), l.rend(),
                            [](const auto &l, const auto &r) { return l.first < r.first; });
                };
                if (use_parallel_sort) {
                    co_await nova::parallel_sort(db.begin(), db.end(), comp, [this] { return schedule(); });
                } else {
                    std::sort(db.begin(), db.end(), comp);
                }
            },
            0);

    auto SU = co_await calcFirstSU(database);

    I itemsToExplore;
    for (auto item: itemsToKeep)
        if (SU[item] >= min_util)
            itemsToExplore.push_back(item);
    time_point("Build");

    sched_no_await = false;
    co_await search({}, std::move(database), std::move(itemsToKeep), std::move(itemsToExplore));
    time_point("Search");
}

template<typename D>
auto DPEFIM::calcFirstSU(D &database) -> nova::task<std::vector<Utility>> {

    if constexpr (std::is_same_v<std::remove_cvref_t<D>, Database>) {
        std::vector<Utility> utils(maxItem + 1);
        co_await for_each_dp(database, [&](auto &transaction, [[maybe_unused]] auto node) {
            Utility sumSU = 0;
            for (auto i = transaction.rbegin(); i != transaction.rend(); ++i) {
                auto [item, utility] = *i;
                sumSU += utility;
                auto p = reinterpret_cast<std::atomic<Utility> *>(&utils[item]);
                std::launder(p)->fetch_add(sumSU, std::memory_order_relaxed);
            }
        });
        co_return utils;
    } else {
        std::vector<std::vector<Utility>> utils(database.size());
        co_await part_map(database, [this, &utils](auto & /*db*/, auto node) {
            utils[node].resize(maxItem + 1);
        });
        co_await for_each_dp(database, [&](Transaction &transaction, int node) {
            Utility sumSU = 0;
            for (auto i = transaction.rbegin(); i != transaction.rend(); ++i) {
                auto [item, utility] = *i;
                sumSU += utility;
                auto p = reinterpret_cast<std::atomic<Utility> *>(&utils[node][item]);
                std::launder(p)->fetch_add(sumSU, std::memory_order_relaxed);
            }
        });
        auto cur_node_id = sched->get_current_node_id().value();
        auto max_node_id = sched->get_max_node_id().value();
        for (auto node = 0; node <= max_node_id; ++node) {
            if (node == cur_node_id)
                continue;
            for (auto i = 0ul; i <= maxItem; ++i) {
                utils[cur_node_id][i] += utils[node][i];
            }
        }
        co_return utils[cur_node_id];
    }
}

auto DPEFIM::calcUtilityAndNextDB(Item x, const std::pair<DPEFIM::Database::const_iterator, DPEFIM::Database::const_iterator> transactionRange, [[maybe_unused]] int node)
        -> std::pair<Utility, DPEFIM::Database> {
    auto [bgn, ed] = transactionRange;

    Utility utilityPx = 0;
    std::vector<Transaction> transactionPx;
    transactionPx.reserve(std::distance(bgn, ed));

    int consecutive_merge_count = 0;
    Transaction previousTransaction;

    for (auto it = bgn; it != ed; ++it) {
        const auto &transaction = *it;
        auto iterX = std::lower_bound(
                transaction.begin(), transaction.end(), Transaction::Elem{x, 0},
                [](const auto &l, const auto &r) { return l.first < r.first; });
        if (iterX == transaction.end() || iterX->first != x) { continue; }
        if (iterX + 1 == transaction.end()) {
            utilityPx += iterX->second + transaction.prefix_utility;
        } else {
            if (DPEFIM::MAXIMUM_SIZE_MERGING >= std::distance(iterX, transaction.end())) {
                auto projected = transaction.projection(iterX);
                utilityPx += projected.prefix_utility;

                if (!previousTransaction) {
                    previousTransaction = std::move(projected);
                } else if (projected.compare_extension(previousTransaction)) {
                    if (consecutive_merge_count == 0) {
                        if (pmem_alloc_type != PmemAllocType::None) {
#ifdef DPHIM_PMEM
                            auto pmem_allocator = get_pmem_allocator(node < 0 ? std::nullopt : std::optional(node));
                            previousTransaction = previousTransaction.clone(
                                    [=](auto size) { return pmem_allocator->alloc(size); },
                                    [=]([[maybe_unused]] auto size) {
                                        return [=](auto *p) {
                                            using T = std::remove_pointer_t<std::remove_cvref_t<decltype(p)>>;
                                            p->~T();
                                            pmem_allocator->dealloc(p);
                                        };
                                    });
#else
                            throw std::runtime_error("pmem is unsupported ");
#endif
                        } else {
                            previousTransaction = previousTransaction.clone();
                        }
                    }
                    previousTransaction.merge(std::move(projected));
                    consecutive_merge_count++;
                } else {
                    transactionPx.push_back(std::move(previousTransaction));
                    previousTransaction = std::move(projected);
                    consecutive_merge_count = 0;
                }
            } else {
                auto projected = transaction.projection(iterX);
                utilityPx += projected.prefix_utility;
                transactionPx.push_back(std::move(projected));
            }
        }
    }

    if (previousTransaction)
        transactionPx.push_back(std::move(previousTransaction));

    return std::make_pair(utilityPx, std::move(transactionPx));
}

template<typename D, typename I>
auto DPEFIM::searchX(int j, const I &prefix, const D &transactionsOfP, const I &itemsToKeep, const I &itemsToExplore) -> nova::task<> {

    if (itemsToExplore.size() > 1)
        co_await schedule();

    constexpr bool do_partitioning = std::is_same_v<D, std::vector<DPEFIM::Database>>;

    auto x = itemsToExplore[j];
    Utility utilityPx = 0;
    D transactionPx;

    if constexpr (do_partitioning) {
        transactionPx.resize(transactionsOfP.size());
        for (auto &&[pid, p]: co_await part_map(
                     transactionsOfP,
                     [this, x](auto &db, [[maybe_unused]] auto node) { return calcUtilityAndNextDB(x, {db.begin(), db.end()}, node); })) {
            utilityPx += p.first;
            transactionPx[pid] = std::move(p.second);
        }
    } else {
        std::tie(utilityPx, transactionPx) = calcUtilityAndNextDB(x, {transactionsOfP.begin(), transactionsOfP.end()});
    }


    I newItemsToKeep, newItemsToExplore;
    newItemsToKeep.reserve(itemsToKeep.size() - j + 1);
    newItemsToExplore.reserve(itemsToKeep.size() - j + 1);

    if constexpr (do_partitioning) {
        UtilityBinArray ub;
        ub.reset(itemsToKeep[j], itemsToKeep.back());

        for (auto &&[pid, ret]: co_await part_map(
                     transactionPx,
                     [this, j, &itemsToKeep](auto &db, [[maybe_unused]] auto node) {
                         return this->calcUpperBounds<true>(j, db, itemsToKeep);
                     })) {
            ub += ret;
        }

        for (int i = j + 1; i < int(itemsToKeep.size()); ++i) {
            auto item = itemsToKeep[i];
            if (ub.getSU(item) >= min_util) {
                newItemsToKeep.push_back(item);
                newItemsToExplore.push_back(item);
            } else if (ub.getLU(item) >= min_util) {
                newItemsToKeep.push_back(item);
            }
        }
    } else {
        decltype(auto) ub = calcUpperBounds<false>(j, transactionPx, itemsToKeep);

        for (int i = j + 1; i < int(itemsToKeep.size()); ++i) {
            auto item = itemsToKeep[i];
            if (ub.getSU(item) >= min_util) {
                newItemsToKeep.push_back(item);
                newItemsToExplore.push_back(item);
            } else if (ub.getLU(item) >= min_util) {
                newItemsToKeep.push_back(item);
            }
        }
    }

    if (utilityPx >= min_util || !newItemsToExplore.empty()) {
        auto p = prefix;
        p.push_back(newNameToOldNames[x]);
        if (utilityPx >= min_util) {
            writeOutput(p, utilityPx);
        }
        if (newItemsToExplore.size() == 1) {
            incCandidateCount(1);
            co_await searchX(0, p, transactionPx, newItemsToKeep, newItemsToExplore);
        } else if (!newItemsToExplore.empty()) {
            co_await search(p, transactionPx, newItemsToKeep, newItemsToExplore);
        }
    }
}

template<bool do_partitioning, typename D, typename I>
auto DPEFIM::calcUpperBounds(std::size_t j, const D &transactionsPx, const I &itemsToKeep) const
        -> std::conditional_t<do_partitioning, UtilityBinArray, const UtilityBinArray &> {

    auto loop = [&](auto &utilityBinArray, const auto &db) {
        const auto &tmpItems = itemsToKeep;
        for (const auto &transaction: db) {
            Utility sum_remaining_utility = 0;
            auto ed = tmpItems.end();
            for (auto it = transaction.rbegin(); it != transaction.rend(); ++it) {
                auto [item, utility] = *it;
                auto lb = std::lower_bound(tmpItems.begin(), ed, item);
                if (lb != ed && *lb == item) {// contains
                    sum_remaining_utility += utility;
                    utilityBinArray.getSU(item) += sum_remaining_utility + transaction.prefix_utility;
                    utilityBinArray.getLU(item) += transaction.transaction_utility + transaction.prefix_utility;
                }
                ed = lb;
            }
        }
    };

    if constexpr (do_partitioning) {
        UtilityBinArray utilityBinArray;
        utilityBinArray.reset(itemsToKeep[j], itemsToKeep.back());
        loop(utilityBinArray, transactionsPx);
        return utilityBinArray;
    } else {
        static thread_local UtilityBinArray utilityBinArray;
        utilityBinArray.reset(itemsToKeep[j], itemsToKeep.back());
        loop(utilityBinArray, transactionsPx);
        return utilityBinArray;
    }
}
}// namespace dphim