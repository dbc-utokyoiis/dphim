#include <dphim/dpefim.hpp>
#include <dphim/parse.hpp>

#include <nova/numa_aware_scheduler.hpp>
#include <nova/parallel_sort.hpp>

namespace dphim {

template<typename I>
auto DPEFIM::run_impl() -> nova::task<> {
    timer_start();

    auto [database, mI] = co_await parseTransactions();
    maxItem = mI;

    std::cout << "database.size(): " << database.size() << std::endl;
    std::cout << "Partition size: " << database.partition_size() << std::endl;
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
    co_await for_each_batched(
            database,
            [this](Transaction &transaction) {
                for (auto &[item, util]: transaction)
                    item = oldNameToNewNames[item];
                transaction.erase_if([&](const auto &p) { return p.first == 0; });// remove invalid name
                std::sort(transaction.begin(), transaction.end(),
                          [&](const auto &l, const auto &r) { return l.first < r.first; });
            },
            [this](auto node_id) { return schedule(node_id); },
            500);

    using std::erase_if;
    erase_if(database, [](const Transaction &t) { return t.empty(); });

    auto comp = [](auto &l, auto &r) {
        return std::lexicographical_compare(
                r.rbegin(), r.rend(), l.rbegin(), l.rend(), [](auto &l, auto &r) { return l.first < r.first; });
    };
    if (use_parallel_sort) {
        co_await nova::parallel_sort(database.begin(), database.end(), comp, [this] { return schedule(); });
    } else {
        std::sort(database.begin(), database.end(), comp);
    }

    auto SU = co_await calcFirstSU(database);

    I itemsToExplore;
    for (auto item: itemsToKeep)
        if (SU[item] >= min_util)
            itemsToExplore.emplace_back(item);
    time_point("Build");

    sched_no_await = false;
    co_await search({}, std::move(database), std::move(itemsToKeep), std::move(itemsToExplore));
    time_point("Search");
}

auto DPEFIM::run() -> nova::task<> {
    co_await schedule(0);

    if (pmem_alloc_type == PmemAllocType::AEK) {
#ifdef DPHIM_PMEM
        co_await run_impl<std::vector<Item, local_pmem_allocator<Item>>>();
#else
        throw std::runtime_error("pmem is unsupported ");
#endif
    } else {
        co_await run_impl<std::vector<Item>>();
    }
    co_return;
}

template<typename D>
auto DPEFIM::calcFirstSU(D &database) -> nova::task<std::vector<Utility>> {
    //    if constexpr (std::is_same_v<std::remove_cvref_t<D>, Database>) {
    std::vector<Utility> utils(maxItem + 1);
    co_await for_each_batched(
            database, [&](auto &transaction) {
            Utility sumSU = 0;
            for (auto i = transaction.rbegin(); i != transaction.rend(); ++i) {
                auto [item, utility] = *i;
                sumSU += utility;
                auto p = reinterpret_cast<std::atomic<Utility> *>(&utils[item]);
                std::launder(p)->fetch_add(sumSU, std::memory_order_relaxed);
            } }, [this](auto i) { return schedule(i); }, 500);
    co_return utils;
    //    } else {
    //        std::vector<std::vector<Utility>> utils(database.size());
    //        co_await scatter(database, [this, &utils](auto & /*db*/, auto node) {
    //            utils[node].resize(maxItem + 1);
    //        });
    //        co_await parallel_by_static_partitioning(database, [&](Transaction &transaction, int node) {
    //            Utility sumSU = 0;
    //            for (auto i = transaction.rbegin(); i != transaction.rend(); ++i) {
    //                auto [item, utility] = *i;
    //                sumSU += utility;
    //                auto p = reinterpret_cast<std::atomic<Utility> *>(&utils[node][item]);
    //                std::launder(p)->fetch_add(sumSU, std::memory_order_relaxed);
    //            }
    //        });
    //        auto cur_node_id = sched->get_current_node_id().value();
    //        auto max_node_id = sched->get_max_node_id().value();
    //        for (auto node = 0; node <= max_node_id; ++node) {
    //            if (node == cur_node_id)
    //                continue;
    //            for (auto i = 0ul; i <= maxItem; ++i) {
    //                utils[cur_node_id][i] += utils[node][i];
    //            }
    //        }
    //        co_return utils[cur_node_id];
    //    }
}

template<typename D, typename I, typename I2>
auto DPEFIM::searchX(int j, I &&prefix, const D &transactionsOfP, I2 &&itemsToKeep, I2 &&itemsToExplore) -> nova::task<> {

    if (itemsToExplore.size() > 1)
        co_await schedule();

    auto x = itemsToExplore[j];

    Utility utilityPx = 0;
    D transactionPx(transactionsOfP.partitions().size());

    auto ret = co_await partition_map(
            transactionsOfP,
            [&, x](auto &db, auto node) { return calcUtilityAndNextDB(x, db, node); },
            [this](auto node) { return schedule(node); } /*, cond*/);

    for (std::size_t nid = 0; nid < ret.size(); ++nid) {
        utilityPx += ret[nid].first;
        transactionPx.get(nid) = std::move(ret[nid].second);
    }

    auto makeNewItems = [j, min_util = min_util](auto &&ub, auto &&K) {
        std::remove_cvref_t<I2> newK, newE;
        newK.reserve(K.size() - j + 1);
        newE.reserve(K.size() - j + 1);
        for (int i = j + 1; i < int(K.size()); ++i) {
            auto item = K[i];
            if (ub.getSU(item) >= min_util) {
                newK.emplace_back(item);
                newE.emplace_back(item);
            } else if (ub.getLU(item) >= min_util) {
                newK.emplace_back(item);
            }
        }
        return std::make_tuple(std::move(newK), std::move(newE));
    };

    std::remove_cvref_t<I2> newK, newE;
    auto &ub = calcUpperBounds<false>(j, transactionPx.get(0), itemsToKeep);
    for (std::size_t nid = 1; nid < transactionPx.partition_size(); ++nid) {
        auto &db = transactionPx.get(nid);
        calcUpperBoundsImpl(ub, j, db, itemsToKeep);
    }
    std::tie(newK, newE) = makeNewItems(ub, itemsToKeep);

    if (utilityPx >= min_util || !newE.empty()) {
        auto p = prefix;
        p.push_back(newNameToOldNames[x]);
        if (utilityPx >= min_util) {
            writeOutput(p, utilityPx);
        }
        if (newE.size() == 1) {
            incCandidateCount(1);
            co_await searchX(0, std::move(p), transactionPx, std::move(newK), std::move(newE));
        } else if (!newE.empty()) {
            co_await search(std::move(p), transactionPx, std::move(newK), std::move(newE));
        }
    }
}

template<typename... Args>
__attribute__((noinline)) auto my_lower_bound(Args &&...args) {
    return std::lower_bound(std::forward<Args>(args)...);
}

template<typename T>
auto DPEFIM::calcUtilityAndNextDB(Item x, T &&db, [[maybe_unused]] int node) -> std::pair<Utility, std::remove_cvref_t<T>> {
    // remote read: \sum log(transaction.size()) + transaction.size()
    using DB = std::remove_cvref_t<T>;

    Utility utilityPx = 0;
    DB transactionPx;
    transactionPx.reserve(db.size());

    int consecutive_merge_count = 0;
    Transaction previousTransaction;

    for (auto &transaction: db) {
        // read: log(transaction.size())
        auto iterX = my_lower_bound(
                transaction.begin(), transaction.end(), Transaction::Elem{x, 0},
                [](const auto &l, const auto &r) { return l.first < r.first; });
        if (iterX == transaction.end() || iterX->first != x) { continue; }
        if (iterX + 1 == transaction.end()) {
            utilityPx += iterX->second + transaction.prefix_utility;
        } else {
            auto projected = transaction.projection(iterX);
            utilityPx += projected.prefix_utility;
            if (DPEFIM::MAXIMUM_SIZE_MERGING >= std::distance(iterX, transaction.end())) {
                if (!previousTransaction) {
                    previousTransaction = std::move(projected);
                } else if (projected.compare_extension(previousTransaction)) {// read: 2 * transaction.size() ?
                    if (consecutive_merge_count == 0) {
                        if (pmem_alloc_type != PmemAllocType::None) {
#ifdef DPHIM_PMEM
                            auto pmem_allocator = get_pmem_allocator(node < 0 ? std::nullopt : std::optional(node));
                            previousTransaction = previousTransaction.clone(
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
                            previousTransaction = previousTransaction.clone();// write
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
                transactionPx.push_back(std::move(projected));
            }
        }
    }

    if (previousTransaction)
        transactionPx.push_back(std::move(previousTransaction));

    return std::make_pair(utilityPx, std::move(transactionPx));
}


template<typename D, typename I>
void DPEFIM::calcUpperBoundsImpl(UtilityBinArray &ub, std::size_t j, const D &db, const I &itemsToKeep) const {
    // remote read: \sum transaction.size()
    // local write: \sum transaction.size()
    if (ub.size() == 0)
        ub.reset(itemsToKeep[j], itemsToKeep.back());
    for (const auto &transaction: db) {
        Utility sum_remaining_utility = 0;
        auto ed = itemsToKeep.end();
        for (auto it = transaction.rbegin(); it != transaction.rend(); ++it) {
            auto [item, utility] = *it;
            auto lb = my_lower_bound(itemsToKeep.begin(), ed, item);
            if (lb != ed && *lb == item) {// contains
                sum_remaining_utility += utility;
                ub.getSU(item) += sum_remaining_utility + transaction.prefix_utility;
                ub.getLU(item) += transaction.transaction_utility + transaction.prefix_utility;
            }
            ed = lb;
        }
    }
}

template<bool no_use_thread_local, typename D, typename I>
auto DPEFIM::calcUpperBounds(std::size_t j, const D &transactionsPx, const I &itemsToKeep) const
        -> std::conditional_t<no_use_thread_local, UtilityBinArray, UtilityBinArray &> {
    if constexpr (no_use_thread_local) {
        UtilityBinArray utilityBinArray;
        utilityBinArray.reset(itemsToKeep[j], itemsToKeep.back());
        calcUpperBoundsImpl(utilityBinArray, j, transactionsPx, itemsToKeep);
        return utilityBinArray;
    } else {
        static thread_local UtilityBinArray utilityBinArray;
        utilityBinArray.reset(itemsToKeep[j], itemsToKeep.back());
        calcUpperBoundsImpl(utilityBinArray, j, transactionsPx, itemsToKeep);
        return utilityBinArray;
    }
}
}// namespace dphim