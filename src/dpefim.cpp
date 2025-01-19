#include <dphim/dpefim.hpp>
#include <dphim/parse.hpp>

#include <nova/jemalloc.hpp>
#include <nova/numa_aware_scheduler.hpp>
#include <nova/parallel_sort.hpp>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace dphim {

template<typename I>
auto DPEFIM::run_impl() -> nova::task<> {

    if (is_debug_mode()) {
        std::cerr << "scatter_type: " << scatter_type << std::endl;
        std::cerr << "speculation thresholds: " << std::endl;
        std::cerr << "  alpha1: " << thresholds.step1_scatter_alloc_threshold << std::endl;
        std::cerr << "  beta1: " << thresholds.step1_task_migration_threshold << std::endl;
        std::cerr << "  beta2: " << thresholds.step2_task_migration_threshold << std::endl;
        std::cerr << "  alpha3: " << thresholds.step3_scatter_alloc_threshold << std::endl;
        std::cerr << "  beta3: " << thresholds.step3_task_migration_threshold << std::endl;
    }

    timer_start();

    auto [database, mI] = co_await parseTransactions([this](std::size_t fsize) {
        auto ret = fsize > this->thresholds.step1_scatter_alloc_threshold
                           ? sched->get_max_node_id().value_or(0) + 1
                           : 1;
        if (is_debug_mode()) {
            std::cerr << "  input file size: " << fsize << " bytes\n";
            std::cerr << "  alpha1 threshold: " << thresholds.step1_scatter_alloc_threshold << '\n';
            std::cerr << "  partition num: " << ret << std::endl;
        }
        return ret;
    });
    partition_num = database.partition_num();
    maxItem = mI;


    time_point("parse");
    if (is_debug_mode()) {
        std::cerr << "  # of transactions: " << database.size() << std::endl;
        std::cerr << "  # of partitions: " << database.partition_num() << std::endl;
        std::cerr << "  maxItem: " << maxItem << std::endl;
    }

    auto [LU, itemsToKeep] = co_await calcTWU<I>(database, maxItem);
    time_point("calcTWU");
    if (is_debug_mode()) {
        std::cerr << " # of itemsToKeep: " << itemsToKeep.size() << std::endl;
        for (std::size_t i = 0; i < database.partition_num(); ++i) {
            std::cerr << "  database@node" << i << ": "
                      << "size=" << database.get(i).size()
                      << "(" << database.get(i).get_sum_value() / 1000 << " kB)" << std::endl;
        }
    }

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
            [this](Transaction &transaction, auto /*part_id*/) {
                for (auto &[item, util]: transaction)
                    item = oldNameToNewNames[item];
                transaction.erase_if([&](const auto &p) { return p.first == 0; });// remove invalid name
                std::sort(transaction.begin(), transaction.end(),
                          [&](const auto &l, const auto &r) { return l.first < r.first; });
            },
            [this](auto node_id, auto bg, auto ed) {
                auto range = PrefixSumRange(bg, ed);
                if (range.get_sum_value() > thresholds.step2_task_migration_threshold) {
                    return schedule(node_id);
                } else {
                    return schedule();
                }
            },
            500);

    using std::erase_if;
    auto pre_size = database.size();
    erase_if(database, [](const Transaction &t) { return t.empty(); });
    if (is_debug_mode()) {
        std::cerr << "remove item with TWU under minutil" << std::endl;
        std::cerr << "  # of transactions: " << pre_size << " -> " << database.size() << std::endl;
    }

    auto comp = [](auto &l, auto &r) {
        return std::lexicographical_compare(
                r.rbegin(), r.rend(), l.rbegin(), l.rend(), [](auto &l, auto &r) { return l.first < r.first; });
    };

    if (is_debug_mode()) {
        std::cerr << "sort transactions" << std::endl;
        std::cerr << "  " << (use_parallel_sort ? "parallel sort" : "simple sort") << std::endl;
    }

    if (use_parallel_sort) {
        std::vector<nova::task<>> tasks;
        for (std::size_t i = 0; i < database.partitions().size(); ++i) {
            auto &part = database.get(i);
            tasks.push_back(nova::parallel_sort(
                    part.begin(), part.end(), comp, [](auto self, auto node) { return self->schedule(node); }, this, i));
        }
        co_await nova::when_all(std::move(tasks));
    } else {
        std::sort(database.begin(), database.end(), comp);
    }

    for (std::size_t i = 0; i < database.partitions().size(); ++i) {
        auto &part = database.get(i);
        part.recalc();
    }
    auto SU = co_await calcFirstSU(database);

    I itemsToExplore;
    for (auto item: itemsToKeep)
        if (SU[item] >= min_util)
            itemsToExplore.emplace_back(item);
    time_point("Build");

    if (is_debug_mode()) {
        std::cerr << "  # of itemsToExplore: " << itemsToExplore.size() << std::endl;
        for (std::size_t i = 0; i < database.partition_num(); ++i) {
            std::cerr << "  database@node" << i << ": "
                      << "size=" << database.get(i).size()
                      << "(" << database.get(i).get_sum_value() / 1000 << " kB)" << std::endl;
        }
    }

    // this->repartition(database);

    if (is_debug_mode()) {
        std::cerr << "Thresholds: " << std::endl;
        std::cerr << "  scatter_alloc_threshold: " << thresholds.step3_scatter_alloc_threshold << std::endl;
        std::cerr << "  task_migration_threshold: " << thresholds.step3_task_migration_threshold << std::endl;
        std::cerr << "  stop_scatter_alloc_depth: " << thresholds.step3_stop_scatter_alloc_depth << std::endl;
        std::cerr << "  stop_task_migration_depth: " << thresholds.step3_stop_task_migration_depth << std::endl;
    }

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

    if (is_debug_mode()) {
        std::cerr << "calcFirstSU" << std::endl;
        std::cerr << " scatter threshold: " << thresholds.step2_task_migration_threshold << std::endl;
    }

    std::vector<Utility> utils(maxItem + 1);

    co_await for_each_batched(
            database, [&](auto &transaction, auto /*part_id*/) {
            Utility sumSU = 0;
            for (auto i = transaction.rbegin(); i != transaction.rend(); ++i) {
                auto [item, utility] = *i;
                sumSU += utility;
                // auto p = reinterpret_cast<std::atomic<Utility> *>(&utils[item]);
                // std::launder(p)->fetch_add(sumSU, MEM_ORDER_RELAXED);
                std::atomic_ref(utils[item]).fetch_add(sumSU, MEM_ORDER_RELAXED);
            } },
            [this](auto i, auto bg, auto ed) {
                auto range = PrefixSumRange(bg, ed);
                if (range.get_sum_value() > thresholds.step2_task_migration_threshold) {
                    return schedule(i);
                } else {
                    return schedule();
                }
            },
            500);
    co_return utils;
}

template<typename D, typename I, typename I2>
auto DPEFIM::searchX(int j, I &&prefix, const D &transactionsOfP, I2 &&itemsToKeep, I2 &&itemsToExplore) -> nova::task<> {

    if (itemsToExplore.size() > 1)
        co_await schedule();

    auto x = itemsToExplore[j];
    auto depth = prefix.size();

    Utility utilityPx = 0;
    D transactionPx(transactionsOfP.partition_num());

    for (auto &&[util, db]:
         co_await partition_map(
                 transactionsOfP,
                 [this, depth, x](auto &db, auto node) {
                     return calcUtilityAndNextDB(x, db, node, depth < thresholds.step3_stop_task_migration_depth);
                 },
                 [this]([[maybe_unused]] auto &part, std::size_t node) {
                     return schedule(static_cast<int>(node));
                 },
                 [this, depth](auto &part, auto /*id*/) {
                     return depth < thresholds.step3_stop_task_migration_depth &&
                            part.get_sum_value() > thresholds.step3_task_migration_threshold;
                 })) {
        utilityPx += util;
        transactionPx.merge(std::move(db));
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

    UtilityBinArray ub;
    for (std::size_t nid = 0; nid < transactionPx.partition_num(); ++nid) {
        auto &db = transactionPx.get(nid);
        if (depth < thresholds.step3_stop_task_migration_depth &&
            db.get_sum_value() > thresholds.step3_task_migration_threshold)
            co_await schedule(nid);
        calcUpperBoundsImpl(ub, j, db, itemsToKeep);
    }

    std::remove_cvref_t<I2> newK, newE;
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
auto DPEFIM::calcUtilityAndNextDB(Item x, T &&db, int node, bool allow_scatter) -> std::pair<Utility, Database> {
    std::size_t alloc_size = 0;

    Utility utilityPx = 0;
    Database ret(partition_num);

    [[maybe_unused]] auto try_aggresive_merge = [&ret](const Transaction &projected) {
        for (std::size_t i = 0; i < ret.size(); ++i) {
            auto &tra = ret[i];
            if (tra && projected.compare_extension(tra)) {
                tra.merge(projected);
                return true;
            }
        }
        return false;
    };

    int consecutive_merge_count = 0;
    Transaction prevTransaction;
    int allocNode = node;

    for (auto &transaction: db) {
        auto iterX = my_lower_bound(
                transaction.begin(), transaction.end(), Transaction::Elem{x, 0},
                [](const auto &l, const auto &r) { return l.first < r.first; });
        if (iterX == transaction.end() || iterX->first != x) { continue; }
        if (iterX + 1 == transaction.end()) {
            utilityPx += iterX->second + transaction.prefix_utility;
        } else {
            auto projected = transaction.projection(iterX);
            utilityPx += projected.prefix_utility;
            if (!prevTransaction) {
                prevTransaction = std::move(projected);
                // } else if (try_aggresive_merge(projected)) {
                // pass
            } else if (projected.compare_extension(prevTransaction)) {
                if (consecutive_merge_count == 0) {
                    if (allow_scatter && (alloc_size > this->thresholds.step3_scatter_alloc_threshold / partition_num)) {
                        // scatter
                        allocNode = (allocNode + 1) % partition_num;
                        prevTransaction = this->cloneTransaction(prevTransaction, allocNode);
                        addMalloc(prevTransaction.bytes());
                        alloc_size += prevTransaction.bytes();
                    } else {
                        // non scatter
                        prevTransaction = this->cloneTransaction(prevTransaction, std::nullopt);
                        allocNode = node;
                        addMalloc(prevTransaction.bytes());
                        alloc_size += prevTransaction.bytes();
                    }
                }
                prevTransaction.merge(std::move(projected));
                consecutive_merge_count++;
            } else {
                ret.get(allocNode).push_back(std::move(prevTransaction));
                prevTransaction = std::move(projected);
                consecutive_merge_count = 0;
            }
        }
    }

    if (prevTransaction)
        ret.get(allocNode).push_back(std::move(prevTransaction));

    return std::make_pair(utilityPx, std::move(ret));
}


template<typename D, typename I>
void DPEFIM::calcUpperBoundsImpl(UtilityBinArray &ub, std::size_t j, const D &db, const I &itemsToKeep) const {
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
        thread_local UtilityBinArray utilityBinArray;
        utilityBinArray.reset(itemsToKeep[j], itemsToKeep.back());
        calcUpperBoundsImpl(utilityBinArray, j, transactionsPx, itemsToKeep);
        return utilityBinArray;
    }
}

double DPEFIM::balanceCheck(const Database &database) const {
    std::size_t min = std::numeric_limits<std::size_t>::max();
    std::size_t max = 0;
    for (std::size_t i = 0; i < database.partition_num(); ++i) {
        min = std::min(min, database.get(i).get_sum_value());
        max = std::max(max, database.get(i).get_sum_value());
    }
    return static_cast<double>(max) / min;
}

void DPEFIM::repartition(Database &database) {
    using namespace std::chrono;
    auto st = high_resolution_clock::now();
    auto ranges = database.balanced_partitions([](auto bg, auto ed) {
        return PrefixSumRange(bg, ed).get_sum_value();
    });
    database.repartition(ranges, [this](auto &vec, auto &&v, std::size_t src, std::size_t dest) {
        if (src == dest) {
            vec.push_back(std::forward<decltype(v)>(v));
        } else {
            auto cpu = sched->get_corresponding_cpu_id(dest);
            vec.push_back(v.clone([this, cpu](auto size) {
                if (cpu) {
                    return nova::malloc_on_thread(size, *cpu);
                } else {
                    return std::malloc(size);
                }
            }));
        }
    });
    if (is_debug_mode()) {
        std::cerr << "repartition: "
                  << duration_cast<microseconds>(high_resolution_clock::now() - st).count()
                  << " us" << std::endl;
        for (std::size_t i = 0; i < database.partition_num(); ++i) {
            std::cerr << "  database@node" << i << ": "
                      << "size=" << database.get(i).size()
                      << "(" << database.get(i).get_sum_value() / 1000 << " kB)" << std::endl;
        }
    }
}

}// namespace dphim
