#include <dphim/efim.hpp>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <execution>
#include <thread>
#include <vector>

#include <boost/sort/sort.hpp>

#include <numa.h>
#include <sched.h>

namespace dphim {

void EFIM::run() {
    timer_start();

    auto [database, mI] = parseTransactions(input_path);
    maxItem = mI;

    std::vector<Item> itemsToKeep;
    std::tie(utilityBinArrayLU, itemsToKeep) = calcTWU(database, maxItem, min_util);
    time_point("calcTWU");

    oldNameToNewNames.resize(maxItem + 1, 0);
    newNameToOldNames.resize(maxItem + 1, 0);

    int currentName = 1;
    for (auto &item: itemsToKeep) {
        oldNameToNewNames[item] = currentName;
        newNameToOldNames[currentName] = item;
        item = currentName;
        currentName++;
    }
    maxItem = currentName;

    if (thread_num <= 1) {
        for (auto &transaction: database) {
            for (auto &[item, util]: transaction)
                item = oldNameToNewNames[item];
            transaction.erase_if([&](const auto &p) { return p.first == 0; });// remove invalid name
            std::sort(transaction.begin(), transaction.end(),
                      [&](const auto &l, const auto &r) { return l.first < r.first; });
        }
    } else {
        std::vector<std::thread> threads;
        auto diff = (database.size() - 1) / thread_num + 1;
        for (auto i = 0u; i < database.size(); i += diff) {
            threads.emplace_back([&](auto bgn, auto ed) {
                for (auto it = bgn; it != ed; ++it) {
                    auto &transaction = *it;
                    for (auto &[item, util]: transaction)
                        item = oldNameToNewNames[item];
                    transaction.erase_if([&](const auto &p) { return p.first == 0; });// remove invalid name
                    std::sort(transaction.begin(), transaction.end(),
                              [&](const auto &l, const auto &r) { return l.first < r.first; });
                }
            },
                                 database.begin() + i,//
                                 database.begin() + std::min<long>(i + diff, database.size()));
        }

        for (auto &t: threads)
            t.join();
    }

    std::erase_if(database, [](const Transaction &t) { return t.empty(); });

    auto comp = [](const Transaction &l, const Transaction &r) {
        return std::lexicographical_compare(r.rbegin(), r.rend(), l.rbegin(), l.rend(),
                                            [](const auto &l, const auto &r) { return l.first < r.first; });
    };

    if (use_parallel_sort) {
        boost::sort::sample_sort(database.begin(), database.end(), comp, thread_num);
    } else {
        std::sort(database.begin(), database.end(), comp);
    }
    utilityBinArraySU = calcFirstSU(database, itemsToKeep.size());

    Itemset itemsToExplore;
    for (auto item: itemsToKeep)
        if (utilityBinArraySU[item] >= min_util)
            itemsToExplore.push_back(item);
    time_point("Build");

    if (thread_num <= 1) {
        search(database, itemsToKeep, itemsToExplore, {});
    } else {
        std::vector<std::thread> threads;
        threads.reserve(thread_num);
        auto d = (itemsToExplore.size() - 1) / thread_num + 1;
        incCandidateCount(itemsToExplore.size());
        for (auto i = 0u; i < itemsToExplore.size(); i += d) {
            threads.emplace_back(
                    [this, i, ed = std::min<std::size_t>(i + d, itemsToExplore.size()), &itemsToKeep, &itemsToExplore, &database = database]() {
                        for (auto j = i; j < ed; ++j) {
                            searchX(j, database, itemsToKeep, itemsToExplore, {});
                        }
                    });
        }
        for (auto &t: threads)
            t.join();
    }

    time_point("Search");
}

std::pair<Transaction, Item> EFIM::parseTransactionOneLine(std::string line) {
    Transaction tra;
    Item max_item = 0;
    try {
        std::size_t i = 0, j = 0;
        std::vector<std::pair<Item, Utility>> buf;
        while (i < line.size()) {
            try {
                auto item = static_cast<Item>(std::stol(line.data() + i, &j));
                max_item = std::max(max_item, item);
                i += j + 1;
                buf.push_back({item, 0});
                if (line[i - 1] == ':')
                    break;
            } catch (std::invalid_argument &e) {
                std::cerr << e.what() << ": " << __LINE__ << " " << line << std::endl;
            }
        }

        if ([[maybe_unused]] auto pmem_allocator = get_pmem_allocator()) {
#ifdef DPHIM_PMEM
            tra.reserve(
                    buf.size(),
                    [=](auto size) { return pmem_allocator->alloc(size); },
                    [=]([[maybe_unused]] auto size) {
                        return [=](Transaction::Elem *p) {
                            p->~pair();
                            pmem_allocator->dealloc(p);
                        };
                    });
#endif
        } else {
            tra.reserve(buf.size());
        }

        for (auto &&e: buf)
            tra.push_back(std::move(e));

        try {
            tra.transaction_utility = static_cast<Utility>(std::stol(line.data() + i, &j));
        } catch (std::invalid_argument &e) {
            std::cerr << e.what() << ": " << __LINE__ << " " << line << std::endl;
        }

        i += j + 1;
        if (line[i - 1] != ':')
            throw std::runtime_error("failed to parse");

        for (auto &[item, util]: tra) {
            try {
                util = static_cast<Utility>(std::stol(line.data() + i, &j));
                i += j + 1;
            } catch (std::invalid_argument &e) {
                std::cerr << e.what() << ": " << __LINE__ << " " << line << std::endl;
            }
        }
    } catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
        std::cerr << "input: " << line << std::endl;
        throw;
    }
    return std::make_pair(std::move(tra), max_item);
}

std::pair<std::vector<Transaction>, Item> EFIM::parseTransactions(const std::string &input_path) {
    int fd = open(input_path.c_str(), O_RDONLY);
    if (fd == -1)
        throw std::runtime_error(strerror(errno));

    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    std::vector<std::string> lines;
    {
        std::string line;
        constexpr std::size_t buf_size = 4092;
        alignas(alignof(std::max_align_t)) char buf[buf_size];
        while (auto bytes_read = read(fd, buf, buf_size)) {
            if (bytes_read < 0)
                throw std::runtime_error("read failed");

            char *prev = buf, *p = nullptr;
            for (; (p = (char *) memchr(prev, '\n', (buf + bytes_read) - prev)); prev = p + 1) {
                line.insert(line.size(), prev, p - prev);
                if (auto comment_pos = line.find_first_of("%#@"); comment_pos != std::string::npos)
                    line.erase(comment_pos);
                if (line.empty())
                    continue;
                lines.push_back(std::move(line));
                line.clear();
            }
            line.insert(line.size(), prev, buf + buf_size - prev);
        }
    }

    std::vector<Transaction> res;
    Item maxItem = 0;
    for (auto &&[tra, mI]: map_sp([this](std::string &&line) { return parseTransactionOneLine(std::move(line)); }, std::move(lines))) {
        res.push_back(std::move(tra));
        maxItem = std::max(maxItem, mI);
    }

    return std::make_pair(std::move(res), maxItem);
}

void EFIM::searchX(int j, const std::vector<Transaction> &transactionsOfP, const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix) {
    auto x = itemsToExplore[j];

    std::vector<Transaction> transactionPx;
    Utility utilityPx = 0;

    Transaction previousTransaction{};
    int consecutive_merge_count = 0;

    for (auto &transaction: transactionsOfP) {
        auto iterX = std::lower_bound(
                transaction.begin(), transaction.end(), Transaction::Elem{x, 0},
                [](const auto &l, const auto &r) { return l.first < r.first; });

        // if the transaction contains 'x'
        if (iterX == transaction.end() || iterX->first != x) {
            continue;
        }
        if (iterX + 1 == transaction.end()) {
            utilityPx += iterX->second + transaction.prefix_utility;
        } else {
            if (activateTransactionMerging && MAXIMUM_SIZE_MERGING >= std::distance(iterX, transaction.end())) {
                auto projected = transaction.projection(iterX);
                utilityPx += projected.prefix_utility;

                if (!previousTransaction) {
                    previousTransaction = std::move(projected);
                } else if (projected.compare_extension(previousTransaction)) {
                    if (consecutive_merge_count == 0) {

                        if ([[maybe_unused]] auto pmem_allocator = get_pmem_allocator()) {
#ifdef DPHIM_PMEM
                            previousTransaction = previousTransaction.clone(
                                    [=](auto size) { return pmem_allocator->alloc(size); },
                                    [=]([[maybe_unused]] auto size) {
                                        return [=](auto *p) {
                                            using T = std::remove_pointer_t<std::remove_cvref_t<decltype(p)>>;
                                            p->~T();
                                            pmem_allocator->dealloc(p);
                                        };
                                    });
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

    if (previousTransaction) {
        transactionPx.push_back(std::move(previousTransaction));
    }

    decltype(auto) UB = calcUpperBounds(transactionPx, j, itemsToKeep);

    std::vector<Item> newItemsToKeep, newItemsToExplore;
    for (auto k = j + 1; k < int(itemsToKeep.size()); ++k) {
        auto itemk = itemsToKeep[k];
        if (UB.getSU(itemk) >= min_util) {
            newItemsToKeep.push_back(itemk);
            newItemsToExplore.push_back(itemk);
        } else if (UB.getLU(itemk) >= min_util) {
            newItemsToKeep.push_back(itemk);
        }
    }

    prefix.push_back(newNameToOldNames[x]);

    if (utilityPx >= min_util) {
        writeOutput(prefix, utilityPx);
    }

    if (activateSubtreeUtilityPruning) {
        if (!newItemsToExplore.empty()) {
            search(transactionPx, newItemsToKeep, newItemsToExplore, prefix);
        }
    } else {
        search(transactionPx, newItemsToKeep, newItemsToKeep, prefix);
    }
}

void EFIM::search(const std::vector<Transaction> &transactionsOfP, const Itemset &itemsToKeep, const Itemset &itemsToExplore, std::vector<Item> prefix) {
    incCandidateCount(itemsToExplore.size());
    for (int j = 0; j < int(itemsToExplore.size()); ++j) {
        searchX(j, transactionsOfP, itemsToKeep, itemsToExplore, prefix);
    }
}

std::pair<std::vector<Utility>, std::vector<Item>> EFIM::calcTWU(const Database &database, Item maxItem, Utility min_util) {
    std::vector<Utility> itemTWU;
    itemTWU.resize(maxItem + 1, 0);
    for (auto &&transaction: database) {
        for (auto &&[item, utility]: transaction) {
            itemTWU[item] += transaction.transaction_utility;
        }
    }

    std::vector<Item> items;
    items.reserve(itemTWU.size());
    for (int i = 1; i < int(itemTWU.size()); ++i) {
        if (itemTWU[i] >= min_util) {
            items.push_back(i);
        }
    }
    std::sort(items.begin(), items.end(),
              [&](auto l, auto r) { return itemTWU[l] < itemTWU[r]; });

    return std::make_pair(std::move(itemTWU), std::move(items));
}

std::vector<Utility> EFIM::calcFirstSU(const Database &database, std::size_t maxItem) {
    std::vector<Utility> ret(maxItem + 1);
    for (auto &&transaction: database) {
        Utility sumSU = 0;
        for (auto i = transaction.rbegin(); i != transaction.rend(); ++i) {
            auto [item, utility] = *i;
            sumSU += utility;
            ret[item] += sumSU;
        }
    }
    return ret;
}

const UtilityBinArray &
EFIM::calcUpperBounds(const std::vector<Transaction> &transactionsPx, std::size_t j, const std::vector<Item> &itemsToKeep) {
    static thread_local UtilityBinArray utilityBinArray;
    utilityBinArray.reset(itemsToKeep[j], itemsToKeep.back());
    for (auto &&transaction: transactionsPx) {
        Utility sum_remaining_utility = 0;
        auto ed = itemsToKeep.end();
        for (auto it = transaction.rbegin(); it != transaction.rend(); ++it) {
            auto [item, utility] = *it;
            auto lb = std::lower_bound(itemsToKeep.begin(), ed, item);
            if (lb != ed && *lb == item) {// contains
                sum_remaining_utility += utility;
                utilityBinArray.getSU(item) += sum_remaining_utility + transaction.prefix_utility;
                utilityBinArray.getLU(item) += transaction.transaction_utility + transaction.prefix_utility;
            }
            ed = lb;
        }
    }
    return utilityBinArray;
}
}// namespace dphim