#pragma once

#include <dphim/dphim_base.hpp>
#include <dphim/util/pair_map.hpp>

#include <algorithm>

namespace dphim {

struct Element {
    Element(std::size_t tid, Utility iutil, Utility rutil)
        : tid(tid), iutil(iutil), rutil(rutil) {}
    std::size_t tid;
    Utility iutil;
    Utility rutil;
};

template<typename Allocator = std::allocator<Element>>
struct UtilityList_ {
    int item = -1;
    Utility sumIUtils = 0;
    Utility sumRUtils = 0;
    std::vector<Element, Allocator> elms;

    explicit UtilityList_(int item = -1, const Allocator &a = Allocator())
        : item(item), elms(a) {}

    UtilityList_(const UtilityList_ &) = default;
    UtilityList_(UtilityList_ &&) noexcept = default;
    UtilityList_ &operator=(const UtilityList_ &) = default;
    UtilityList_ &operator=(UtilityList_ &&) noexcept = default;

    template<typename A>
    explicit UtilityList_(const UtilityList_<A> &other, const Allocator &a = Allocator())
        : item(other.item),
          sumIUtils(other.sumIUtils),
          sumRUtils(other.sumRUtils),
          elms(a) {
        elms.insert(elms.end(), other.elms.begin(), other.elms.end());
    }

    template<typename A>
    explicit UtilityList_(UtilityList_<A> &&other, const Allocator &a = Allocator())
        : item(other.item),
          sumIUtils(other.sumIUtils),
          sumRUtils(other.sumRUtils),
          elms(a) {
        elms.insert(elms.end(), std::make_move_iterator(other.elms.begin()), std::make_move_iterator(other.elms.end()));
    }

    void reserve(std::size_t n) {
        elms.reserve(n);
    }

    void addElement(const Element &tuple) {
        sumIUtils += tuple.iutil;
        sumRUtils += tuple.rutil;
        elms.push_back(tuple);
    }

    [[nodiscard]] bool is_null() const {
        return item == -1;
    }

    //    template<typename Iterator, typename T, typename Comp>
    //    static auto lower_bound_bf(Iterator bgn, Iterator ed, T &&value, Comp &&comp) {
    //        auto it = bgn;
    //        auto n = ed - bgn;
    //        while (n > 1) {
    //            auto m = n / 2;
    //            it = comp(*(it + m), value) ? (it + m) : it;
    //            n -= m;
    //        }
    //        return it + comp(*it, value);
    //    }

    template<typename Iter>
    [[nodiscard]] Iter findIterWithTID(const Element &e, Iter bgn) const {
        return std::lower_bound(
                bgn, elms.end(), e,
                [](const Element &el, const Element &er) { return el.tid < er.tid; });
    }

    [[nodiscard]] std::optional<Element> findWithTID(const Element &e) const {
        auto it = findIterWithTID(e, elms.begin());
        if (it == elms.end() || it->tid != e.tid) {
            return std::nullopt;
        } else {
            return *it;
        }
    }

    void reset(int item_ = -1) {
        item = item_;
        sumIUtils = 0;
        sumRUtils = 0;
        elms.clear();
    }
};

struct DPFHM : dphim_base {

    DPFHM(std::shared_ptr<nova::scheduler_base> sched, std::string input_path, std::string output_path, Utility minutil, int th_num, bool do_partitioning = true)
        : dphim_base(sched, std::move(input_path), std::move(output_path), minutil, th_num),
          do_partitioning(do_partitioning),
          mapFMAP(do_partitioning ? sched->get_max_node_id().value_or(0) + 1 : 1) {}

    bool do_partitioning = false;

private:
    using UtilityList = UtilityList_<>;
    std::vector<Item> items2Keep;
    std::vector<Utility> mapItem2TWU;
    std::vector<UtilityList> listOfUtilityLists;
    std::vector<decltype(listOfUtilityLists)::iterator> mapItem2UtilityList;
    PairMap<Utility> mapFMAP;

public:
    auto greaterItem(Item l, Item r) -> bool {
        long diff = static_cast<long>(mapItem2TWU.at(l)) - static_cast<long>(mapItem2TWU.at(r));
        return diff == 0 ? l > r : diff < 0;
    }

    auto calcListOfUtilityLists() -> nova::task<> {
        listOfUtilityLists.reserve(items2Keep.size());

        for (auto item: items2Keep) {
            listOfUtilityLists.emplace_back(item);
        }

        co_await nova::parallel_sort(
                listOfUtilityLists.begin(), listOfUtilityLists.end(),
                [this](const auto &l, const auto &r) { return greaterItem(l.item, r.item); },
                [this] { return schedule(); });
    }

    auto calcMapFMAP(Database &database) -> nova::task<> {
        using R = std::vector<std::pair<Item, Element>>;
        std::vector<R> ret_list(database.size());
        auto scan_range = [&](auto bg, auto ed) -> nova::task<> {
            co_await schedule();
            for (auto i = bg; i < ed; ++i)
                ret_list[i] = co_await scanOneTransaction(database[i], i);
        };
        std::vector<nova::task<>> tasks;
        for (std::size_t tid = 0; tid < database.size(); tid += 500) {
            tasks.emplace_back(scan_range(tid, std::min(tid + 500, database.size())));
        }
        co_await nova::when_all(std::move(tasks));
        for (auto &ret: ret_list)
            for (auto &&[i, elm]: ret)
                mapItem2UtilityList[i]->addElement(elm);
    }

    auto calcMapFMAP(std::vector<Database> &database) -> nova::task<> {
        using R = std::vector<std::pair<Item, Element>>;
        std::size_t db_size = 0;
        for (auto &db: database)
            db_size += db.size();
        std::vector<R> ret_list(db_size);

        auto scan_range = [&](std::size_t node, auto tid, auto bg, auto ed) -> nova::task<> {
            co_await schedule(node);
            for (auto i = bg; i < ed; ++i) {
                ret_list[tid + i] = co_await scanOneTransaction(database[node][i], tid + i);
            }
        };

        std::vector<nova::task<>> tasks;
        std::size_t tid = 0;
        for (std::size_t node = 0; node < database.size(); ++node) {
            for (std::size_t i = 0; i < database[node].size(); i += 500)
                tasks.emplace_back(scan_range(node, tid, i, std::min(i + 500, database[node].size())));
            tid += database[node].size();
        }
        co_await nova::when_all(std::move(tasks));
        for (auto &ret: ret_list)
            for (auto &&[i, elm]: ret)
                mapItem2UtilityList[i]->addElement(elm);
    }

    auto scanOneTransaction(const Transaction &transaction, std::size_t tid) -> nova::task<std::vector<std::pair<Item, Element>>> {
        std::vector<std::pair<Item, Utility>> revisedTransaction;
        Utility remainingUtility = 0, newTWU = 0;
        for (auto [i, u]: transaction) {
            if (mapItem2TWU[i] >= min_util) {
                revisedTransaction.emplace_back(i, u);
                remainingUtility += u;
                newTWU += u;
            }
        }

        std::sort(revisedTransaction.begin(), revisedTransaction.end(),
                  [this](const auto &l, const auto &r) { return greaterItem(l.first, r.first); });

        std::vector<std::pair<Item, Element>> ret;
        for (auto [i, u]: revisedTransaction) {
            remainingUtility -= u;
            ret.emplace_back(i, Element(tid, u, remainingUtility));
        }

        for (std::size_t i = 0; i < revisedTransaction.size(); ++i) {
            auto [i1, u1] = revisedTransaction[i];
            auto utilityListOfI1 = mapItem2UtilityList[i1];
            for (std::size_t j = i + 1; j < revisedTransaction.size(); ++j) {
                auto [i2, u2] = revisedTransaction[j];
                auto utilityListOfI2 = mapItem2UtilityList[i2];
                auto p = std::make_pair(std::distance(std::begin(listOfUtilityLists), utilityListOfI1), std::distance(std::begin(listOfUtilityLists), utilityListOfI2));
                if (p.first == p.second)
                    continue;
                mapFMAP.at_raw(p).atomic_insert_or_add(newTWU, std::memory_order_relaxed);
            }
        }
        co_return ret;
    }

    template<typename I>
    auto search(const I &prefix, const UtilityList &utilityListOfP, const std::vector<UtilityList> &candidates) {
        incCandidateCount(candidates.size());

        std::vector<nova::task<>> tasks;
        tasks.reserve(candidates.size());

        for (std::size_t i = 0; i < candidates.size(); ++i) {
            tasks.push_back(searchX(i, prefix, utilityListOfP, candidates));
        }

        return nova::when_all(std::move(tasks));
    }

    template<typename I>
    auto searchX(std::size_t i, const I &prefix, const UtilityList &utilityListOfP, const std::vector<UtilityList> &candidates) -> nova::task<> {
        auto &X = candidates[i];

        auto p = prefix;
        p.push_back(X.item);

        if (X.sumIUtils >= min_util) {
            writeOutput(p, X.sumIUtils);
        }

        if (X.sumIUtils + X.sumRUtils >= min_util) {
            auto exULs = co_await make_exULs(i, utilityListOfP, candidates);
            co_await search(p, X, exULs);
        }
        co_return;
    }

    UtilityList construct(const UtilityList &P, const UtilityList &px, const UtilityList &py) {
        auto get_ey = [bgn = py.elms.begin(), &py](const Element &e) mutable {
            return bgn = py.findIterWithTID(e, bgn);
        };
        auto get_e = [bgn = P.elms.begin(), &P](const Element &e) mutable {
            return bgn = P.findIterWithTID(e, bgn);
        };

        UtilityList pxyUL;
        pxyUL.reset(py.item);
        pxyUL.reserve(px.elms.size());
        Utility totalUtility = px.sumIUtils + px.sumRUtils;

        for (auto &ex: px.elms) {
            auto ey = get_ey(ex);
            if (ey == py.elms.end() || ey->tid != ex.tid) {
                {// LA-prune strategy
                    totalUtility -= (ex.iutil + ex.rutil);
                    if (totalUtility < min_util)
                        return UtilityList{};
                }
                continue;
            }

            if (P.is_null()) {
                pxyUL.addElement({ex.tid, ex.iutil + ey->iutil, ey->rutil});
            } else {
                auto e = get_e(ex);
                if (e != P.elms.end() && e->tid == ex.tid) {
                    pxyUL.addElement({ex.tid, ex.iutil + ey->iutil - e->iutil, ey->rutil});
                }
            }
        }
        return pxyUL;
    }

    auto make_exULs(std::size_t i, const UtilityList &pUL, const std::vector<UtilityList> &ULs) -> nova::task<std::vector<UtilityList>> {

        auto &X = ULs.at(i);

        std::vector<std::size_t> explore_j;

        for (std::size_t j = i + 1; j < ULs.size(); ++j) {
            auto &Y = ULs.at(j);

            auto utilityListOfI1 = mapItem2UtilityList[X.item];
            auto utilityListOfI2 = mapItem2UtilityList[Y.item];
            auto p = std::make_pair(std::distance(std::begin(listOfUtilityLists), utilityListOfI1),
                                    std::distance(std::begin(listOfUtilityLists), utilityListOfI2));

            auto mapTWUFIt = mapFMAP.find(p);
            if (mapTWUFIt == mapFMAP.end())
                continue;
            auto twu = mapTWUFIt->second;
            if (twu < min_util)
                continue;
            explore_j.push_back(j);
        }
        incCandidateCount(explore_j.size());

        std::vector<UtilityList> exULs;
        exULs.reserve(explore_j.size());

        if (explore_j.size() > 1) {
            std::vector<nova::task<UtilityList>> construct_tasks;
            construct_tasks.reserve(explore_j.size());
            for (auto &j: explore_j) {
                construct_tasks.emplace_back([](auto self, auto &&...args) -> nova::task<UtilityList> {
                    co_await self->schedule();
                    co_return self->construct(std::forward<decltype(args)>(args)...);
                }(this, pUL, X, ULs[j]));
            }
            for (auto &&exUL: co_await nova::when_all(std::move(construct_tasks))) {
                if (!exUL.is_null())
                    exULs.push_back(std::move(exUL));
            }
        } else {
            for (auto j: explore_j) {
                auto exUL = construct(pUL, X, ULs[j]);
                if (!exUL.is_null())
                    exULs.push_back(std::move(exUL));
            }
        }

        co_return std::move(exULs);
    }

    template<bool do_partitioning = true>
    auto run_impl() -> nova::task<> {
        timer_start();

        auto [database, maxItem] = co_await parseTransactions();

        std::cout << "Transactions: " << database.size() << std::endl;
        std::cout << "maxItem: " << maxItem << std::endl;
        time_point("parse");

        auto [mapTWU, items] = co_await calcTWU<decltype(items2Keep)>(database, maxItem);
        mapItem2TWU = std::move(mapTWU);
        items2Keep = std::move(items);
        std::cout << "itemsToKeep.size(): " << items2Keep.size() << std::endl;
        time_point("calcTWU");

        co_await calcListOfUtilityLists();

        mapItem2UtilityList.resize(maxItem);
        for (auto it = listOfUtilityLists.begin(); it < listOfUtilityLists.end(); ++it) {
            mapItem2UtilityList[it->item] = it;
        }

        std::cout << mapFMAP.part_num() << std::endl;

        mapFMAP.set_size(maxItem + 1);

        std::vector<nova::task<>> tasks;
        auto f = [this](auto pid) -> nova::task<> {
            while (sched->get_current_node_id().value_or(pid) != static_cast<int>(pid)) {
                co_await schedule(pid);
            }
            if (pmem_alloc_type == PmemAllocType::None) {
                mapFMAP.reserve(pid);
            } else {

#ifdef DPHIM_PMEM
                auto pmem_allocator = get_pmem_allocator();
                mapFMAP.reserve(
                        pid,
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
            }
            mapFMAP.clear(pid);
        };
        for (std::size_t i = 0; i < mapFMAP.part_num(); ++i) {
            tasks.emplace_back(f(i));
        }
        co_await nova::when_all(std::move(tasks));

        co_await calcMapFMAP(database);
        time_point("Build");

        co_await search(std::vector<Item>{}, UtilityList{}, listOfUtilityLists);
        time_point("Search");
    }

    auto run() -> nova::task<> {
        if (do_partitioning) {
            co_await run_impl<true>();
        } else {
            co_await run_impl<false>();
        }
    }
};

}// namespace dphim