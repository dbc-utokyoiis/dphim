#pragma once

#include <algorithm>
#include <memory>
#include <numeric>
#include <ostream>
#include <vector>

namespace dphim {

using Item = std::uint32_t;
using Utility = std::uint64_t;

#ifdef USE_OLD_TRANSACTION
#warning "USE_OLD_TRANSACTION"
struct Transaction {

    using Elem = std::pair<Item, Utility>;

    Transaction() = default;
    Transaction(const Transaction &) = default;
    Transaction &operator=(const Transaction &) = default;

    Transaction(Transaction &&) noexcept = default;
    Transaction &operator=(Transaction &&) noexcept = default;

    explicit operator bool() const noexcept {
        return bool(elems);
    }

    auto begin() noexcept { return elems->begin() + offset; }
    auto begin() const noexcept { return elems->begin() + offset; }
    auto end() noexcept { return elems->end(); }
    auto end() const noexcept { return elems->end(); }
    auto rbegin() noexcept { return elems->rbegin(); }
    auto rbegin() const noexcept { return elems->rbegin(); }
    auto rend() noexcept { return elems->rend() - offset; }
    auto rend() const noexcept { return elems->rend() - offset; }
    auto size() const noexcept { return elems->size() - offset; }
    auto empty() const noexcept { return size() == 0; }

    template<typename Cond>
    void erase_if(Cond &&cond) {
        std::erase_if(*elems, std::forward<Cond>(cond));
    }

    void reserve(std::size_t n) {
        if (!elems)
            elems = std::make_shared<std::vector<Elem>>();
        elems->reserve(n);
    }

    auto push_back(const Elem &v) { elems->push_back(v); }

    bool compare_extension(const Transaction &other) const {
        return std::equal(begin(), end(), other.begin(), other.end(), [](const Elem &l, const Elem &r) { return l.first == r.first; });
    }

    Transaction projection(std::vector<Elem>::const_iterator iter) const {
        auto ret = *this;
        auto utilityE = iter->second;
        ret.prefix_utility += utilityE;
        ret.transaction_utility -= utilityE;
        for (auto i = begin(); i < iter; ++i) {
            ret.transaction_utility -= i->second;
        }
        ret.offset = (iter - elems->begin()) + 1;
        return ret;
    }

    Transaction clone() const {
        return Transaction{std::vector<Elem>(begin(), end()), 0, transaction_utility, prefix_utility};
    }

    void merge(Transaction &&other) {
        auto iter1 = begin(), iter2 = other.begin();
        for (; iter1 != end(); ++iter1, ++iter2) {
            iter1->second += iter2->second;
        }
        transaction_utility += other.transaction_utility;
        prefix_utility += other.prefix_utility;
    }

    friend std::ostream &operator<<(std::ostream &os, const Transaction &t) {
        for (auto &&[i, u]: t) {
            os << i << "[" << u << "] ";
        }
        os << " Remaining Utility:" << t.transaction_utility;
        os << " Prefix Utility:" << t.prefix_utility;
        return os;
    }

private:
    Transaction(std::vector<Elem> &&elems, std::ptrdiff_t offset, Utility transaction_utility, Utility prefix_utility)
        : elems(std::make_shared<std::vector<Elem>>(std::move(elems))),
          offset(offset),
          transaction_utility(transaction_utility),
          prefix_utility(prefix_utility) {}

    std::shared_ptr<std::vector<Elem>> elems;
    std::ptrdiff_t offset = 0;

public:
    Utility transaction_utility = 0;
    Utility prefix_utility = 0;
};

#else

struct Transaction {

    using Elem = std::pair<Item, Utility>;

    Transaction() = default;

private:
    Transaction(const Transaction &) = default;

    Transaction &operator=(const Transaction &) = default;

public:
    Transaction(Transaction &&) noexcept = default;
    Transaction &operator=(Transaction &&) noexcept = default;

    explicit operator bool() const noexcept {
        return bool(elems);
    }

    auto begin() noexcept { return elems.get() + offset; }
    auto begin() const noexcept { return elems.get() + offset; }
    auto end() noexcept { return elems.get() + elems_size; }
    auto end() const noexcept { return elems.get() + elems_size; }
    auto rbegin() noexcept { return std::make_reverse_iterator(end()); }
    auto rbegin() const noexcept { return std::make_reverse_iterator(end()); }
    auto rend() noexcept { return std::make_reverse_iterator(begin()); }
    auto rend() const noexcept { return std::make_reverse_iterator(begin()); }
    auto size() const noexcept { return elems_size - offset; }
    auto empty() const noexcept { return size() == 0; }

    template<typename Cond>
    void erase_if(Cond &&cond) {
        auto new_end = std::remove_if(begin(), end(), std::forward<Cond>(cond));
        elems_size = std::distance(begin(), new_end);
    }

    void reserve(std::size_t n) {
        if (elems)
            throw std::runtime_error("re-allocation");
        elems = std::shared_ptr<Elem[]>(new Elem[n]);
        elems_size = 0;
        reserved_size = n;
    }

    template<typename A, typename D>
    void reserve(std::size_t n, A &&alloc_func, D &&deleter_factory) {
        if (elems)
            throw std::runtime_error("re-allocation");
        auto *p = reinterpret_cast<Elem *>(alloc_func(sizeof(Elem) * n));
        elems = std::shared_ptr<Elem[]>(p, deleter_factory(sizeof(Elem) * n));
        elems_size = 0;
        reserved_size = n;
    }

    void push_back(Elem &&v) {
        if (elems_size >= reserved_size)
            throw std::out_of_range("elems out_of_range");
        elems[elems_size++] = std::move(v);
    }

    void push_back(const Elem &v) {
        if (elems_size >= reserved_size)
            throw std::out_of_range("elems out_of_range");
        elems[elems_size++] = v;
    }

    bool compare_extension(const Transaction &other) const {
        return std::equal(begin(), end(), other.begin(), other.end(), [](const Elem &l, const Elem &r) { return l.first == r.first; });
    }

    template<typename Iter>
    Transaction projection(Iter iter) const {
        auto ret = *this;
        auto utilityE = iter->second;
        ret.prefix_utility += utilityE;
        ret.transaction_utility -= utilityE;
        for (auto i = begin(); i < iter; ++i) {
            ret.transaction_utility -= i->second;
        }
        ret.offset = (iter - elems.get()) + 1;
        return ret;
    }

    Transaction clone() const {
        auto elems = std::shared_ptr<Elem[]>(new Elem[size()]);
        std::copy(begin(), end(), elems.get());
        return Transaction{std::move(elems), size(), size(), 0, transaction_utility, prefix_utility};
    }

    template<typename A, typename D>
    Transaction
    clone(A &&alloc_func, D &&deleter_factory) const {
        auto *p = reinterpret_cast<Elem *>(alloc_func(sizeof(Elem) * size()));
        auto elems = std::shared_ptr<Elem[]>(p, deleter_factory(sizeof(Elem) * size()));
        std::copy(begin(), end(), elems.get());
        return Transaction{std::move(elems), size(), size(), 0, transaction_utility, prefix_utility};
    }

    void merge(Transaction &&other) {
        auto iter1 = begin(), iter2 = other.begin();
        for (; iter1 != end(); ++iter1, ++iter2) {
            iter1->second += iter2->second;
        }
        transaction_utility += other.transaction_utility;
        prefix_utility += other.prefix_utility;
    }

    friend std::ostream &operator<<(std::ostream &os, const Transaction &t) {
        for (auto &&[i, u]: t) {
            os << i << "[" << u << "] ";
        }
        os << " Remaining Utility:" << t.transaction_utility;
        os << " Prefix Utility:" << t.prefix_utility;
        return os;
    }

private:
    Transaction(std::shared_ptr<Elem[]> &&elems, std::size_t elems_size, std::size_t reserved_size,
                std::ptrdiff_t offset, Utility transaction_utility, Utility prefix_utility)
        : elems(std::move(elems)),
          elems_size(elems_size),
          reserved_size(reserved_size),
          offset(offset),
          transaction_utility(transaction_utility),
          prefix_utility(prefix_utility) {}

    std::shared_ptr<Elem[]> elems;
    std::size_t elems_size = 0;
    std::size_t reserved_size = 0;
    std::ptrdiff_t offset = 0;

public:
    Utility transaction_utility = 0;
    Utility prefix_utility = 0;
};

#endif

}// namespace dphim