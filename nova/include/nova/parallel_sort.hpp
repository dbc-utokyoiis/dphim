#pragma once

#include <nova/sync_wait.hpp>
#include <nova/task.hpp>
#include <nova/when_all.hpp>

namespace nova {

template<class Compare, class Iter>
unsigned sort3(Iter x, Iter y, Iter z, Compare cmp) {
    using std::swap;
    unsigned r = 0;
    if (!cmp(*y, *x))// if x <= y
    {
        if (!cmp(*z, *y))// if y <= z
            return r;    // x <= y && y <= z
        // x <= y && y > z
        swap(*y, *z);// x <= z && y < z
        r = 1;
        if (cmp(*y, *x))// if x > y
        {
            swap(*x, *y);// x < y && y <= z
            r = 2;
        }
        return r;// x <= y && y < z
    }
    if (cmp(*z, *y))// x > y, if y > z
    {
        swap(*x, *z);// x < y && y < z
        r = 1;
        return r;
    }
    swap(*x, *y);   // x > y && y <= z
    r = 1;          // x < y && x <= z
    if (cmp(*z, *y))// if y > z
    {
        swap(*y, *z);// x <= y && y < z
        r = 2;
    }
    return r;
}// x <= y && y <= z

template<typename RandomAccessIterator, typename Compare, typename Schedule>
auto parallel_sort(
        RandomAccessIterator first, RandomAccessIterator last, Compare comp, Schedule &&schedule) -> nova::task<> {

restart:

    auto len = last - first;
    if (len < 1024) {
        std::sort(first, last, comp);
        co_return;
    }

    co_await schedule();

    auto m = first + len / 2;
    auto lm1 = std::prev(last);

    sort3(first, m, lm1, comp);// *first <= *m <= *lm1

    //    _VSTD::__sort5<Compare>(first, first + len / 4, m, m + len / 4, lm1, comp);

    auto i = first;
    auto j = lm1;

    if (!comp(*i, *m)) {// if *i == *m
                        //        assert(*i == *m);
        while (true) {
            if (i == --j) {
                ++i;// __first + 1
                j = last;
                if (!comp(*first, *--j))// we need a guard if *__first == *(__last-1)
                {
                    //                    assert(*first == *(last - 1));
                    while (true) {
                        if (i == j)
                            co_return;// [__first, __last) all equivalent elements
                        if (comp(*first, *i)) {
                            std::swap(*i, *j);
                            ++i;
                            break;
                        }
                        ++i;
                    }
                }
                if (i == j)
                    co_return;
                while (true) {
                    while (!comp(*first, *i))
                        ++i;
                    while (comp(*first, *--j))
                        ;
                    if (i >= j)
                        break;
                    std::swap(*i, *j);
                    ++i;
                }
                first = i;
                goto restart;
            }
            if (comp(*j, *m)) {
                std::swap(*i, *j);
                break;// found guard for downward moving __j, now use unguarded partition
            }
        }
    }
    ++i;

    if (i < j) {
        while (true) {
            while (comp(*i, *m))// *i < *m
                ++i;
            while (!comp(*--j, *m))// *m <= *j
                ;
            if (i > j)
                break;
            std::swap(*i, *j);
            if (m == i)
                m = j;
            ++i;
        }
    }

    if (i != m && comp(*m, *i)) {
        std::swap(*i, *m);
    }

    co_await nova::when_all(
            parallel_sort(first, i, comp, schedule),
            parallel_sort(i + 1, last, comp, schedule));
}

}// namespace nova