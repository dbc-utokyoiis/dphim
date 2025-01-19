#include <boost/intrusive/list.hpp>
#include <boost/lockfree/stack.hpp>
#include <nova/numa_aware_scheduler.hpp>
#include <nova/util/parallel_intrusive_list.hpp>

#include <iostream>
#include <memory>
#include <thread>
#include <vector>

template<typename Item, Item *Item::*Next>
struct nonblocking_intrusive_list {

    nonblocking_intrusive_list() noexcept : m_head(nullptr), m_size(0) {}
    nonblocking_intrusive_list(const nonblocking_intrusive_list &other) = delete;
    nonblocking_intrusive_list(nonblocking_intrusive_list &&other) = delete;
    nonblocking_intrusive_list &operator=(const nonblocking_intrusive_list &other) = delete;
    nonblocking_intrusive_list &operator=(nonblocking_intrusive_list &&other) = delete;

    using size_type = long;

    void push_front(Item *item) {
        auto new_head = item;
        new_head->*Next = m_head.load();
        while (!m_head.compare_exchange_weak(new_head->*Next, new_head)) {
        }
        m_size += 1;
    }

    Item *pop_front() noexcept {
        auto old_head = m_head.load();
        while (old_head && !m_head.compare_exchange_weak(old_head, old_head->*Next)) {
        }
        return old_head;
    }

private:
    explicit nonblocking_intrusive_list(void *head) noexcept
        : m_head(head), m_size(0) {}
    std::atomic<Item *> m_head;
    std::atomic<std::size_t> m_size;
};

struct Task {
    Task(int c, int t) : cpu_id(c), task_id(t) {}
    Task *next;
    int cpu_id;
    int task_id;
    static void delete_task(Task *t) {
        delete t;
    }
};

int main() {
    std::vector<std::thread> thread_pool;

    auto th_num = std::thread::hardware_concurrency();
    // auto th_num = std::min(16u, std::thread::hardware_concurrency());
    std::cout << "th_num: " << th_num << std::endl;

    nova::nonblocking_intrusive_list<Task, &Task::next> list;

    thread_pool.reserve(th_num);
    std::atomic<std::size_t> sum_num(0);
    std::atomic<std::size_t> task_num(0);
    for (int i = 0; i < th_num; ++i) {
        thread_pool.emplace_back([&, cpu = i] {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(cpu, &cpuset);
            auto self_th = pthread_self();
            if (auto e = pthread_setaffinity_np(self_th, sizeof(cpuset), &cpuset); e != 0) {
                throw std::runtime_error(std::string("[pthread_setaffinity_np] ") + strerror(e));
            }

            std::size_t num = 0;

            for (int j = 0; j < 1000; ++j) {
                if (std::rand() % 2 == 0) {
                    auto t = new Task(cpu, j);
                    list.push_front(t);
                    task_num += 1;
                } else {
                    auto n = list.consume_once([](Task *p) {
                        Task::delete_task(p);
                    });
                    num += n;
                }
            }
            while (true) {
                auto n = list.consume_once([](Task *p) {
                    Task::delete_task(p);
                });
                num += n;
                if (n == 0) {
                    break;
                }
            }

            std::cout << "cnt:" << num << std::endl;
            sum_num += num;
        });
    }

    for (auto &th: thread_pool) {
        if (th.joinable())
            th.join();
    }
    std::cout << sum_num << std::endl;
    std::cout << task_num << std::endl;
}