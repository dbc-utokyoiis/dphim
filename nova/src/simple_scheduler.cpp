#include <nova/simple_scheduler.hpp>

#include <algorithm>
#include <nova/util/concurrent_list.hpp>
#include <random>
#include <thread>

namespace nova {
inline namespace scheduler {

struct simple_scheduler::worker_t : worker_base<worker_t> {
    using base = worker_base<worker_t>;
    using base::this_thread_worker_id;
    friend base;
    friend simple_scheduler;

    explicit worker_t(simple_scheduler &sched, std::size_t worker_id)
        : base(worker_id), sched(std::addressof(sched)) {}

    void post(task_base *tb) {
        task_queue.push_front(tb);
    }

private:
    auto execute_one() -> bool {
        if (!this_thread_worker_id) {
            throw std::runtime_error("simple_worker is executed on an unlinked thread.");
        }

        if (task_queue.consume_once([](auto *op) {
                op->execute();
            }) > 0) {
            return true;
        }

        if (sched->try_steal(this->id, [](auto *op) {
                op->execute();
            })) {
            return true;
        }

        return false;
    }

    simple_scheduler *sched;
    concurrent_stack<task_base *> task_queue;
};

bool simple_scheduler::try_steal(worker_t::id_t stealer, void (*func)(task_base *)) {
    if (global_task_queue.consume_once(func) > 0) {
        return true;
    }

    thread_local std::random_device seed_gen;
    auto worker_list = workers;
    std::shuffle(worker_list.begin(), worker_list.end(), std::mt19937(seed_gen()));

    for (auto &w: worker_list) {
        if (w && w->id != stealer && w->task_queue.consume_once(func) > 0) {
            return true;
        }
    }
    return false;
}

void simple_scheduler::delegate(task_base *op, std::optional<worker_t::id_t> source_worker) {
    for (auto &w: workers)
        if (w && w->id != source_worker && w->try_wake_up([op](auto &&w) { w.post(op); }))
            return;

    global_task_queue.push_front(op);
    for (auto &w: workers)
        if (w && w->try_wake_up())
            return;
}

void simple_scheduler::post(task_base *op, int /*option*/) {
    for (auto &w: workers)
        if (w && w->try_wake_up([op](auto &&w) { w.post(op); }))
            return;
    if (auto w = worker_t::this_thread_worker_id) {
        workers[*w]->post(op);
        workers[*w]->try_wake_up();
    } else {
        delegate(op, 0);
    }
}

void simple_scheduler::run_worker(int wid) {
    auto w = std::make_shared<worker_t>(*this, wid);
    workers.at(wid) = w;
    w->run();
}

void simple_scheduler::stop_request() {
    for (auto &w: workers) {
        if (w) {
            w->stop_request();
        }
    }
}

}// namespace scheduler
}// namespace nova