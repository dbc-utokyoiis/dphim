#include <nova/single_queue_scheduler.hpp>

#include <boost/lockfree/queue.hpp>
#include <random>
#include <thread>

namespace nova {
inline namespace scheduler {

struct single_queue_scheduler::worker_t : worker_base<worker_t> {
    using base = worker_base<worker_t>;
    using base::this_thread_worker_id;
    friend base;
    friend single_queue_scheduler;

    explicit worker_t(single_queue_scheduler &sched, std::size_t worker_id)
        : base(worker_id), sched(std::addressof(sched)) {}

    void try_sleep() {
        sched->sleeping_worker_count.fetch_add(1, MEM_ORDER_RELAXED);
        base::try_sleep();
        sched->sleeping_worker_count.fetch_sub(1, MEM_ORDER_REL);
    }

private:
    auto execute_one() -> bool {
        if (!this_thread_worker_id) {
            throw std::runtime_error("simple_worker is executed on an unlinked thread.");
        }

        //        if (auto op = sched->global_task_queue.pop_front()) {
        //            op->execute();
        //            return true;
        //        }

        {
            std::unique_lock<std::mutex> lk(sched->queue_mtx);
            if (!sched->global_task_queue.empty()) {
                auto op = sched->global_task_queue.front();
                sched->global_task_queue.pop();
                lk.unlock();
                op->execute();
                return true;
            }
        }

        return false;
    }

    single_queue_scheduler *sched;
};

void single_queue_scheduler::post(task_base *op, int /*option*/) {
    //    global_task_queue.push_front(op);

    {
        std::lock_guard lk(queue_mtx);
        global_task_queue.push(op);
    }

    if (sleeping_worker_count.load(MEM_ORDER_ACQ) > 0) {
        for (auto &w: workers)
            if (w && w->try_wake_up())
                return;
    }
}

void single_queue_scheduler::run_worker(int wid) {
    auto w = std::make_shared<worker_t>(*this, wid);
    workers.at(wid) = w;
    w->run();
}

void single_queue_scheduler::stop_request() {
    for (auto &w: workers) {
        if (w)
            w->stop_request();
    }
}

}// namespace scheduler
}// namespace nova