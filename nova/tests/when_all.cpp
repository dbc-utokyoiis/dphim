#include <nova/sync_wait.hpp>
#include <nova/task.hpp>
#include <nova/when_all.hpp>

struct Hoge {
    Hoge() = default;
    Hoge(const Hoge &) noexcept {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
    }
    Hoge(Hoge &&) noexcept {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
    }
    ~Hoge() {
        std::cout << __PRETTY_FUNCTION__ << std::endl;
    }
};

// using R = int;
// template<typename T, typename A, typename F, typename Sched>
// auto partition_map(const std::vector<std::vector<T, A>> &vec, F &&f, Sched &&sched)
//         -> nova::task<auto> {
//     // using R = invocable_invoke_result_t<F, const typename parted_vec<T, A>::partition_type &, std::size_t>;
//     std::vector<nova::task<R>> tasks;
//     for (std::size_t i = 0; i < vec.partitions().size(); ++i) {
//         auto &part = vec.partitions()[i];
//         tasks.emplace_back([](auto &part, auto part_id, auto &f, auto &sched) -> nova::task<R> {
//             // co_await invocable_invoke(sched, part_id); // TODO: 消すと動く。なぜ。
//             co_return invocable_invoke(f, part, part_id);
//         }(part, i, f, sched));
//     }
//     co_return co_await nova::when_all(std::move(tasks));
// }

int main() {
    std::vector<nova::task<>> tasks;
    Hoge h;
    for (int i = 0; i < 3; ++i) {
        tasks.emplace_back([](auto &&v) -> nova::task<> {
            co_return;
        }(h));
    }
    auto t = nova::when_all(std::move(tasks));
    nova::sync_wait([](auto &&t) -> nova::task<> {
        co_await t;
    }(std::move(t)));
}