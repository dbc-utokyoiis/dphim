#undef NDEBUG

#include <future>
#include <iostream>
#include <memory>
#include <thread>

#include <dphim/dpefim.hpp>
#include <dphim/dpfhm.hpp>
#include <dphim/efim.hpp>
#include <dphim/util/pmem_allocator.hpp>

#include <nova/numa_aware_scheduler.hpp>
#include <nova/os_thread_scheduler.hpp>
#include <nova/simple_scheduler.hpp>
#include <nova/single_queue_scheduler.hpp>
#include <nova/sync_wait.hpp>

#include "cmdline.h"

#include <jemalloc/jemalloc.h>


std::shared_ptr<nova::scheduler_base> get_scheduler(const cmdline::parser &parser) {
    auto threads = parser.get<int>("threads");
    auto sched_type = parser.get<std::string>("sched");
    if (sched_type == "global") {
        return std::make_shared<nova::single_queue_scheduler>(threads);
    } else if (sched_type == "local") {
        return std::make_shared<nova::simple_scheduler>(threads);
    } else if (sched_type == "local-numa") {
        return std::make_shared<nova::numa_aware_scheduler>(threads);
    } else if (sched_type == "local-numa-interleave") {
        return std::make_shared<nova::numa_aware_scheduler>(threads, true, true);
    } else if (sched_type == "dphim") {
        return std::make_shared<nova::numa_aware_scheduler>(threads, true);
    } else if (sched_type == "osthread" || sched_type == "para63") {
        return std::make_shared<nova::os_thread_scheduler>(threads);
    } else if (sched_type == "sp") {
        return nullptr;
    } else {
        throw std::runtime_error("no matching scheduler type: " + sched_type);
    }
}

int main(int argc, char *argv[]) {

    cmdline::parser parser;
    parser.add<std::string>("algorithm", 'a', "The kind of HUIM algorithm [efim, fhm]", false, "efim");
    parser.add<std::string>("input", 'i', "Input file", true);
    parser.add<std::string>("output", 'o', "Output path", false, "/dev/stdout");
    parser.add<dphim::Utility>("minutil", 'm', "Minimum utility", true);
    parser.add<int>("threads", 't', "# of threads", false, 1);
    parser.add<std::string>("sched", 's', "type of scheduler[global, local, local-numa, dphim, osthread, sp]", false, "local-numa");
    parser.add<std::string>("part-strategy", '\0', "Partitioning Strategy (enabled only for sp) [normal, rnd, weighted, twolen]", false, "normal");

    parser.add<int>("scatter-alloc-threshold1", '\0', "speculation threshold alpha for step3", false);
    parser.add<int>("task-migration-threshold1", '\0', "speculation threshold beta for step3", false);
    parser.add<int>("task-migration-threshold2", '\0', "speculation threshold beta for step3", false);
    parser.add<int>("scatter-alloc-threshold3", '\0', "speculation threshold alpha for step3", false);
    parser.add<int>("task-migration-threshold3", '\0', "speculation threshold beta for step3", false);
    parser.add<int>("stop-scatter-alloc-depth", '\0', "speculation threshold alpha for step3", false);
    parser.add<int>("stop-task-migration-depth", '\0', "speculation threshold beta for step3", false);
    parser.add<int>("alpha1", '\0', "speculation threshold alpha for step3", false);
    parser.add<int>("beta1", '\0', "speculation threshold beta for step3", false);
    parser.add<int>("beta2", '\0', "speculation threshold beta for step3", false);
    parser.add<int>("alpha3", '\0', "speculation threshold alpha for step3", false);
    parser.add<int>("beta3", '\0', "speculation threshold beta for step3", false);

    parser.add<std::string>("pmem", '\0', "which persistent memory is used: [single,numa]", false, "");
    parser.add<std::string>("pmem-alloc", '\0', "what is allocated on persistent memory: [none,elems,aek]", false, "");

    parser.add("print-pmems", '\0', "Print pmems");
    parser.add("json", '\0', "Output log in JSON format");
    parser.add("debug", '\0', "Debug mode");

    parser.parse_check(argc, argv);

    auto alg = parser.get<std::string>("algorithm");
    auto in = parser.get<std::string>("input");
    auto out = parser.get<std::string>("output");
    auto minutil = parser.get<dphim::Utility>("minutil");
    auto threads = parser.get<int>("threads");
    auto sched_type = parser.get<std::string>("sched");
    auto pmem_type = parser.get<std::string>("pmem");
    auto pmem_alloc_type = parser.get<std::string>("pmem-alloc");
    auto json_format = parser.exist("json");
    auto debug_mode = parser.exist("debug");
    auto part_strategy = parser.get<std::string>("part-strategy");

    dphim::DPEFIM::SpeculationThresholds thresholds = {};
    if (sched_type == "dphim") {
        if (parser.exist("alpha1"))
            thresholds.step1_scatter_alloc_threshold = parser.get<int>("alpha1");
        if (parser.exist("beta1"))
            thresholds.step1_task_migration_threshold = parser.get<int>("beta1");
        if (parser.exist("beta2"))
            thresholds.step2_task_migration_threshold = parser.get<int>("beta2");
        if (parser.exist("alpha3"))
            thresholds.step3_scatter_alloc_threshold = parser.get<int>("alpha3");
        if (parser.exist("beta3"))
            thresholds.step3_task_migration_threshold = parser.get<int>("beta3");
        if (parser.exist("scatter-alloc-threshold1"))
            thresholds.step1_scatter_alloc_threshold = parser.get<int>("scatter-alloc-threshold1");
        if (parser.exist("task-migration-threshold1"))
            thresholds.step1_task_migration_threshold = parser.get<int>("task-migration-threshold1");
        if (parser.exist("task-migration-threshold2"))
            thresholds.step2_task_migration_threshold = parser.get<int>("task-migration-threshold2");
        if (parser.exist("scatter-alloc-threshold3"))
            thresholds.step3_scatter_alloc_threshold = parser.get<int>("scatter-alloc-threshold3");
        if (parser.exist("task-migration-threshold3"))
            thresholds.step3_task_migration_threshold = parser.get<int>("task-migration-threshold3");
        if (parser.exist("stop-scatter-alloc-depth"))
            thresholds.step3_stop_scatter_alloc_depth = parser.get<int>("stop-scatter-alloc-depth");
        if (parser.exist("stop-task-migration-depth"))
            thresholds.step3_stop_task_migration_depth = parser.get<int>("stop-task-migration-depth");
    }

    if (debug_mode) {
        std::cerr << "pmem type: " << pmem_type << std::endl;
        std::cerr << "sched_type: " << sched_type << std::endl;
        std::cerr << "algorithm: " << alg << std::endl;
    }

    if (threads > static_cast<int>(std::thread::hardware_concurrency())) {
        std::cerr << "# of threads is larger than hardware concurrency: "
                  << std::thread::hardware_concurrency() << std::endl;
    }

    auto set_pmem = [](auto &executor, const std::string &pmem_type) {
        if (pmem_type == "numa") {
            nova::numa::numa_info numa;
            for (auto &node: numa.nodes()) {
                auto devs = dphim::get_pmem_devdax(node.id());
                if (devs.size() == 1) {
                    executor.set_pmem_devdax_path(node.id(), "/dev/" + devs[0]);
                } else {
                    throw std::runtime_error(std::string("failed to configure for pmem: pmem type:") + pmem_type);
                }
            }
        } else if (pmem_type == "single") {
            auto devs = dphim::get_pmem_devdax(std::nullopt);
            if (!devs.empty()) {
                executor.set_pmem_devdax_path(0, "/dev/" + devs[0]);
            } else {
                throw std::runtime_error(std::string("failed to configure for pmem: pmem type:") + pmem_type);
            }
        } else if (!pmem_type.empty()) {
            throw std::runtime_error(std::string("failed to configure for pmem: pmem type:") + pmem_type);
        }
    };

    auto exec_dp = [&out, &json_format](auto &executor, auto &sched) {
        sched->start([&] {
            executor.register_thread();
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));
        nova::sync_wait(executor.run());
        auto f = std::async([&] { sched->stop(); });
        if (f.wait_for(std::chrono::seconds(3)) == std::future_status::timeout) {
            std::cerr << "stop request is timeout." << std::endl;
            std::exit(-1);
        }
        if (out != "/dev/null")
            executor.flushOutput();
        if (json_format) {
            executor.print_json(std::cout);
        } else {
            executor.print(std::cout);
        }
    };

    auto sched = get_scheduler(parser);

    if (alg == "efim") {
        if (sched_type == "sp") {
            dphim::EFIM efim{in, out, minutil, threads};
            efim.set_debug_mode(debug_mode);
            efim.set_partition_strategy(part_strategy);
            set_pmem(efim, pmem_type);
            efim.run();
            if (out != "/dev/null")
                efim.flushOutput();
            if (json_format) {
                efim.print_json(std::cout);
            } else {
                efim.print(std::cout);
            }
        } else {
            dphim::DPEFIM dpefim(sched, in, out, minutil, threads);
            dpefim.set_debug_mode(debug_mode);
            dpefim.set_sched_no_await(sched_type == "para63");
            dpefim.set_speculation_thresholds(thresholds);
            dpefim.set_pmem_alloc_type(pmem_alloc_type);
            set_pmem(dpefim, pmem_type);
            exec_dp(dpefim, sched);
        }
    } else if (alg == "fhm") {
        if (sched_type == "sp") {
            throw std::runtime_error("sp scheduler is not supported for fhm");
        } else {
            dphim::DPFHM dpfhm(sched, in, out, minutil, threads, sched_type == "dphim");
            if (threads == 1)
                dpfhm.set_sched_no_await(true);
            set_pmem(dpfhm, pmem_type);
            dpfhm.set_pmem_alloc_type(pmem_alloc_type);
            exec_dp(dpfhm, sched);
        }
    } else {
        throw std::runtime_error("no matching algorithm: " + alg);
    }

    // malloc_stats_print(nullptr, nullptr, nullptr);

    return 0;
}
