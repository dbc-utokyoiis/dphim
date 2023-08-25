#undef NDEBUG

#include <future>
#include <iostream>
#include <memory>
#include <thread>

#include <dphim/dpefim.hpp>
#include <dphim/dpfhm.hpp>
#include <dphim/efim.hpp>
#include <dphim/util/parted_vec.hpp>
#include <dphim/util/pmem_allocator.hpp>

#include <nova/numa_aware_scheduler.hpp>
#include <nova/os_thread_scheduler.hpp>
#include <nova/simple_scheduler.hpp>
#include <nova/single_queue_scheduler.hpp>
#include <nova/sync_wait.hpp>

#include "cmdline.h"

int main(int argc, char *argv[]) {

    cmdline::parser parser;
    parser.add<std::string>("algorithm", 'a', "The king of HUIM algorithm [efim, fhm]", false, "efim");
    parser.add<std::string>("input", 'i', "Input file", true);
    parser.add<std::string>("output", 'o', "Output path", false, "/dev/stdout");
    parser.add<dphim::Utility>("minutil", 'm', "Minimum utility", true);
    parser.add<int>("threads", 't', "# of threads", false, 1);
    parser.add<std::string>("sched", 's', "type of scheduler", false, "local-numa");
    parser.add("mp", '\0', "use memory pool");
    parser.add("pl", '\0', "use pipeline parallel");
    parser.add<std::string>("pmem", '\0', "which persistent memory is used: [single,numa]", false, "");
    parser.add<std::string>("pmem-alloc", '\0', "what is allocated on persistent memory: [elems,aek,all]", false, "");

    parser.parse_check(argc, argv);

    auto alg = parser.get<std::string>("algorithm");
    auto in = parser.get<std::string>("input");
    auto out = parser.get<std::string>("output");
    auto minutil = parser.get<dphim::Utility>("minutil");
    auto threads = parser.get<int>("threads");
    auto sched_type = parser.get<std::string>("sched");
    auto use_mem_pool = parser.exist("mp");
    auto use_pipeline_parallel = parser.exist("pl");
    auto pmem_type = parser.get<std::string>("pmem");
    auto pmem_alloc_type = parser.get<std::string>("pmem-alloc");

    std::cout << "pmem type: " << pmem_type << std::endl;

    if (threads > long(std::thread::hardware_concurrency())) {
        std::cerr << "# of threads is larger than hardware concurrency: " << std::thread::hardware_concurrency() << std::endl;
    }

    std::shared_ptr<nova::scheduler_base> sched;
    std::cout << "sched_type: " << sched_type << std::endl;
    if (sched_type == "global") {
        sched = std::make_shared<nova::single_queue_scheduler>(threads);
    } else if (sched_type == "local") {
        sched = std::make_shared<nova::simple_scheduler>(threads);
    } else if (sched_type == "local-numa" || sched_type == "dphim") {
        sched = std::make_shared<nova::numa_aware_scheduler>(threads, use_mem_pool);
    } else if (sched_type == "osthread") {
        sched = std::make_shared<nova::os_thread_scheduler>(threads);
    } else if (sched_type == "sp") {
        // nop
    } else {
        throw std::runtime_error("no matching scheduler type: " + sched_type);
    }

    std::vector<std::string> pmem_paths;
    if (pmem_type == "numa") {
        pmem_paths = {"/dev/dax0.0", "/dev/dax1.0", "/dev/dax2.0", "/dev/dax3.0"};
    } else if (pmem_type == "single") {
        pmem_paths = {"/dev/dax0.0"};
    } else if (pmem_type.empty()) {
        pmem_paths = {};
    } else {
        throw std::runtime_error("unknown pmem type: " + pmem_type);
    }

    dphim::dphim_base::PmemAllocType pat;
    if (pmem_alloc_type == "aek") {
        pat = dphim::dphim_base::PmemAllocType::AEK;
    } else if (pmem_alloc_type == "elems") {
        pat = dphim::dphim_base::PmemAllocType::Elems;
    } else if (pmem_alloc_type.empty()) {
        if (pmem_type.empty()) {
            pat = dphim::dphim_base::PmemAllocType::None;
        } else {
            pat = dphim::dphim_base::PmemAllocType::Elems;
        }
    } else {
        throw std::runtime_error("unknown pmem alloc type: " + pmem_alloc_type);
    }

    std::cout << "algorithm: " << alg << std::endl;
    if (alg == "efim") {
        if (sched_type == "sp") {
            dphim::EFIM efim{in, out, minutil, threads};
            efim.set_pmem_path(pmem_paths);
            efim.run();
            if (out != "/dev/null")
                efim.flushOutput();
            efim.print(std::cout);
        } else {
            dphim::DPEFIM dpefim(sched, in, out, minutil, threads);
            dpefim.use_parted_database = (sched_type == "dphim");
            dpefim.sched_no_await = (sched_type == "para63");
            dpefim.pipeline_parallel = use_pipeline_parallel;
            dpefim.set_pmem_path(pmem_paths);
            dpefim.pmem_alloc_type = pat;

            sched->start();
            std::this_thread::sleep_for(std::chrono::milliseconds(1500));
            nova::sync_wait(dpefim.run());

            auto f = std::async([&] { sched->stop(); });
            if (f.wait_for(std::chrono::seconds(3)) == std::future_status::timeout) {
                std::cerr << "stop request is timeout." << std::endl;
                std::exit(-1);
            }
            if (out != "/dev/null")
                dpefim.flushOutput();
            dpefim.print(std::cout);
        }
    } else if (alg == "fhm") {
        if (sched_type == "sp") {
            //
        } else {
            dphim::DPFHM dpfhm(sched, in, out, minutil, threads, sched_type == "dphim");
            if (threads == 1)
                dpfhm.sched_no_await = true;
            dpfhm.set_pmem_path(pmem_paths);
            dpfhm.pmem_alloc_type = pat;

            sched->start();
            std::this_thread::sleep_for(std::chrono::milliseconds(1500));
            nova::sync_wait(dpfhm.run());
            auto f = std::async([&] { sched->stop(); });
            if (f.wait_for(std::chrono::seconds(3)) == std::future_status::timeout) {
                std::cerr << "stop request is timeout." << std::endl;
                std::exit(-1);
            }
            std::cout << "end" << std::endl;
            if (out != "/dev/null")
                dpfhm.flushOutput();
            dpfhm.print(std::cout);
        }
    } else {
        throw std::runtime_error("no matching algorithm: " + alg);
    }

    return 0;
}
