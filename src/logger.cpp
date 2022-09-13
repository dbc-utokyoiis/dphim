#include <dphim/logger.hpp>

#include <filesystem>

namespace dphim {
Logger::Logger(const std::string &output_path, Utility min_util, std::size_t thread_num)
    : output(output_path, std::ios::out | std::ios::trunc), min_util(min_util), thread_num(thread_num),
      output_is_null(output_path == "/dev/null") {
    if (!output) {
        throw std::runtime_error{"failed to open output (" + output_path + ")"};
    }
}

void Logger::writeOutput(const std::vector<Item> &prefix, Utility utility) {
    hui_count++;
    if (not output_is_null) {
        results.emplace_back(std::make_pair(prefix, utility));
    }
}

void Logger::flushOutput() {
    if (not output_is_null) {
        for (auto &[items, util]: results) {
            for (auto i: items) {
                output << i << " ";
            }
            output << "#UTIL: " << util << std::endl;
        }
    }
}

void Logger::time_point(const std::string &name) {
    std::cout << "time point: " << name << std::endl;
    timer.point(name);
}

void Logger::print(std::ostream &out) {
    using namespace std::chrono;
    out << "============= RESULT ===============\n";
    out << "minUtil = " << min_util << "\n";
    out << "High utility itemsets count: " << hui_count << "\n";
    out << "Candidate count: " << candidate_count << "\n";
    out << "# of threads: " << thread_num << "\n";
    out << "Total time ~: " << duration_cast<milliseconds>(timer.totalTime()).count() << " ms\n";
    //    out << "=========== STATISITCS =============\n";
    //    timer.print(out);
    out << "====================================" << std::endl;
}

ConcurrentLogger::ConcurrentLogger(const std::string &output_path, Utility min_util, std::size_t thread_num)
    : min_util(min_util), thread_num(thread_num),
      results(thread_num), output_is_null(output_path == "/dev/null") {
    std::filesystem::path path{output_path};
    auto dir = path;
    dir.remove_filename();
    create_directories(dir);
    output.open(output_path, std::ios::out | std::ios::trunc);
    if (!output) {
        throw std::runtime_error{"failed to open output (" + output_path + ")"};
    }
}

void ConcurrentLogger::flushOutput() {
    if (not output_is_null) {
        for (auto &result: results) {
            for (auto &[items, util]: result) {
                for (auto i: items) {
                    output << i << " ";
                }
                output << "#UTIL: " << util << std::endl;
            }
        }
    }
}


void ConcurrentLogger::time_point(const std::string &name) {
    //    std::cout << "time point: " << name << std::endl;
    timer.point(name);
}

void ConcurrentLogger::print(std::ostream &out) {
    using namespace std::chrono;
    auto tot_time = duration_cast<milliseconds>(timer.totalTime()).count();
    auto cpu_time = duration_cast<milliseconds>(timer.totalCpuTime()).count();
    out << "============= RESULT ===============\n";
    out << "minUtil = " << min_util << "\n";
    out << "High utility itemsets count: " << hui_count.get() << "\n";
    out << "Candidate count: " << candidate_count.get() << "\n";
    out << "# of threads: " << thread_num << "\n";
    out << "Total time ~: " << tot_time << " ms\n";
    out << "CPU time ~: " << cpu_time << " ms\n";
    out << "CPU Usage ~: " << 1.0 * cpu_time / tot_time << " \n";
    //    out << "=========== STATISITCS =============\n";
    //    timer.print(out);
    out << "====================================" << std::endl;
}

}// namespace dphim