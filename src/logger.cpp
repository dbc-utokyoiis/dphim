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
    // std::cout << "time point: " << name << std::endl;
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
    out << "=========== STATISITCS =============\n";
    timer.print(out);
    out << "====================================" << std::endl;
}

ConcurrentLogger::ConcurrentLogger(const std::string &output_path, Utility min_util, std::size_t thread_num, bool is_debug)
    : min_util(min_util), thread_num(thread_num),
      output_is_null(output_path == "/dev/null"),
      is_debug(is_debug),
      results(thread_num) {
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
    if (is_debug) {
        std::cerr << "time point: " << name << std::endl;
    }
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
    if (is_debug) {
        out << "Step3 Internal Malloc: " << malloc_log.get() / 1000 << "kB\n";
        out << "                  Avg: " << malloc_log.get() / malloc_count.get() << "B\n";
    }
    out << "=========== STATISITCS =============\n";
    timer.print(out, false);
    out << "====================================" << std::endl;
}

void ConcurrentLogger::print_json(std::ostream &out) {
    using namespace std::chrono;
    auto tot_time = duration_cast<milliseconds>(timer.totalTime()).count();
    auto cpu_time = duration_cast<milliseconds>(timer.totalCpuTime()).count();
    auto indent = "  ";
    ;
    out << "{\n";
    out << "\"result\": {\n"
        << indent << "\"minUtil\": " << min_util << ",\n"
        << indent << "\"hui_count\": " << hui_count.get() << ",\n"
        << indent << "\"candidate_count\": " << candidate_count.get() << ",\n"
        << indent << "\"thread_num\": " << thread_num << ",\n"
        << indent << "\"total_time\": " << tot_time << ",\n"
        << indent << "\"cpu_time\": " << cpu_time << ",\n"
        << indent << "\"cpu_usage\": " << 1.0 * cpu_time / tot_time << "\n"
        << "},\n";
    out << "\"statistics\": ";
    timer.print(out, true);
    out << "}\n";
}

}// namespace dphim
