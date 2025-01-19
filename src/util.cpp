#include <dphim/util/pmem_allocator.hpp>

#include <cstdarg>
#include <cstdio>
#include <fstream>
#include <memory>
#include <nova/util/numa_info.hpp>
#include <sstream>
#include <string>
#include <vector>

namespace dphim {
namespace {
std::string exec(const std::string &cmd) {
    std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        return "";
    }
    char buffer[128];
    std::string result;
    while (!feof(pipe.get())) {
        if (fgets(buffer, 128, pipe.get()) != nullptr)
            result += buffer;
    }
    return result;
}
std::string trim(const std::string &str) {
    auto start = str.begin();
    while (start != str.end() && std::isspace(*start)) {
        ++start;
    }

    auto end = str.end();
    do {
        --end;
    } while (std::distance(start, end) > 0 && std::isspace(*end));

    return std::string(start, end + 1);
}
}// namespace

std::vector<std::string> get_pmem_devdax(std::optional<int> numa_node) {

    std::stringstream cmd;
    if (numa_node.has_value()) {
        cmd << "ndctl list --type=pmem --numa-node=" << *numa_node << " | jq '.[].chardev'";
    } else {
        cmd << "ndctl list --type=pmem | jq '.[].chardev'";
    }

    std::stringstream ss(exec(cmd.str()));
    std::vector<std::string> ret;

    std::string line;
    while (std::getline(ss, line)) {
        line = trim(line);
        if (line.length() >= 2 && line.front() == '"' && line.back() == '"') {
            line = line.substr(1, line.length() - 2);
        }
        ret.push_back(std::move(line));
    }
    return ret;
}
}// namespace dphim