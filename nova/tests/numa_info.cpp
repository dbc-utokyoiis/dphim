#include <iostream>
#include <nova/util/numa_info.hpp>

int main() {
    std::cout << "numa_info" << std::endl;

    nova::numa_info info;
    for (auto &node: info.nodes()) {
        std::cout << "node " << node.id() << ": ";
        for (auto cpu: node.cpu_ids()) {
            std::cout << cpu << " ";
        }
        std::cout << std::endl;
    }

    std::cout << "distances:" << std::endl;
    for (auto &l: info.nodes()) {
        for (auto &r: info.nodes()) {
            std::cout << info.node_distance(l.id(), r.id()) << " ";
        }
        std::cout << std::endl;
    }

    std::cout << "near nodes:" << std::endl;
    for (auto &node: info.nodes()) {
        std::cout << "node " << node.id() << ": ";
        for (auto near_node: node.near_node_ids()) {
            std::cout << near_node << " ";
        }
        std::cout << std::endl;
    }
}