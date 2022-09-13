#pragma once

#include <numa.h>
#include <vector>

namespace nova {
inline namespace numa {

struct numa_info {
    struct node_t {
        friend numa_info;

        explicit node_t(int node_id) : m_id(node_id), m_cpus{}, m_distances{} {
            auto *cpumask = numa_allocate_cpumask();
            if (numa_node_to_cpus(node_id, cpumask) != 0) {
                throw std::runtime_error{std::string{"[numa_node_to_cpus] "} + strerror(errno)};
            }
            m_cpus.reserve(cpumask->size);
            for (auto i = 0u; i < cpumask->size; ++i) {
                if (*(reinterpret_cast<const char *>(cpumask->maskp) + i / 8) & (0x1 << i % 8)) {
                    m_cpus.push_back(static_cast<int>(i));
                }
            }
            numa_free_cpumask(cpumask);
        }

        int id() const { return m_id; }
        const auto &cpus() const { return m_cpus; }
        const auto &distances() const { return m_distances; }
        const auto &distance(int other) const { return m_distances[other]; }

        template<typename T>
        struct allocator {
            using value_type = T;

            allocator() = delete;
            allocator(int node_id) : node_id(node_id) {}

            T *allocate(std::size_t n) {
                return reinterpret_cast<T *>(numa_alloc_onnode(sizeof(T) * n, node_id));
            }

            void deallocate(T *p, std::size_t n) {
                numa_free(p, sizeof(T) * n);
            }

        private:
            int node_id;
        };

        template<typename T>
        allocator<T> get_allocator() const {
            return allocator<T>(m_id);
        }

    private:
        int m_id;
        std::vector<int> m_cpus;
        std::vector<int> m_distances;
    };

    numa_info() : m_nodes() {
        std::cout << "numa available: " << numa_available() << std::endl;
        if (numa_available() < 0) {
            throw std::runtime_error("NUMA is not available");
        }

        auto *numamask = numa_get_run_node_mask();
        for (int i = 0; i <= numa_max_node(); ++i) {
            if (*(reinterpret_cast<const char *>(numamask->maskp) + i / 8) & (0x1 << i % 8)) {
                m_nodes.emplace_back(i);
            }
        }

        // set distances
        for (auto &node1: m_nodes) {
            node1.m_distances.resize(m_nodes.size());
            for (auto &node2: m_nodes) {
                node1.m_distances[node2.m_id] = numa_distance(node1.m_id, node2.m_id);
            }
        }

        // set cpu -> node
        m_cpu2node.resize(numa_num_configured_cpus());
        for (int node = 0; node < int(m_nodes.size()); ++node) {
            for (auto cpu: m_nodes.at(node).m_cpus) {
                m_cpu2node[cpu] = node;
            }
        }

        // set near nodes
        m_near_nodes.resize(m_nodes.size());
        for (int node = 0; node < int(m_nodes.size()); ++node) {
            m_near_nodes.at(node).reserve(m_nodes.size());
            for (auto &n: m_nodes)
                m_near_nodes.at(node).push_back(n.m_id);
            std::sort(m_near_nodes.at(node).begin(), m_near_nodes.at(node).end(), [&](auto l, auto r) {
                if (m_nodes.at(node).distance(l) == m_nodes.at(node).distance(r)) {
                    return (l - node) % m_nodes.size() < (r - node) % m_nodes.size();
                }
                return m_nodes.at(node).distance(l) < m_nodes.at(node).distance(r);
            });
        }
    }

    [[nodiscard]] const std::vector<node_t> &nodes() const { return m_nodes; }
    [[nodiscard]] const node_t &cpu_to_node(int cpu) const { return m_nodes[m_cpu2node[cpu]]; }
    [[nodiscard]] const node_t &node(int node_id) const { return m_nodes[node_id]; }
    [[nodiscard]] const std::vector<int> &near_nodes(int node_id) const { return m_near_nodes[node_id]; }

private:
    std::vector<node_t> m_nodes;
    std::vector<std::vector<int>> m_near_nodes;
    std::vector<int> m_cpu2node;
};
}// namespace numa
}// namespace nova
