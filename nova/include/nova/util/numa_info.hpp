#pragma once

#if 1

extern "C" {
#include <numa.h>
}
#include <algorithm>
#include <vector>

namespace nova::inline numa {

struct numa_info {
    struct node_t {
        friend struct numa_info;
        explicit node_t(int node_id) : m_id(node_id) {
            auto *cpumask = numa_allocate_cpumask();
            if (numa_node_to_cpus(node_id, cpumask) != 0) {
                throw std::runtime_error{std::string{"[numa_node_to_cpus] "} + strerror(errno)};
            }
            m_cpu_ids.reserve(cpumask->size);
            for (auto i = 0u; i < cpumask->size; ++i) {
                if (*(reinterpret_cast<const char *>(cpumask->maskp) + i / 8) & (0x1 << i % 8)) {
                    m_cpu_ids.push_back(static_cast<int>(i));
                }
            }
            numa_free_cpumask(cpumask);
        }

        int id() const { return m_id; }
        const auto &cpu_ids() const { return m_cpu_ids; }
        const auto &near_node_ids() const { return m_near_nodes; }

    private:
        int m_id;
        std::vector<int> m_cpu_ids;
        std::vector<int> m_near_nodes;

        template<typename F>
        void set_near_nodes(const std::vector<node_t> &nodes, F &&get_distance) {
            for (auto &node: nodes) {
                m_near_nodes.push_back(node.m_id);
            }
            std::sort(
                    m_near_nodes.begin(), m_near_nodes.end(),
                    [this, nodes_num = nodes.size(), &get_distance](int l, int r) {
                        if (get_distance(m_id, l) == get_distance(m_id, r)) {
                            return (l - m_id) % nodes_num < (r - m_id) % nodes_num;
                        }
                        return get_distance(m_id, l) < get_distance(m_id, r);
                    });
        }
    };

private:
    static auto make_nodes() {
        if (numa_available() < 0) {
            throw std::runtime_error("NUMA is not available");
        }

        std::vector<node_t> nodes;
        auto *numamask = numa_get_run_node_mask();
        for (int i = 0; i <= numa_max_node(); ++i) {
            if (*(reinterpret_cast<const char *>(numamask->maskp) + i / 8) & (0x1 << i % 8)) {
                nodes.emplace_back(i);
            }
        }
        numa_free_nodemask(numamask);

        return nodes;
    }

    static auto calc_node_distances(int num_nodes) {
        std::vector<std::vector<int>> distances(num_nodes, std::vector<int>(num_nodes));
        for (auto i = 0; i < num_nodes; ++i) {
            for (auto j = 0; j < num_nodes; ++j) {
                distances[i][j] = numa_distance(i, j);
            }
        }
        return distances;
    }

    static auto make_cpu2node(const auto &nodes) {
        std::vector<int> cpu2node;
        cpu2node.resize(numa_num_configured_cpus());
        for (auto &node: nodes) {
            for (auto cpu_id: node.m_cpu_ids) {
                cpu2node[cpu_id] = node.id();
            }
        }
        return cpu2node;
    }

public:
    explicit numa_info()
        : m_nodes(make_nodes()),
          m_numa_distances(calc_node_distances(m_nodes.size())),
          m_cpu2node(make_cpu2node(m_nodes)) {
        for (auto &node: m_nodes) {
            node.set_near_nodes(m_nodes, [this](int l, int r) {
                return this->node_distance(l, r);
            });
        }
    }

    int node_distance(int lhs_id, int rhs_id) const {
        return m_numa_distances.at(lhs_id).at(rhs_id);
    }

    [[nodiscard]] const std::vector<node_t> &nodes() const { return m_nodes; }
    [[nodiscard]] const node_t &node(int node_id) const { return m_nodes[node_id]; }
    [[nodiscard]] const node_t &cpu2node(int cpu_id) const { return m_nodes[m_cpu2node[cpu_id]]; }

private:
    std::vector<node_t> m_nodes;
    std::vector<std::vector<int>> m_numa_distances;
    std::vector<int> m_cpu2node;
};

}// namespace nova::inline numa

#else

#include <numa.h>
#include <vector>

namespace nova {
inline namespace numa {

struct numa_info {
    struct node_t {
        friend numa_info;

        explicit node_t(int node_id) : m_id(node_id) {
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
        const auto &cpu_ids() const { return m_cpus; }
        const auto &distances() const { return m_distances; }
        const auto &distance(int other_id) const { return m_distances.at(other_id); }

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

    numa_info() {
        if (numa_available() < 0) {
            throw std::runtime_error("NUMA is not available");
        }

        auto *numamask = numa_get_run_node_mask();
        for (int i = 0; i <= numa_max_node(); ++i) {
            if (*(reinterpret_cast<const char *>(numamask->maskp) + i / 8) & (0x1 << i % 8)) {
                m_nodes.emplace_back(i);
            }
        }
        numa_free_nodemask(numamask);

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
            for (auto &n: m_nodes) {
                m_near_nodes.at(node).push_back(n.m_id);
            }
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
    [[nodiscard]] const std::vector<int> &near_node_ids(int node_id) const { return m_near_nodes[node_id]; }

private:
    std::vector<node_t> m_nodes;
    std::vector<std::vector<int>> m_near_nodes;
    std::vector<int> m_cpu2node;
};
}// namespace numa
}// namespace nova

#endif