
#include <dphim/dphim_base.hpp>
#include <dphim/util/pmem_allocator.hpp>
#include <nova/when_all.hpp>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>

namespace dphim {

std::pair<Transaction, Item> dphim_base::parseOneLine(std::string line, [[maybe_unused]] int node) {
    Transaction tra;
    Item max_item = 0;

    try {
        std::size_t i = 0, j = 0;
        std::vector<std::pair<Item, Utility>> buf;
        while (i < line.size()) {
            try {
                auto item = static_cast<Item>(std::stol(line.data() + i, &j));
                max_item = std::max(max_item, item);
                i += j + 1;
                buf.emplace_back(item, 0);
                if (line[i - 1] == ':')
                    break;
            } catch (std::invalid_argument &e) {
                std::cerr << e.what() << ": " << __LINE__ << " " << line << std::endl;
            }
        }

        if (pmem_alloc_type != PmemAllocType::None) {
#ifdef DPHIM_PMEM
            auto pmem_allocator = get_pmem_allocator(node < 0 ? std::nullopt : std::optional(node));
            tra.reserve(
                    buf.size(),
                    [=](auto size) { return pmem_allocator->alloc(size); },
                    [=]([[maybe_unused]] auto size) {
                        return [=](auto *p) {
                            using T = std::remove_pointer_t<std::remove_cvref_t<decltype(p)>>;
                            p->~T();
                            pmem_allocator->dealloc(p); };
                    });
#else
            throw std::runtime_error("pmem is not supported");
#endif
        } else {
            tra.reserve(buf.size());
        }

        for (auto &&e: buf)
            tra.push_back(std::move(e));

        try {
            tra.transaction_utility = static_cast<Utility>(std::stol(line.data() + i, &j));
        } catch (std::invalid_argument &e) {
            std::cerr << e.what() << ": " << __LINE__ << " " << line << std::endl;
        }

        i += j + 1;
        if (line[i - 1] != ':')
            throw std::runtime_error("failed to parse");

        for (auto &[item, util]: tra) {
            try {
                util = static_cast<Utility>(std::stol(line.data() + i, &j));
                i += j + 1;
            } catch (std::invalid_argument &e) {
                std::cerr << e.what() << ": " << __LINE__ << " " << line << std::endl;
            }
        }
    } catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
        std::cerr << "input: " << line << std::endl;
        throw;
    }
    return std::make_pair(std::move(tra), max_item);
}

auto dphim_base::parseTransactions() -> nova::task<std::pair<Database, Item>> {
    struct stat st;
    if (stat(input_path.c_str(), &st) == -1)
        throw std::runtime_error(strerror(errno));

    std::vector<nova::task<std::pair<Transactions, Item>>> tasks;
    auto max_node = sched->get_max_node_id().value_or(0);
    tasks.reserve(max_node + 1);
    auto fsize = st.st_size;
    auto diff = (fsize - 1) / (max_node + 1) + 1;
    for (int i = 0; i < max_node + 1; ++i) {
        auto bg = diff * i;
        auto ed = std::min(diff * (i + 1), fsize + 1);
        tasks.emplace_back(parseFileRange(input_path.c_str(), bg, ed, i));
    }

    Database db(tasks.size());
    Item maxItem = 0;
    std::size_t i = 0;
    for (auto &&[trans, mI]: co_await nova::when_all(std::move(tasks))) {
        maxItem = std::max(mI, maxItem);
        db.get(i++) = std::move(trans);
    }
    co_return std::pair<Database, Item>{std::move(db), maxItem};
}

auto dphim_base::parseFileRange(const char *pathname, off_t bg, off_t ed, int node) -> nova::task<std::pair<Transactions, Item>> {

    co_await schedule();

    if (sched->get_current_node_id().has_value() && node > 0) {
        while (sched->get_current_node_id().value() != node)
            co_await schedule(node);
    }

    auto parse_task = [](auto self, auto node, std::vector<std::string> lines) -> nova::task<std::pair<Transactions, Item>> {
        co_await self->schedule(node);
        Transactions transactions;
        try {
            transactions.reserve(lines.size());
        } catch (std::exception &e) {
            std::cerr << __PRETTY_FUNCTION__ << ": " << __LINE__ << " " << e.what() << " " << lines.size() << std::endl;
        }
        Item I = 0;
        for (auto &line: lines) {
            auto [tra, mI] = self->parseOneLine(std::move(line), node);
            transactions.push_back(std::move(tra));
            I = std::max(I, mI);
        }
        co_return std::make_pair(std::move(transactions), I);
    };

    int fd = open(pathname, O_RDONLY);
    if (fd == -1)
        throw std::runtime_error(strerror(errno));

    if (auto err = posix_fadvise(fd, bg, ed - bg, POSIX_FADV_SEQUENTIAL); err != 0)
        throw std::runtime_error(strerror(err));

    std::vector<std::string> lines;
    std::string line;

    auto launch_type = nova::launch::immediate;
    auto when_all_parse = nova::when_all(std::vector<nova::task<std::pair<Transactions, Item>>>{});
    std::size_t transaction_num = 0;

    constexpr std::size_t buf_size = 4092;
    alignas(alignof(std::max_align_t)) char buf[buf_size];
    off_t offset = bg;
    bool exit_flag = false;
    ssize_t bytes_read;
    while (!exit_flag && (bytes_read = pread(fd, buf, buf_size, offset))) {
        if (bytes_read < 0)
            throw std::runtime_error("read failed");

        char *prev = buf, *p = nullptr;
        if (offset == bg && bg != 0) {
            // 最初の\nまで飛ばす
            auto ret = (char *) memchr(prev, '\n', buf_size);
            if (ret)
                prev = ret;
        }

        for (; (p = (char *) memchr(prev, '\n', (buf + bytes_read) - prev)); prev = p + 1) {
            line.insert(line.size(), prev, p - prev);
            auto cur_pos = offset + (p - buf);

            if (auto comment_pos = line.find_first_of("%#@"); comment_pos != std::string::npos)
                line.erase(comment_pos);
            if (line.empty())
                continue;

            lines.push_back(std::move(line));
            line.clear();

            if (cur_pos >= ed) {
                exit_flag = true;
                break;
            }

            if (lines.size() >= 500) {// parse task size
                transaction_num += lines.size();
                when_all_parse.add_task(parse_task(this, node, std::move(lines)), launch_type);
                lines.clear();
            }
        }
        line.insert(line.size(), prev, buf + buf_size - prev);
        offset += bytes_read;
    }
    close(fd);

    transaction_num += lines.size();
    when_all_parse.add_task(parse_task(this, node, std::move(lines)), launch_type);

    Transactions res;
    res.reserve(transaction_num);
    Item max_item = 0;
    for (auto &&[trans, mI]: co_await std::move(when_all_parse)) {
        std::copy(std::make_move_iterator(trans.begin()), std::make_move_iterator(trans.end()), std::back_inserter(res));
        max_item = std::max(max_item, mI);
    }

    co_return std::make_pair(std::move(res), max_item);
}
}// namespace dphim