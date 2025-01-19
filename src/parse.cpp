#include <dphim/parse.hpp>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <iostream>
#include <string>

namespace dphim {

std::pair<Transaction, Item> parseTransactionOneLine(std::string line) {
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
                throw e;
            }
        }

        tra.reserve(buf.size());

        for (auto &&e: buf)
            tra.push_back(std::move(e));

        try {
            tra.transaction_utility = static_cast<Utility>(std::stol(line.data() + i, &j));
        } catch (std::invalid_argument &e) {
            std::cerr << e.what() << ": " << __LINE__ << " " << line << std::endl;
            throw e;
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

std::pair<std::vector<Transaction>, Item> parseTransactions(const std::string &input_path) {
    const int fd = open(input_path.c_str(), O_RDONLY);
    if (fd == -1)
        throw std::runtime_error(strerror(errno));

    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    constexpr std::size_t buf_size = 4092;
    alignas(alignof(std::max_align_t)) char buf[buf_size];
    std::string line;
    std::vector<Transaction> res;
    Item maxItem = 0;
    while (auto bytes_read = read(fd, buf, buf_size)) {
        if (bytes_read < 0)
            throw std::runtime_error("read failed");

        char *prev = buf, *p = nullptr;
        for (; (p = (char *) memchr(prev, '\n', (buf + bytes_read) - prev)); prev = p + 1) {
            line.insert(line.size(), prev, p - prev);

            if (auto comment_pos = line.find_first_of("%#@"); comment_pos != std::string::npos)
                line.erase(comment_pos);
            if (line.empty())
                continue;

            auto [tra, mI] = parseTransactionOneLine(std::move(line));
            line.clear();
            res.push_back(std::move(tra));
            maxItem = std::max(maxItem, mI);
        }
        line.insert(line.size(), prev, buf + buf_size - prev);
    }

    {
        auto [tra, mI] = parseTransactionOneLine(std::move(line));
        line.clear();
        res.push_back(std::move(tra));
        maxItem = std::max(maxItem, mI);
    }

    return std::make_pair(std::move(res), maxItem);
}
}// namespace dphim