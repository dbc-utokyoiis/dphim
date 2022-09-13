#pragma once

#include "transaction.hpp"

namespace dphim {

std::pair<Transaction, Item> parseTransactionOneLine(std::string line);

std::pair<std::vector<Transaction>, Item> parseTransactions(const std::string &input_path);

}// namespace dphim