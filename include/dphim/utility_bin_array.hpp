#pragma once

#include <dphim/transaction.hpp>
#include <vector>

namespace dphim {
struct UtilityBinArray {

    UtilityBinArray() = default;
    UtilityBinArray(std::size_t bgn, std::size_t ed, Utility d = 0)
        : offset(bgn), LU(ed - bgn + 1, d), SU(ed - bgn + 1, d) {}

    UtilityBinArray(const UtilityBinArray &) = delete;
    UtilityBinArray(UtilityBinArray &&) noexcept = default;
    UtilityBinArray &operator=(const UtilityBinArray &) = delete;
    UtilityBinArray &operator=(UtilityBinArray &&) noexcept = default;

    auto size() const { return LU.size(); }

    void reset(Item bgn, Item ed) {
        offset = bgn;
        LU.resize(ed - bgn + 1);
        std::fill(LU.begin(), LU.end(), 0);
        SU.resize(ed - bgn + 1);
        std::fill(SU.begin(), SU.end(), 0);
    }

    auto &getLU(Item i) { return LU[i - offset]; }
    const auto &getLU(Item i) const { return LU[i - offset]; }
    auto &getSU(Item i) { return SU[i - offset]; }
    const auto &getSU(Item i) const { return SU[i - offset]; }

    UtilityBinArray &operator+=(const UtilityBinArray &other) {
        if (offset != other.offset) {
            throw std::runtime_error("not match offset");
        } else if (LU.size() != other.LU.size()) {
            throw std::runtime_error("not match LU.size()");
        } else if (SU.size() != other.SU.size()) {
            throw std::runtime_error("not match SU.size()");
        }
        for (auto i = 0ul; i < LU.size(); ++i) {
            LU[i] += other.LU[i];
        }
        for (auto i = 0ul; i < SU.size(); ++i) {
            SU[i] += other.SU[i];
        }
        return *this;
    }

private:
    std::size_t offset = 0;
    std::vector<Utility> LU;
    std::vector<Utility> SU;
};

}// namespace dphim