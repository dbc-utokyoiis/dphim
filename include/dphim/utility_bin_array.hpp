#pragma once

#include <dphim/transaction.hpp>
#include <vector>

namespace dphim {

#if 0
struct UtilityBinArray {
    UtilityBinArray() = default;
    UtilityBinArray(std::size_t bgn, std::size_t ed, Utility d = 0)
        : offset(bgn), LU(ed - bgn + 1, d), SU(ed - bgn + 1, d) {}

private:
    UtilityBinArray(const UtilityBinArray &) = default;
    UtilityBinArray &operator=(const UtilityBinArray &) = default;

public:
    UtilityBinArray(UtilityBinArray &&) noexcept = default;
    UtilityBinArray &operator=(UtilityBinArray &&) noexcept = default;

    void reset(Item bgn, Item ed) {
        offset = bgn;
        LU.resize(ed - bgn + 1);
        std::fill(LU.begin(), LU.end(), 0);
        SU.resize(ed - bgn + 1);
        std::fill(SU.begin(), SU.end(), 0);
    }

    auto size() const { return LU.size(); }
    auto &getLU(Item i) { return LU[i - offset]; }
    const auto &getLU(Item i) const { return LU[i - offset]; }
    auto &getSU(Item i) { return SU[i - offset]; }
    const auto &getSU(Item i) const { return SU[i - offset]; }
    auto clone() const { return *this; }

    UtilityBinArray &operator+=(const UtilityBinArray &other) {
        //        if (offset != other.offset) {
        //            throw std::runtime_error("not match offset");
        //        } else if (LU.size() != other.LU.size()) {
        //            throw std::runtime_error("not match LU.size()");
        //        } else if (SU.size() != other.SU.size()) {
        //            throw std::runtime_error("not match SU.size()");
        //        }
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

#else

struct UtilityBinArray {
    UtilityBinArray() = default;
    UtilityBinArray(std::size_t bgn, std::size_t ed, Utility d = 0)
        : offset(bgn), data(ed - bgn + 1, {d, d}) {}

    UtilityBinArray(const UtilityBinArray &) = delete;
    UtilityBinArray(UtilityBinArray &&) noexcept = default;
    UtilityBinArray &operator=(const UtilityBinArray &) = delete;
    UtilityBinArray &operator=(UtilityBinArray &&) noexcept = default;

    void reset(Item bgn, Item ed) {
        offset = bgn;
        data.resize(ed - bgn + 1);
        std::fill(data.begin(), data.end(), std::pair<Utility, Utility>{0, 0});
    }

    auto size() const { return data.size(); }
    auto &getLU(Item i) { return data[i - offset].first; }
    const auto &getLU(Item i) const { return data[i - offset].first; }
    auto &getSU(Item i) { return data[i - offset].second; }
    const auto &getSU(Item i) const { return data[i - offset].second; }

    UtilityBinArray &operator+=(const UtilityBinArray &other) {
        for (auto i = 0ul; i < data.size(); ++i) {
            data[i].first += other.data[i].first;
            data[i].second += other.data[i].second;
        }
        return *this;
    }

private:
    std::size_t offset = 0;
    std::vector<std::pair<Utility, Utility>> data;
};
#endif

}// namespace dphim