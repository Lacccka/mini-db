#include "crc32.h"
#include <array>

namespace {

constexpr std::array<uint32_t, 256> make_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < table.size(); ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j) {
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}

constexpr auto table_ = make_table();

} // namespace

uint32_t crc32(const void* data, size_t len, uint32_t seed) {
    auto* p = static_cast<const unsigned char*>(data);
    uint32_t c = seed;
    for (size_t i = 0; i < len; ++i) {
        c = table_[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}
