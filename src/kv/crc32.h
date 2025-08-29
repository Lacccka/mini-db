#pragma once
#include <cstdint>
#include <cstddef>
uint32_t crc32(const void* data, size_t len, uint32_t seed = 0xFFFFFFFFu);
