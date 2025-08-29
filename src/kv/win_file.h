#pragma once
#include <string>
#include <string_view>
#include <filesystem>
#include <cstdint>
#include <windows.h>

class WinFile {
public:
    WinFile() = default;
    ~WinFile();

    void open_append(const std::filesystem::path& p);
    void open_readonly(const std::filesystem::path& p);

    uint64_t append(const void* data, uint32_t size);
    void read_at(uint64_t offset, void* out, uint32_t size) const;
    void flush();
    uint64_t size() const;
    bool is_open() const { return handle_ != INVALID_HANDLE_VALUE; }
    void close();

private:
    HANDLE handle_ = INVALID_HANDLE_VALUE;
    std::filesystem::path path_;
};
