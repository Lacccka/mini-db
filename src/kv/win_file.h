#pragma once
#include <filesystem>
#include <cstdint>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

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
    bool is_open() const {
#ifdef _WIN32
        return handle_ != INVALID_HANDLE_VALUE;
#else
        return fd_ >= 0;
#endif
    }
    void close();

private:
#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int fd_ = -1;
#endif
    std::filesystem::path path_;
};