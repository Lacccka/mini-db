#include "win_file.h"
#include <stdexcept>

static std::wstring to_w(const std::filesystem::path& p) {
    return p.wstring();
}

WinFile::~WinFile() { close(); }

void WinFile::open_append(const std::filesystem::path& p) {
    close();
    path_ = p;
    handle_ = ::CreateFileW(
        to_w(p).c_str(),
        GENERIC_READ | FILE_APPEND_DATA | GENERIC_WRITE,
        FILE_SHARE_READ,
        nullptr,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    if (handle_ == INVALID_HANDLE_VALUE)
        throw std::runtime_error("CreateFileW (append) failed");
}

void WinFile::open_readonly(const std::filesystem::path& p) {
    close();
    path_ = p;
    handle_ = ::CreateFileW(
        to_w(p).c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    if (handle_ == INVALID_HANDLE_VALUE)
        throw std::runtime_error("CreateFileW (ro) failed");
}

uint64_t WinFile::size() const {
    LARGE_INTEGER sz{};
    if (!::GetFileSizeEx(handle_, &sz)) throw std::runtime_error("GetFileSizeEx failed");
    return static_cast<uint64_t>(sz.QuadPart);
}

uint64_t WinFile::append(const void* data, uint32_t size) {
    uint64_t off = this->size();
    LARGE_INTEGER li; li.QuadPart = 0;
    if (!::SetFilePointerEx(handle_, li, nullptr, FILE_END))
        throw std::runtime_error("SetFilePointerEx(FILE_END) failed");

    DWORD written = 0;
    if (!::WriteFile(handle_, data, size, &written, nullptr) || written != size)
        throw std::runtime_error("WriteFile failed");
    return off;
}

void WinFile::read_at(uint64_t offset, void* out, uint32_t size) const {
    OVERLAPPED ov{};
    ov.Offset     = static_cast<DWORD>(offset & 0xFFFFFFFFull);
    ov.OffsetHigh = static_cast<DWORD>((offset >> 32) & 0xFFFFFFFFull);

    DWORD read = 0;
    if (!::ReadFile(handle_, out, size, &read, &ov) || read != size)
        throw std::runtime_error("ReadFile failed");
}

void WinFile::flush() {
    if (!::FlushFileBuffers(handle_)) throw std::runtime_error("FlushFileBuffers failed");
}

void WinFile::close() {
    if (handle_ != INVALID_HANDLE_VALUE) {
        ::CloseHandle(handle_);
        handle_ = INVALID_HANDLE_VALUE;
    }
}
