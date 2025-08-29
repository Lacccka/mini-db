#include "win_file.h"
#include <stdexcept>

#ifdef _WIN32
static std::wstring to_w(const std::filesystem::path& p) {
    return p.wstring();
}
#endif

WinFile::~WinFile() { close(); }

void WinFile::open_append(const std::filesystem::path& p) {
    close();
    path_ = p;
#ifdef _WIN32
    handle_ = ::CreateFileW(
        to_w(p).c_str(),
        GENERIC_READ | FILE_APPEND_DATA | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        nullptr,
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    if (handle_ == INVALID_HANDLE_VALUE)
        throw std::runtime_error("CreateFileW (append) failed");
#else
    fd_ = ::open(p.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd_ < 0) throw std::runtime_error("open (append) failed");
#endif
}

void WinFile::open_readonly(const std::filesystem::path& p) {
    close();
    path_ = p;
#ifdef _WIN32
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
#else
    fd_ = ::open(p.c_str(), O_RDONLY);
    if (fd_ < 0) throw std::runtime_error("open (ro) failed");
#endif
}

uint64_t WinFile::size() const {
#ifdef _WIN32
    LARGE_INTEGER sz{};
    if (!::GetFileSizeEx(handle_, &sz)) throw std::runtime_error("GetFileSizeEx failed");
    return static_cast<uint64_t>(sz.QuadPart);
#else
    struct stat st{};
    if (fstat(fd_, &st) != 0) throw std::runtime_error("fstat failed");
    return static_cast<uint64_t>(st.st_size);
#endif
}

uint64_t WinFile::append(const void* data, uint32_t size) {
    uint64_t off = this->size();
#ifdef _WIN32
    LARGE_INTEGER li; li.QuadPart = 0;
    if (!::SetFilePointerEx(handle_, li, nullptr, FILE_END))
        throw std::runtime_error("SetFilePointerEx(FILE_END) failed");

    DWORD written = 0;
    if (!::WriteFile(handle_, data, size, &written, nullptr) || written != size)
        throw std::runtime_error("WriteFile failed");
    return off;
#else
    if (::lseek(fd_, 0, SEEK_END) == -1) throw std::runtime_error("lseek failed");
    ssize_t written = ::write(fd_, data, size);
    if (written != static_cast<ssize_t>(size)) throw std::runtime_error("write failed");
    return off;
#endif
}

void WinFile::read_at(uint64_t offset, void* out, uint32_t size) const {
#ifdef _WIN32
    OVERLAPPED ov{};
    ov.Offset     = static_cast<DWORD>(offset & 0xFFFFFFFFull);
    ov.OffsetHigh = static_cast<DWORD>((offset >> 32) & 0xFFFFFFFFull);

    DWORD read = 0;
    if (!::ReadFile(handle_, out, size, &read, &ov) || read != size)
        throw std::runtime_error("ReadFile failed");
#else
    ssize_t read = ::pread(fd_, out, size, static_cast<off_t>(offset));
    if (read != static_cast<ssize_t>(size)) throw std::runtime_error("pread failed");
#endif
}

void WinFile::flush() {
#ifdef _WIN32
    if (!::FlushFileBuffers(handle_)) throw std::runtime_error("FlushFileBuffers failed");
#else
    if (::fsync(fd_) != 0) throw std::runtime_error("fsync failed");
#endif
}

void WinFile::close() {
#ifdef _WIN32
    if (handle_ != INVALID_HANDLE_VALUE) {
        ::CloseHandle(handle_);
        handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
#endif
}