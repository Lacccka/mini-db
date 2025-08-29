#pragma once
#include <string>
#include <string_view>
#include <filesystem>
#include <cstdint>
#include <functional>
#include "win_file.h"

enum class OpCode : uint8_t { SET=1, DEL=2 };

struct Location {
    uint32_t file_id;
    uint64_t offset;
    uint32_t record_size;
    uint64_t seq;
    bool tombstone;
};

class LogSegment {
public:
    explicit LogSegment(uint32_t id, std::filesystem::path path);

    void open_for_append();
    void open_readonly();

    Location append(OpCode op, uint64_t seq,
                    std::string_view key, std::string_view value,
                    bool do_fsync = false);

    std::string read_value(const Location& loc) const;

    void scan(std::function<void(std::string&&, Location, const char*, uint32_t)> cb) const;

    uint64_t size_bytes() const { return file_.size(); }
    uint32_t id() const { return id_; }
    const std::filesystem::path& path() const { return path_; }

private:
    uint32_t id_;
    std::filesystem::path path_;
    mutable WinFile file_;
};
