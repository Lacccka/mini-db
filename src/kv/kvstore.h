#pragma once
#include <unordered_map>
#include <string>
#include <string_view>
#include <optional>
#include <vector>
#include <filesystem>
#include <system_error>
#include <shared_mutex>
#include <atomic>
#include <mutex>
#include "log_segment.h"

struct Config {
    std::filesystem::path data_dir = L"./data";
    uint64_t segment_max_bytes = 64ull * 1024 * 1024;
    bool fsync_each_write = true;
};

class KVStore {
public:
    explicit KVStore(Config cfg);
    ~KVStore();

    void set(std::string_view key, std::string_view value);
    bool del(std::string_view key);
    std::optional<std::string> get(std::string_view key) const;

    std::error_code compact();
    void flush();

private:
    Config cfg_;
    mutable std::shared_mutex mu_;

    struct Meta { Location loc; };
    std::unordered_map<std::string, Meta> index_;

    std::vector<uint32_t> segment_ids_;
    std::unique_ptr<LogSegment> active_;
    std::atomic<uint64_t> seq_{0};

    // read-only сегменты кэшируем для быстрых GET
    mutable std::mutex cache_mu_;
    mutable std::unordered_map<uint32_t, std::unique_ptr<LogSegment>> ro_cache_;

    // внутренние помощники
    void bootstrap_();
    uint32_t next_segment_id_() const;
    std::filesystem::path seg_path_(uint32_t id) const;
    void roll_segment_if_needed_();

    // hint
    bool load_hint_(uint32_t id, uint64_t& max_seq);
    void write_hint_(uint32_t id, const std::unordered_map<std::string, Location>& last_in_seg);

    LogSegment& ro_segment_(uint32_t id) const;
};
