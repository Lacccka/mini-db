#include "kvstore.h"
#include <algorithm>
#include <format>
#include <fstream>

static std::filesystem::path hint_path_for(const std::filesystem::path& seg_path) {
    auto p = seg_path;
    p.replace_extension(".hint");
    return p;
}

KVStore::KVStore(Config cfg) : cfg_(std::move(cfg)) {
    std::filesystem::create_directories(cfg_.data_dir);
    bootstrap_();
}

KVStore::~KVStore() = default;

std::filesystem::path KVStore::seg_path_(uint32_t id) const {
    auto name = std::format("{:06}.log", id);
    return cfg_.data_dir / std::filesystem::path(name);
}

uint32_t KVStore::next_segment_id_() const {
    if (segment_ids_.empty()) return 1;
    return segment_ids_.back() + 1;
}

bool KVStore::load_hint_(uint32_t id, uint64_t& max_seq) {
    auto hpath = hint_path_for(seg_path_(id));
    if (!std::filesystem::exists(hpath)) return false;
    try {
        auto ht = std::filesystem::last_write_time(hpath);
        auto lt = std::filesystem::last_write_time(seg_path_(id));
        if (ht < lt) return false;
    } catch(...) { return false; }

    std::ifstream in(hpath, std::ios::binary);
    if (!in) return false;
    auto rd_u32 = [&](uint32_t& v){ in.read(reinterpret_cast<char*>(&v), 4); };
    auto rd_u64 = [&](uint64_t& v){ in.read(reinterpret_cast<char*>(&v), 8); };

    uint32_t magic=0; rd_u32(magic);
    if (magic != 0x314E5448u) return false; // 'HNT1'
    uint8_t ver=0; in.read(reinterpret_cast<char*>(&ver),1);
    if (ver != 1) return false;

    uint32_t file_id=0; rd_u32(file_id);
    if (file_id != id) return false;

    uint32_t count=0; rd_u32(count);

    for (uint32_t i=0; i<count; ++i) {
        uint64_t seq=0; rd_u64(seq);
        uint8_t tomb=0; in.read(reinterpret_cast<char*>(&tomb),1);
        uint32_t klen=0; rd_u32(klen);
        uint32_t recsize=0; rd_u32(recsize);
        uint64_t offset=0; rd_u64(offset);

        std::string key(klen, '\0');
        if (klen) in.read(key.data(), klen);
        if (!in) return false;

        Location loc{ id, offset, recsize, seq, tomb!=0 };
        auto it = index_.find(key);
        if (it == index_.end() || it->second.loc.seq < seq) index_[key] = Meta{ loc };
        if (seq > max_seq) max_seq = seq;
    }
    return true;
}

void KVStore::write_hint_(uint32_t id,
    const std::unordered_map<std::string, Location>& last_in_seg)
{
    auto hpath = hint_path_for(seg_path_(id));
    std::ofstream out(hpath, std::ios::binary | std::ios::trunc);
    if (!out) return;
    auto wr_u32 = [&](uint32_t v){ out.write(reinterpret_cast<const char*>(&v),4); };
    auto wr_u64 = [&](uint64_t v){ out.write(reinterpret_cast<const char*>(&v),8); };

    wr_u32(0x314E5448u); // 'HNT1'
    uint8_t ver=1; out.write(reinterpret_cast<const char*>(&ver),1);
    wr_u32(id);
    wr_u32(static_cast<uint32_t>(last_in_seg.size()));
    for (auto& [key, loc] : last_in_seg) {
        wr_u64(loc.seq);
        uint8_t tomb = loc.tombstone ? 1 : 0; out.write(reinterpret_cast<const char*>(&tomb),1);
        wr_u32(static_cast<uint32_t>(key.size()));
        wr_u32(loc.record_size);
        wr_u64(loc.offset);
        if (!key.empty()) out.write(key.data(), static_cast<std::streamsize>(key.size()));
    }
}

void KVStore::bootstrap_() {
    segment_ids_.clear();
    for (auto& e : std::filesystem::directory_iterator(cfg_.data_dir)) {
        if (!e.is_regular_file()) continue;
        auto name = e.path().filename().wstring();
        if (name.size()==10 && name.ends_with(L".log")) {
            try {
                uint32_t id = std::stoul(std::wstring(name.begin(), name.begin()+6));
                segment_ids_.push_back(id);
            } catch(...) {}
        }
    }
    std::sort(segment_ids_.begin(), segment_ids_.end());
    index_.clear();
    uint64_t max_seq = 0;

    for (auto id : segment_ids_) {
        if (load_hint_(id, max_seq)) continue;

        std::unordered_map<std::string, Location> last_in_seg;
        LogSegment seg(id, seg_path_(id));
        seg.open_readonly();
        seg.scan([&](std::string&& key, Location loc, const char*, uint32_t){
            auto it = last_in_seg.find(key);
            if (it == last_in_seg.end() || it->second.seq < loc.seq) {
                last_in_seg.emplace(std::move(key), loc);
            }
        });
        for (auto& [key, loc] : last_in_seg) {
            auto it = index_.find(key);
            if (it == index_.end() || it->second.loc.seq < loc.seq) index_[key] = Meta{ loc };
            if (loc.seq > max_seq) max_seq = loc.seq;
        }
        write_hint_(id, last_in_seg);
    }

    seq_.store(max_seq);

    uint32_t active_id = segment_ids_.empty() ? 1 : segment_ids_.back();
    if (segment_ids_.empty() || !std::filesystem::exists(seg_path_(active_id))) {
        active_id = 1;
        segment_ids_.push_back(active_id);
    }
    active_ = std::make_unique<LogSegment>(active_id, seg_path_(active_id));
    active_->open_for_append();
}

void KVStore::roll_segment_if_needed_() {
    if (active_->size_bytes() < cfg_.segment_max_bytes) return;
    uint32_t id = next_segment_id_();
    segment_ids_.push_back(id);
    active_ = std::make_unique<LogSegment>(id, seg_path_(id));
    active_->open_for_append();
}

void KVStore::flush() {
    // запись синхронизируется при append, если cfg_.fsync_each_write=true;
    // для batch-режима можно будет реализовать явный fsync активного сегмента.
}

void KVStore::set(std::string_view key, std::string_view value) {
    std::unique_lock lk(mu_);
    roll_segment_if_needed_();
    const uint64_t seq = seq_.fetch_add(1, std::memory_order_relaxed) + 1;
    auto loc = active_->append(OpCode::SET, seq, key, value, cfg_.fsync_each_write);
    index_.insert_or_assign(std::string(key), Meta{ loc });
}

bool KVStore::del(std::string_view key) {
    std::unique_lock lk(mu_);
    auto it = index_.find(std::string(key));
    if (it == index_.end() || it->second.loc.tombstone) return false;
    roll_segment_if_needed_();
    const uint64_t seq = seq_.fetch_add(1, std::memory_order_relaxed) + 1;
    auto loc = active_->append(OpCode::DEL, seq, key, {}, cfg_.fsync_each_write);
    it->second = Meta{ loc };
    return true;
}

LogSegment& KVStore::ro_segment_(uint32_t id) const {
    std::scoped_lock g(cache_mu_);
    auto it = ro_cache_.find(id);
    if (it != ro_cache_.end()) return *(it->second);
    auto seg = std::make_unique<LogSegment>(id, const_cast<KVStore*>(this)->seg_path_(id));
    seg->open_readonly();
    auto& ref = *seg;
    ro_cache_[id] = std::move(seg);
    return ref;
}

std::optional<std::string> KVStore::get(std::string_view key) const {
    std::shared_lock lk(mu_);
    auto it = index_.find(std::string(key));
    if (it == index_.end() || it->second.loc.tombstone) return std::nullopt;
    auto& seg = ro_segment_(it->second.loc.file_id);
    return seg.read_value(it->second.loc);
}

void KVStore::compact() {
    std::unique_lock lk(mu_);

    uint32_t new_id = next_segment_id_();
    LogSegment out(new_id, seg_path_(new_id));
    out.open_for_append();

    std::unordered_map<std::string, Location> last_in_new;

    for (auto& [key, meta] : index_) {
        if (meta.loc.tombstone) continue;
        LogSegment seg(meta.loc.file_id, seg_path_(meta.loc.file_id));
        seg.open_readonly();
        auto val = seg.read_value(meta.loc);

        const uint64_t seq = seq_.fetch_add(1, std::memory_order_relaxed) + 1;
        auto nl = out.append(OpCode::SET, seq, key, val, cfg_.fsync_each_write);
        meta.loc = nl;
        last_in_new[key] = nl;
    }

    active_.reset();
    segment_ids_.push_back(new_id);
    active_ = std::make_unique<LogSegment>(new_id, seg_path_(new_id));
    active_->open_for_append();

    std::vector<uint32_t> to_remove;
    for (auto id : segment_ids_) if (id != new_id) to_remove.push_back(id);
    for (auto id : to_remove) {
        try { std::filesystem::remove(seg_path_(id)); } catch(...) {}
        try { std::filesystem::remove(hint_path_for(seg_path_(id))); } catch(...) {}
    }
    segment_ids_.clear();
    segment_ids_.push_back(new_id);

    write_hint_(new_id, last_in_new);

    {
        std::scoped_lock g(cache_mu_);
        ro_cache_.clear();
    }
}
