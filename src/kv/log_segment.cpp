#include "log_segment.h"
#include "endian.h"
#include "crc32.h"
#include <vector>
#include <stdexcept>

static constexpr uint32_t MAGIC = 0x314C564Bu; // 'KVL1' (LE)
static constexpr uint8_t  VER   = 1;

LogSegment::LogSegment(uint32_t id, std::filesystem::path path)
    : id_(id), path_(std::move(path)) {}

void LogSegment::open_for_append() { file_.open_append(path_); }
void LogSegment::open_readonly()   { file_.open_readonly(path_); }

Location LogSegment::append(OpCode op, uint64_t seq,
                            std::string_view key, std::string_view value,
                            bool do_fsync)
{
    const uint32_t klen = static_cast<uint32_t>(key.size());
    const uint32_t vlen = (op==OpCode::SET) ? static_cast<uint32_t>(value.size()) : 0;

    unsigned char hdr[4+1+1+2+8+4+4+4]{}; // MAGIC,VER,OP,RES,SEQ,KLEN,VLEN,CRC
    put_u32_le(MAGIC, hdr+0);
    hdr[4] = VER;
    hdr[5] = static_cast<uint8_t>(op);
    put_u64_le(seq, hdr+8);
    put_u32_le(klen, hdr+16);
    put_u32_le(vlen, hdr+20);

    std::vector<unsigned char> to_crc;
    to_crc.reserve(1+1+2+8+4+4 + klen + vlen);
    to_crc.insert(to_crc.end(), hdr+4, hdr+24);
    to_crc.insert(to_crc.end(), reinterpret_cast<const unsigned char*>(key.data()),
                               reinterpret_cast<const unsigned char*>(key.data()) + klen);
    if (op==OpCode::SET) {
        to_crc.insert(to_crc.end(), reinterpret_cast<const unsigned char*>(value.data()),
                                   reinterpret_cast<const unsigned char*>(value.data()) + vlen);
    }
    const uint32_t c = crc32(to_crc.data(), to_crc.size());
    put_u32_le(c, hdr+24);

    const uint32_t rec_size = 28 + klen + vlen;
    std::vector<unsigned char> buf;
    buf.reserve(rec_size);
    buf.insert(buf.end(), hdr, hdr+28);
    buf.insert(buf.end(), reinterpret_cast<const unsigned char*>(key.data()),
                           reinterpret_cast<const unsigned char*>(key.data()) + klen);
    if (op==OpCode::SET) {
        buf.insert(buf.end(), reinterpret_cast<const unsigned char*>(value.data()),
                               reinterpret_cast<const unsigned char*>(value.data()) + vlen);
    }

    const uint64_t off = file_.append(buf.data(), rec_size);
    if (do_fsync) file_.flush();
    return Location{ id_, off, rec_size, seq, op==OpCode::DEL };
}

std::string LogSegment::read_value(const Location& loc) const {
    unsigned char hdr[28];
    file_.read_at(loc.offset, hdr, 28);
    const uint32_t magic = get_u32_le(hdr+0);
    if (magic != MAGIC) throw std::runtime_error("Bad magic");
    const uint8_t op = hdr[5];
    const uint32_t klen = get_u32_le(hdr+16);
    const uint32_t vlen = get_u32_le(hdr+20);
    if (op != static_cast<uint8_t>(OpCode::SET)) throw std::runtime_error("Not a SET");

    std::string val(vlen, '\0');
    const uint64_t val_off = loc.offset + 28 + klen;
    if (vlen) file_.read_at(val_off, val.data(), vlen);
    return val;
}

void LogSegment::scan(std::function<void(std::string&&, Location, const char*, uint32_t)> cb) const {
    uint64_t pos = 0;
    const uint64_t end = file_.size();

    std::vector<char> scratch;
    while (pos + 28 <= end) {
        unsigned char hdr[28];
        file_.read_at(pos, hdr, 28);

        if (get_u32_le(hdr+0) != MAGIC) break;
        if (hdr[4] != VER) break;

        const uint8_t op   = hdr[5];
        const uint64_t seq = get_u64_le(hdr+8);
        const uint32_t klen= get_u32_le(hdr+16);
        const uint32_t vlen= get_u32_le(hdr+20);
        const uint32_t crc = get_u32_le(hdr+24);

        const uint64_t rec_size = 28ull + klen + vlen;
        if (pos + rec_size > end) break;

        scratch.resize(klen + vlen);
        if (klen + vlen)
            file_.read_at(pos + 28, scratch.data(), klen + vlen);

        std::vector<unsigned char> to_crc;
        to_crc.reserve(1+1+2+8+4+4 + klen + vlen);
        to_crc.insert(to_crc.end(), hdr+4, hdr+24);
        to_crc.insert(to_crc.end(), scratch.begin(), scratch.end());
        const uint32_t actual = crc32(to_crc.data(), to_crc.size());
        if (actual != crc) break;

        Location loc{ id_, pos, static_cast<uint32_t>(rec_size), seq, op==static_cast<uint8_t>(OpCode::DEL) };
        if (op == static_cast<uint8_t>(OpCode::SET)) {
            cb(std::string(scratch.data(), klen), loc, scratch.data()+klen, vlen);
        } else if (op == static_cast<uint8_t>(OpCode::DEL)) {
            cb(std::string(scratch.data(), klen), loc, nullptr, 0);
        } else {
            break;
        }
        pos += rec_size;
    }
}
